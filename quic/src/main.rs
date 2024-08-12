use std::error::Error;
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Context, Result};
use async_trait::async_trait;
use clap::Parser;
use gossipod::{DefaultBroadcastQueue, DefaultMetadata};
use gossipod::{config::{GossipodConfigBuilder, NetworkType}, DispatchEventHandler, Gossipod, Node, NodeMetadata};
use tracing::*;
use tokio::sync::mpsc;
use tokio::time;
use quic::QuicTransport;
use tracing_subscriber::{fmt, EnvFilter};
use tracing::{info, error};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

mod quic;

const NODE_NAME: &str = "QUINN_NODE_1";
const BIND_PORT: u16 = 7948;

struct SwimNode {
    gossipod: Arc<Gossipod>,
    receiver: mpsc::Receiver<Vec<u8>>,
}

struct EventHandler{
    sender: mpsc::Sender<Vec<u8>>,
}

impl EventHandler {
    fn new(sender: mpsc::Sender<Vec<u8>>) -> Self{
        Self { sender }
    }
}

#[async_trait]
impl<M: NodeMetadata> DispatchEventHandler<M> for EventHandler {
    async fn notify_dead(&self, node: &Node<M>) -> Result<(), Box<dyn Error + Send + Sync>>  {
        info!("Node {} detected as dead", node.name);
        Ok(())
    }

    async fn notify_leave(&self, node: &Node<M>) -> Result<(), Box<dyn Error + Send + Sync>>  {
        info!("Node {} is leaving the cluster", node.name);
        Ok(())
    }

    async fn notify_join(&self, node: &Node<M>) -> Result<(), Box<dyn Error + Send + Sync>>  {
        info!("Node {} has joined the cluster", node.name);
        Ok(())
    }

    async fn notify_message(&self, from: SocketAddr, message: Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("Received message from {}: {:?}", from, message);
        self.sender.send(message).await?;
        Ok(())
    }
}

impl SwimNode {
    async fn new(args: &Args) -> Result<Self> {
        let config = GossipodConfigBuilder::new()
            .name(&args.name)
            .port(args.port)
            .addr(args.ip.parse::<IpAddr>().expect("Invalid IP address"))
            .probing_interval(Duration::from_secs(3))
            .ack_timeout(Duration::from_millis(1_500))
            .indirect_ack_timeout(Duration::from_secs(1))
            .suspicious_timeout(Duration::from_secs(5))
            .network_type(NetworkType::Local)
            .build()
            .await?;

        let (sender, receiver) = mpsc::channel(1000);
        let dispatch_event_handler = Arc::new(EventHandler::new(sender));
    
        let broadcast_queue =  Arc::new(DefaultBroadcastQueue::new(1));
        let addr = SocketAddr::new(config.ip_addr(), args.port);
        let quinn_transport = Arc::new(QuicTransport::new(addr).await?);

        let gossipod = Gossipod::with_custom(config.clone(), DefaultMetadata::default(), broadcast_queue, quinn_transport, Some(dispatch_event_handler))
        .await
        .context("Failed to initialize Gossipod with custom metadata")?;
        
        Ok(SwimNode {
            gossipod: Arc::new(gossipod),
            receiver,
        })
    }
    async fn start(&self) -> Result<()> {
        let gossipod_clone = self.gossipod.clone();
        tokio::spawn(async move {
            if let Err(e) = gossipod_clone.start().await {
                error!("[ERR] Error starting Gossipod: {:?}", e);
            }
        });

        while !self.gossipod.is_running().await {
            time::sleep(Duration::from_millis(100)).await;
        }

        let local_node = self.gossipod.get_local_node().await?;
        info!("Local node: {}:{}", local_node.ip_addr, local_node.port);

        Ok(())
    }

    async fn run(&self) -> Result<()> {

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Signal received, stopping Gossipod...");
                    self.gossipod.stop().await?;
                    return Ok(());
                }
            }
        }
    }

}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = NODE_NAME)]
    name: String,

    #[arg(long, default_value_t = BIND_PORT)]
    port: u16,

    #[arg(long, default_value = "127.0.0.1")]
    ip: String,

    #[arg(long)]
    join_addr: Option<String>,
}

fn setup_tracing() {
    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_ansi(true)
        .with_level(true);

    let filter_layer = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("debug"));

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    setup_tracing();
    let node = SwimNode::new(&args).await?;
    node.start().await?;

    if let Some(join_addr) = args.join_addr {
        info!("Joining cluster via {}", join_addr);
        node.gossipod.join(join_addr.parse()?).await?;
    }

    node.run().await?;

    info!("Node stopped. Goodbye!");
    Ok(())
}