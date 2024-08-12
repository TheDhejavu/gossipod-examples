use std::convert::Infallible;
use std::error::Error;
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::{Context, Result};
use async_trait::async_trait;
use clap::Parser;
use gossipod::{config::{GossipodConfigBuilder, NetworkType}, DispatchEventHandler, Gossipod, Node, NodeMetadata};
use hash_ring::HashRing;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use metrics_exporter_prometheus::{PrometheusBuilder,  PrometheusHandle};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::time;
use tracing_subscriber::{fmt, EnvFilter};
use tracing::{info, error, warn};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use metrics::{counter,  gauge, histogram};

const NODE_NAME: &str = "NODE_1";
const BIND_PORT: u16 = 7948;
const REPLICATION_FACTOR: isize = 5;

#[derive(Debug, Clone)]
struct VNode {
    name: String,
    addr: SocketAddr,
}

impl VNode {
    fn new(addr: SocketAddr, name: String) -> Self {
        VNode { name, addr }
    }
}

impl ToString for VNode {
    fn to_string(&self) -> String {
        format!("{}|{}", self.addr, self.name)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Message {
    key: String,
    value: String,
}

struct SwimNode {
    gossipod: Arc<Gossipod>,
    ring: Arc<RwLock<HashRing<VNode>>>,
    config: gossipod::config::GossipodConfig,
}

struct SwimEventHandler {
    ring: Arc<RwLock<HashRing<VNode>>>,
}

type DispatchError = Box<dyn Error + Send + Sync>;

#[async_trait]
impl<M: NodeMetadata> DispatchEventHandler<M> for SwimEventHandler {
    async fn notify_dead(&self, node: &Node<M>) -> Result<(), DispatchError> {
        info!("Node {} detected as dead", node.name);
        counter!("swimnode.state_changes.dead", 1);
        let mut ring = self.ring.write().await;
        if let Ok(socket_addr) = node.socket_addr() {
            let node = VNode::new(socket_addr, node.name.to_string());
            ring.remove_node(&node);
            info!("Removed dead node {} from hash ring", node.name);
        } else {
            warn!("Failed to get socket address for dead node {}", node.name);
        }
        Ok(())
    }

    async fn notify_leave(&self, node: &Node<M>) -> Result<(), DispatchError> {
        info!("Node {} is leaving the cluster", node.name);
        counter!("swimnode.state_changes.leave", 1);
        let mut ring = self.ring.write().await;
        if let Ok(socket_addr) = node.socket_addr() {
            let node = VNode::new(socket_addr, node.name.to_string());
            ring.remove_node(&node);
            info!("Removed leaving node {} from hash ring", node.name);
        } else {
            warn!("Failed to get socket address for leaving node {}", node.name);
        }
        Ok(())
    }

    async fn notify_join(&self, node: &Node<M>) -> Result<(), DispatchError> {
        info!("Node {} has joined the cluster", node.name);
        counter!("swimnode.state_changes.join", 1);
        let mut ring = self.ring.write().await;
        if let Ok(socket_addr) = node.socket_addr() {
            let node = VNode::new(socket_addr, node.name.to_string());
            ring.add_node(&node);
            info!("Added new node {} to hash ring", node.name);
        } else {
            warn!("Failed to get socket address for joining node {}", node.name);
        }
        Ok(())
    }

    async fn notify_message(&self, from: SocketAddr, message: Vec<u8>) -> Result<(), DispatchError> {
        info!("Received message from {}: {:?}", from, message);
        Ok(())
    }
}

impl SwimNode {
    async fn new(args: &Args) -> Result<Self> {
        let config = GossipodConfigBuilder::new()
            .name(&args.name)
            .port(args.port)
            .addr(args.ip.parse::<IpAddr>().context("Invalid IP address")?)
            .probing_interval(Duration::from_secs(5))
            .ack_timeout(Duration::from_millis(500))
            .indirect_ack_timeout(Duration::from_secs(1))
            .suspicious_timeout(Duration::from_secs(5))
            .network_type(NetworkType::Local)
            .build()
            .await?;

        let hash_ring: HashRing<VNode> = HashRing::new(vec![], REPLICATION_FACTOR);
        let ring = Arc::new(RwLock::new(hash_ring));
        let event_handler = SwimEventHandler {
            ring: ring.clone(),
        };

        let gossipod = Gossipod::with_event_handler(config.clone(), Arc::new(event_handler))
            .await
            .context("Failed to initialize Gossipod with custom metadata")?;

        counter!("swimnode.state_changes.join", 1);

        Ok(SwimNode {
            gossipod: Arc::new(gossipod),
            ring,
            config,
        })
    }

    async fn start(&self) -> Result<()> {
        
        let start = Instant::now();
        let gossipod_clone = self.gossipod.clone();
        tokio::spawn(async move {
            if let Err(e) = gossipod_clone.start().await {
                error!("[ERR] Error starting Gossipod: {:?}", e);
                counter!("swimnode.errors.gossipod", 1);
            }
        });

        self.setup_metrics_exporter().await?;

        // Wait for the Gossipod to be fully running
        while !self.gossipod.is_running().await {
            time::sleep(Duration::from_millis(100)).await;
        }

        let local_node = self.gossipod.get_local_node().await?;
        info!("Local node: {}:{}", local_node.ip_addr, local_node.port);

        let mut ring = self.ring.write().await;
        if let Ok(socket_addr) = local_node.socket_addr() {
            let node = VNode::new(socket_addr, local_node.name.to_string());
            ring.add_node(&node);
            info!("Added local node {} to hash ring", local_node.name);
        } else {
            warn!("Failed to get socket address for local node {}", local_node.name);
        }

        histogram!("swimnode.performance.node_update_time", start.elapsed().as_secs_f64());
        self.update_metrics().await;
        Ok(())
    }

    async fn run(&self) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.print_cluster_state().await?;
                    self.update_metrics().await;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Signal received, stopping Gossipod...");
                    self.gossipod.stop().await?;
                    return Ok(());
                }
            }
        }
    }

    async fn print_cluster_state(&self) -> Result<()> {
        let start = Instant::now();
        let members = self.gossipod.members().await?;
        let ring = self.ring.read().await;

        info!("<> Current cluster state <>");
        for member in &members {
            info!("Node: {}, State: {:?}", member.name, member.state());
        }

        let test_keys = vec!["key1", "key2", "key3", "key4", "key5"];
    
        let mut table = String::from("\nKey Distribution Demonstration\n");
        table.push_str("+------+------------+\n");
        table.push_str("| Key  | Node       |\n");
        table.push_str("+------+------------+\n");

        for &key in &test_keys {
            let node = ring.get_node(key.to_owned())
                .map(|n| n.name.as_str())
                .unwrap_or("No node found");
            table.push_str(&format!("| {:<4} | {:<10} |\n", key, node));
        }

        table.push_str("+------+------------+\n");

        info!("{}", table);

        histogram!("swimnode.performance.key_lookup_time", start.elapsed().as_secs_f64());
        Ok(())
    }

    async fn update_metrics(&self) {
        let members = self.gossipod.members().await.unwrap_or_default();
        let alive_nodes = members.iter().filter(|n| n.is_alive()).count() as f64;
        let suspect_nodes = members.iter().filter(|n| n.is_suspect()).count() as f64;
        let dead_nodes = members.iter().filter(|n| n.is_dead()).count() as f64;
        let node = self.gossipod.get_local_node().await.unwrap().name;

        gauge!("swimnode_cluster_total_nodes", members.len() as f64, "node" => node.clone());
        gauge!("swimnode_cluster_alive_nodes", alive_nodes, "node" => node.clone());
        gauge!("swimnode_cluster_suspect_nodes", suspect_nodes, "node" => node.clone());
        gauge!("swimnode_cluster_dead_nodes", dead_nodes, "node" => node.clone());
    }

    pub async fn setup_metrics_exporter(&self) -> Result<()> {
        let metrics_port = self.config.port() + 1000;
        tokio::spawn(Self::metrics_exporter(metrics_port));
        Ok(())
    }

    async fn serve_metrics(_req: Request<Body>, handle: Arc<PrometheusHandle>) -> Result<Response<Body>, Infallible> {
        let encoder = handle.render();
        Ok(Response::new(Body::from(encoder)))
    }

    async fn metrics_exporter(metrics_port: u16) {    
        let builder = PrometheusBuilder::new();
        let handle = Arc::new(builder.install_recorder().expect("failed to install Prometheus recorder"));
        let handle_clone = Arc::clone(&handle);
        let make_svc = make_service_fn(move |_conn| {
            let handle = Arc::clone(&handle_clone);
            async move {
                Ok::<_, Infallible>(service_fn(move |req| Self::serve_metrics(req, Arc::clone(&handle))))
            }
        });
        let addr = ([127, 0, 0, 1], metrics_port).into();
        let server = Server::bind(&addr).serve(make_svc);
    
        info!("Metrics server running on http://{}", addr);
        if let Err(e) = server.await {
            error!("Metric Server error: {}", e);
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
