use std::collections::BTreeMap;
use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use clap::Parser;
use gossipod::{config::{GossipodConfigBuilder, NetworkType}, DispatchEventHandler, Gossipod, Node, NodeMetadata};
use hash_ring::HashRing;
use log::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::RwLock;
use tokio::time;
use std::str::FromStr;

const NODE_NAME: &str = "NODE_1";
const BIND_PORT: u16 = 7948;
const PARTITION_COUNT: usize = 100;
const REPLICATION_FACTOR: isize = 5;


#[derive(Debug, Clone)]
struct VNode {
    name: String,
    addr: SocketAddr,
}

impl VNode {
    fn new(addr: SocketAddr, name: String) -> Self {
        VNode {
            name,
            addr,
        }
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

#[async_trait]
impl<M: NodeMetadata> DispatchEventHandler<M> for SwimEventHandler {
    async fn notify_dead(&self, node: &Node<M>) -> Result<()> {
        info!("Node {} detected as dead", node.name);
        let mut ring = self.ring.write().await;
        let node = VNode::new(node.socket_addr()? ,node.name.to_string());
        ring.remove_node(&node);

        info!("Removed dead node {} from hash ring", node.name);
        Ok(())
    }

    async fn notify_leave(&self, node: &Node<M>) -> Result<()> {
        info!("Node {} is leaving the cluster", node.name);
        let mut ring = self.ring.write().await;
        let node = VNode::new(node.socket_addr()? ,node.name.to_string());
        ring.remove_node(&node);

        info!("Removed leaving node {} from hash ring", node.name);
        Ok(())
    }

    async fn notify_join(&self, node: &Node<M>) -> Result<()> {
        info!("Node {} has joined the cluster", node.name);
        let mut ring = self.ring.write().await;
        
        let node = VNode::new(node.socket_addr()? ,node.name.to_string());
        ring.add_node(&node);
        info!("Added new node {} to hash ring", node.name);
        Ok(())
    }

    async fn notify_message(&self, from: SocketAddr, message: Vec<u8>) -> Result<()> {
        info!("Received message from {}: {:?}", from, message);
        Ok(())
    }
}

impl SwimNode {
    async fn new(args: &Args) -> Result<Self> {
        let config = GossipodConfigBuilder::new()
            .name(&args.name)
            .port(args.port)
            .addr(args.ip.parse::<Ipv4Addr>().expect("Invalid IP address"))
            .probing_interval(Duration::from_secs(5))
            .ack_timeout(Duration::from_millis(500))
            .indirect_ack_timeout(Duration::from_secs(1))
            .suspicious_timeout(Duration::from_secs(5))
            .network_type(NetworkType::Local)
            .build()
            .await?;

    
        let hash_ring: HashRing<VNode> =HashRing::new(vec![], REPLICATION_FACTOR);
        let ring =  Arc::new(RwLock::new(hash_ring));
        let event_handler = SwimEventHandler {
            ring: ring.clone(),
        };

        let gossipod = Gossipod::with_event_handler(config.clone(), Arc::new(event_handler))
            .await
            .context("Failed to initialize Gossipod with custom metadata")?;

        Ok(SwimNode {
            gossipod: Arc::new(gossipod),
            ring,
            config,
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

        // Add the local node to the hash ring
        let mut ring = self.ring.write().await;
        let node = VNode::new(local_node.socket_addr()? ,local_node.name.to_string());
        
        ring.add_node(&node);
        info!("Added local node {} to hash ring", local_node.name);

        Ok(())
    }

    async fn run(&self) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.print_cluster_state().await?;
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
        let members = self.gossipod.members().await?;
        let ring = self.ring.read().await;

        info!("Current cluster state:");
        for member in members {
            info!("Node: {}, State: {:?}", member.name, member.state());
        }

        // Demonstrate key distribution
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

        Ok(())
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

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