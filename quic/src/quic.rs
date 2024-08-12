use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;
use anyhow::{Result, anyhow};
use gossipod::{Datagram, DatagramTransport};
use quinn::{Endpoint, Connection, ClientConfig, ServerConfig, ConnectionError};
use tokio::sync::{broadcast, Mutex, RwLock};
use async_trait::async_trait;
use std::net::SocketAddr;
use tracing::*;
use bytes::Bytes;


pub struct QuicTransport {
    endpoint: Endpoint,
    datagram_tx: broadcast::Sender<Datagram>,
    conns: Arc<RwLock<HashMap<SocketAddr, Arc<Mutex<Option<Connection>>>>>>,
    shutdown_signal: broadcast::Sender<()>,
}

impl QuicTransport {
    pub async fn new(bind_addr: SocketAddr) -> Result<Self> {
        let (server_config, client_config) = build_quinn_configs().await?;
        let mut endpoint = Endpoint::server(server_config, bind_addr)?;
        endpoint.set_default_client_config(client_config);

        let (datagram_tx, _) = broadcast::channel(10000);
        let (shutdown_signal, _) = broadcast::channel(1);

        let transport = Self {
            endpoint,
            datagram_tx,
            conns: Arc::new(RwLock::new(HashMap::new())),
            shutdown_signal,
        };

        transport.spawn_connection_listener();
        Ok(transport)
    }

    fn spawn_connection_listener(&self) {
        let endpoint = self.endpoint.clone();
        let datagram_tx = self.datagram_tx.clone();
        let conns = self.conns.clone();
        let mut shutdown_rx = self.shutdown_signal.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(conn) = endpoint.accept() => {
                        let remote_addr = conn.remote_address();
                        info!("New QUIC connection from {}", remote_addr);
                        
                        let connection = match conn.await {
                            Ok(c) => c,
                            Err(e) => {
                                error!("Failed to accept connection: {}", e);
                                continue;
                            }
                        };
                        
                        let datagram_tx = datagram_tx.clone();
                        let conns = conns.clone();

                        tokio::spawn(async move {
                            handle_connection(connection, remote_addr, datagram_tx, conns).await;
                        });
                    }
                    _ = shutdown_rx.recv() => {
                        info!("QUIC listener received shutdown signal");
                        break;
                    }
                }
            }
            info!("QUIC listener shut down");
        });
    }

    async fn connect(&self, addr: SocketAddr) -> Result<Connection, ConnectionError> {
        let conn_lock = self.get_conn_lock(addr).await;
        let mut lock = conn_lock.lock().await;

        if let Some(conn) = lock.as_ref() {
            if conn.close_reason().is_none(){
                return Ok(conn.clone());
            }
        }

        *lock = None;

        let conn = self.measured_connect(addr, addr.ip().to_string()).await?;
        *lock = Some(conn.clone());
        Ok(conn)
    }

    async fn get_conn_lock(&self, addr: SocketAddr) -> Arc<Mutex<Option<Connection>>> {
        let mut w = self.conns.write().await;
        w.entry(addr).or_default().clone()
    }

    async fn measured_connect(&self, addr: SocketAddr, server_name: String) -> Result<Connection, ConnectionError> {
        let start = std::time::Instant::now();
        if let Ok(conn) = self.endpoint.connect(addr, &server_name).map_err(|e| anyhow!(e)) {
            let duration = start.elapsed();
            debug!("Connection to {} established in {:?}", addr, duration);
            return Ok(conn.await?);
        }
        Err(ConnectionError::TimedOut)
    }
}

async fn handle_connection(
    connection: Connection,
    remote_addr: SocketAddr,
    datagram_tx: broadcast::Sender<Datagram>,
    conns: Arc<RwLock<HashMap<SocketAddr, Arc<Mutex<Option<Connection>>>>>>,
) {
    debug!("New connection established with {}", remote_addr);
    loop {
        match connection.read_datagram().await {
            Ok(bytes) => {
                if bytes == Bytes::from_static(b"keep-alive") {
                    debug!("Received keep-alive from {}", remote_addr);
                    continue;
                }
                
                debug!("Received datagram from {}: {} bytes", remote_addr, bytes.len());
                let datagram = Datagram {
                    remote_addr,
                    data: bytes.to_vec(),
                };
                if datagram_tx.send(datagram).is_err() {
                    error!("All QUIC receivers have been dropped");
                    break;
                }
            },
            Err(ConnectionError::ApplicationClosed(error)) => {
                warn!("Connection closed by application for {}: {}", remote_addr, error);
                warn!("Connection stats: {:?}", connection.stats());
                break;
            },
            Err(ConnectionError::ConnectionClosed(error)) => {
                warn!("Connection closed for {}: {:?}", remote_addr, error);
                warn!("Connection stats: {:?}", connection.stats());
                break;
            },
            Err(e) => {
                error!("Error reading datagram from {}: {}", remote_addr, e);
                break
            }
        }
    }

    // Remove the closed connection from the list
    let mut conns = conns.write().await;
    conns.remove(&remote_addr);
    drop(conns);

    info!("Connection handler for {} finished", remote_addr);
}

type TransportError = Box<dyn std::error::Error + Send + Sync>;

#[async_trait]
impl DatagramTransport for QuicTransport {
    fn incoming(&self) -> broadcast::Receiver<Datagram> {
        self.datagram_tx.subscribe()
    }

    async fn send_to(&self, target: SocketAddr, data: &[u8]) -> Result<(), TransportError> {
        let conn = self.connect(target).await?;
        let bytes = Bytes::copy_from_slice(data);
        conn.send_datagram(bytes)?;
        Ok(())
    }

    fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        Ok(self.endpoint.local_addr()?)
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        self.shutdown_signal.send(())
            .map_err(|_| anyhow!("Failed to send shutdown signal"))?;
        
        let conns = self.conns.read().await;
        for conn_mutex in conns.values() {
            if let Some(conn) = conn_mutex.lock().await.as_ref() {
                conn.close(0u32.into(), b"Server shutting down");
            }
        }
        
        self.endpoint.wait_idle().await;
        Ok(())
    }
}

async fn build_quinn_configs() -> Result<(ServerConfig, ClientConfig)> {
    let mut server_config = quinn_plaintext::server_config();
    let client_config = quinn_plaintext::client_config();

    let mut transport_config = quinn::TransportConfig::default();
    transport_config.max_idle_timeout(Some(
        Duration::from_secs(120).try_into().unwrap(),
    ));
    transport_config.keep_alive_interval(Some(Duration::from_secs(15)));
    transport_config.max_concurrent_bidi_streams(32u32.into());
    transport_config.max_concurrent_uni_streams(256u32.into());

    let transport_config = Arc::new(transport_config);
    server_config.transport_config(transport_config.clone());

    info!("QUIC transport config: {:?}", transport_config);

    Ok((server_config, client_config))
}