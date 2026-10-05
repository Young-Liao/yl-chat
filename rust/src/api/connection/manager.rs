use super::traits::{ConnectionManager};
use crate::api::engine::NetworkEngine;
use crate::api::models::{MessageEnvelope, MessagePayload};
use crate::api::storage::traits::{StorageRepository};
use async_trait::async_trait;
use flutter_rust_bridge::frb;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info};
use uuid::Uuid;

pub struct PeerConnection {
    pub(crate) remote_mac: RwLock<String>,
    pub remote_ip: String,
    pub(crate) tx: mpsc::Sender<MessageEnvelope>,
}

impl PeerConnection {
    pub async fn send_envelope(&self, envelope: &MessageEnvelope) -> Result<(), String> {
        self.tx
            .send(envelope.clone())
            .await
            .map_err(|e| format!("Failed to queue message envelope: {e}"))
    }
}

#[frb(ignore)]
pub struct TcpConnectionManager {
    self_mac: String,
    storage: Arc<dyn StorageRepository>,
    connections: Arc<RwLock<HashMap<String, Arc<PeerConnection>>>>,
    engine: RwLock<Option<Weak<NetworkEngine>>>,
}

impl TcpConnectionManager {
    pub fn new(self_mac: String, storage: Arc<dyn StorageRepository>) -> Self {
        Self {
            self_mac,
            storage,
            connections: Arc::new(RwLock::new(HashMap::new())),
            engine: RwLock::new(None),
        }
    }

    /// 绑定 NetworkEngine 的弱引用，用于将读取流 (Read Half) 传回 Engine 处理
    pub async fn set_engine(&self, engine: Weak<NetworkEngine>) {
        *self.engine.write().await = Some(engine);
    }

    /// 手动/被动握手成功后直接插入连接池
    pub async fn insert_connection(&self, peer_mac: String, conn: Arc<PeerConnection>) {
        self.connections.write().await.insert(peer_mac, conn);
    }

    pub(crate) fn connections_map(&self) -> Arc<RwLock<HashMap<String, Arc<PeerConnection>>>> {
        Arc::clone(&self.connections)
    }

    pub(crate) async fn setup_connection(
        &self,
        stream: TcpStream,
        remote_ip: String,
        peer_mac: String,
    ) -> (mpsc::Sender<MessageEnvelope>, Arc<PeerConnection>) {
        let (read_half, mut write_half) = stream.into_split();
        let (tx, mut rx) = mpsc::channel::<MessageEnvelope>(100);

        let conn = Arc::new(PeerConnection {
            remote_mac: RwLock::new(peer_mac.clone()),
            remote_ip: remote_ip.clone(),
            tx: tx.clone(),
        });

        let ip_writer = remote_ip.clone();
        // 1. 启动 Write 循环
        tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                let bytes = match serde_json::to_vec(&envelope) {
                    Ok(b) => b,
                    Err(e) => {
                        error!(error = %e, "Failed to serialize envelope");
                        continue;
                    }
                };

                let len = (bytes.len() as u32).to_be_bytes();
                if write_half.write_all(&len).await.is_err()
                    || write_half.write_all(&bytes).await.is_err()
                    || write_half.flush().await.is_err()
                {
                    error!(ip = %ip_writer, "Socket write error");
                    break;
                }
            }
        });

        // 2. 启动 Read 循环（如果绑定了 NetworkEngine，把 read_half 交给 handle_read_loop 处理）
        if let Some(engine_weak) = self.engine.read().await.as_ref() {
            if let Some(engine) = engine_weak.upgrade() {
                let conn_clone = Arc::clone(&conn);
                let ip_clone = remote_ip.clone();
                let peer_mac_clone = peer_mac.clone();

                tokio::spawn(async move {
                    engine
                        .handle_read_loop(read_half, conn_clone, peer_mac_clone, ip_clone)
                        .await;
                });
            } else {
                error!("NetworkEngine weak reference upgrade failed during setup_connection");
            }
        } else {
            error!("NetworkEngine weak reference not bound in TcpConnectionManager");
        }

        (tx, conn)
    }
}

#[frb(ignore)]
#[async_trait]
impl ConnectionManager for TcpConnectionManager {
    async fn get_or_connect(&self, peer_mac: &str) -> Result<Arc<PeerConnection>, String> {
        if let Some(conn) = self.connections.read().await.get(peer_mac) {
            return Ok(Arc::clone(conn));
        }

        let peer_record = self
            .storage
            .get_peer(peer_mac)
            .await
            .ok_or_else(|| format!("Peer MAC {} not found in local storage", peer_mac))?;

        let addr = format!("{}:{}", peer_record.last_known_ip, peer_record.port);
        info!(peer_mac = %peer_mac, address = %addr, "Connecting to peer");

        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("Connect failed: {e}"))?;

        let (_tx, conn) = self
            .setup_connection(stream, peer_record.last_known_ip.clone(), peer_mac.to_string())
            .await;

        let handshake = MessageEnvelope {
            version: 1,
            msg_id: Uuid::new_v4().to_string(),
            sender_mac: self.self_mac.clone(),
            payload: MessagePayload::Handshake {
                sender_mac: self.self_mac.clone(),
            },
        };

        conn.send_envelope(&handshake).await?;
        self.insert_connection(peer_mac.to_string(), Arc::clone(&conn)).await;
        self.storage.set_peer_online_status(peer_mac, true).await;

        Ok(conn)
    }

    async fn remove_connection(&self, peer_mac: &str) {
        self.connections.write().await.remove(peer_mac);
    }

    async fn has_connection(&self, peer_mac: &str) -> bool {
        self.connections.read().await.contains_key(peer_mac)
    }

    async fn handle_message_payload(&self,
                                    conn: &Arc<PeerConnection>,
                                    current_peer_mac: &mut String,
                                    env: &MessageEnvelope) -> Option<String> {
        match &env.payload {
            MessagePayload::Handshake { sender_mac } => {
                *current_peer_mac = sender_mac.clone();
                *conn.remote_mac.write().await = sender_mac.clone();
                self.insert_connection(sender_mac.clone(), Arc::clone(&conn)).await;
                self.storage.set_peer_online_status(&sender_mac, true).await;
                Some("PEER_LIST_UPDATED".into())
            }
            _ => None
        }
    }
}

