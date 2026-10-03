use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use flutter_rust_bridge::frb;
use crate::api::discovery::ProtocolConfig;
use crate::frb_generated::StreamSink;

// ==========================================
// 1. Models & Shared Storage
// ==========================================

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MessageStatus {
    Pending,
    Acked,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistentMessage {
    pub msg_id: String,
    pub peer_mac: String, // Remote peer associated with this chat
    pub is_outgoing: bool,
    pub content: String,
    pub timestamp: i64,
    pub status: MessageStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerRecord {
    pub mac_address: String,
    pub last_known_ip: String,
    pub port: u16,
    pub device_name: String,
    pub last_seen: i64,
}

/// Shared In-Memory Storage
#[derive(Default)]
pub struct LocalStorage {
    peers: RwLock<HashMap<String, PeerRecord>>,
    conversations: RwLock<HashMap<String, Vec<PersistentMessage>>>,
}

impl LocalStorage {
    pub async fn upsert_peer(&self, record: PeerRecord) {
        let mut peers = self.peers.write().await;
        peers.insert(record.mac_address.clone(), record);
    }

    pub async fn get_all_peers(&self) -> Vec<PeerRecord> {
        let peers = self.peers.read().await;
        peers.values().cloned().collect()
    }

    pub async fn get_peer(&self, mac: &str) -> Option<PeerRecord> {
        self.peers.read().await.get(mac).cloned()
    }

    pub async fn append_message(&self, peer_mac: String, msg: PersistentMessage) {
        let mut convs = self.conversations.write().await;
        convs.entry(peer_mac).or_default().push(msg);
    }

    pub async fn mark_acked(&self, peer_mac: &str, target_msg_id: &str) {
        let mut convs = self.conversations.write().await;
        if let Some(messages) = convs.get_mut(peer_mac) {
            if let Some(msg) = messages.iter_mut().find(|m| m.msg_id == target_msg_id) {
                msg.status = MessageStatus::Acked;
            }
        }
    }

    pub async fn get_messages_for_peer(&self, peer_mac: &str) -> Vec<PersistentMessage> {
        let convs = self.conversations.read().await;
        convs.get(peer_mac).cloned().unwrap_or_default()
    }

    pub async fn get_pending_messages(&self, peer_mac: &str) -> Vec<PersistentMessage> {
        let convs = self.conversations.read().await;
        convs
            .get(peer_mac)
            .map(|list| {
                list.iter()
                    .filter(|m| m.is_outgoing && m.status == MessageStatus::Pending)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

// ==========================================
// 2. Wire Protocol Envelopes
// ==========================================

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageEnvelope {
    pub version: u8,
    pub msg_id: String,
    pub sender_mac: String,
    pub payload: MessagePayload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "data")]
pub enum MessagePayload {
    Handshake { sender_mac: String },
    ChatMessage { content: String },
    Ack { target_msg_id: String },
}

#[frb(ignore)]
pub struct PeerConnection {
    pub remote_mac: String,
    stream: Mutex<TcpStream>,
}

impl PeerConnection {
    pub async fn send_envelope(&self, envelope: &MessageEnvelope) -> Result<(), String> {
        let bytes = serde_json::to_vec(envelope).map_err(|e| e.to_string())?;
        let len = (bytes.len() as u32).to_be_bytes();
        let mut guard = self.stream.lock().await;
        guard.write_all(&len).await.map_err(|e| e.to_string())?;
        guard.write_all(&bytes).await.map_err(|e| e.to_string())?;
        guard.flush().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn read_envelope(&self) -> Result<MessageEnvelope, String> {
        let mut guard = self.stream.lock().await;
        let mut len_bytes = [0u8; 4];
        guard.read_exact(&mut len_bytes).await.map_err(|e| e.to_string())?;
        let len = u32::from_be_bytes(len_bytes) as usize;

        let mut buffer = vec![0u8; len];
        guard.read_exact(&mut buffer).await.map_err(|e| e.to_string())?;
        serde_json::from_slice(&buffer).map_err(|e| e.to_string())
    }
}

// ==========================================
// 3. Unified Engine
// ==========================================

#[derive(Clone)]
pub struct NetworkEngine {
    pub self_mac: String,
    pub(crate) storage: Arc<LocalStorage>,
    connections: Arc<RwLock<HashMap<String, Arc<PeerConnection>>>>,
    notify_sink: Arc<RwLock<Option<StreamSink<String>>>>,
}

impl NetworkEngine {
    pub fn new(self_mac: String) -> Self {
        Self {
            self_mac,
            storage: Arc::new(LocalStorage::default()),
            connections: Arc::new(RwLock::new(HashMap::new())),
            notify_sink: Arc::new(RwLock::new(None)),
        }
    }

    /// Stores long-lived stream sink registered from start_listener
    pub async fn register_notify_sink(&self, sink: StreamSink<String>) {
        let mut guard = self.notify_sink.write().await;
        *guard = Some(sink);
    }

    /// Helper to safely emit events to Flutter
    async fn emit_event(&self, event: String) {
        if let Some(ref sink) = *self.notify_sink.read().await {
            let _ = sink.add(event);
        }
    }

    /// Connects or gets existing active connection socket
    async fn get_or_connect(&self, peer_mac: &str) -> Result<Arc<PeerConnection>, String> {
        if let Some(conn) = self.connections.read().await.get(peer_mac) {
            return Ok(Arc::clone(conn));
        }

        let peer_info = self.storage.get_peer(peer_mac).await
            .ok_or_else(|| format!("Peer MAC {} not found in storage", peer_mac))?;

        let addr = format!("{}:{}", peer_info.last_known_ip, peer_info.port);
        let stream = TcpStream::connect(&addr).await.map_err(|e| e.to_string())?;

        let conn = Arc::new(PeerConnection {
            remote_mac: peer_mac.to_string(),
            stream: Mutex::new(stream),
        });

        // Send handshake envelope
        let handshake = MessageEnvelope {
            version: 1,
            msg_id: Uuid::new_v4().to_string(),
            sender_mac: self.self_mac.clone(),
            payload: MessagePayload::Handshake { sender_mac: self.self_mac.clone() },
        };
        conn.send_envelope(&handshake).await?;

        self.connections.write().await.insert(peer_mac.to_string(), Arc::clone(&conn));

        // Background reader task for outbound connection (receives ACKs / incoming messages)
        let engine = self.clone();
        let conn_clone = Arc::clone(&conn);
        let peer_mac_owned = peer_mac.to_string();

        tokio::spawn(async move {
            engine.handle_connection_loop(conn_clone, peer_mac_owned).await;
        });

        Ok(conn)
    }

    /// High-level API to queue and deliver chat messages
    pub async fn send_message(&self, recipient_mac: String, content: String) -> Result<(), String> {
        let msg = PersistentMessage {
            msg_id: Uuid::new_v4().to_string(),
            peer_mac: recipient_mac.clone(),
            is_outgoing: true,
            content,
            timestamp: chrono_now_timestamp(),
            status: MessageStatus::Pending,
        };

        self.storage.append_message(recipient_mac.clone(), msg).await;
        self.flush_outbox(&recipient_mac).await;
        Ok(())
    }

    /// Outbox Processor: Clears pending outgoing queue for a given peer
    pub async fn flush_outbox(&self, peer_mac: &str) {
        let pending = self.storage.get_pending_messages(peer_mac).await;
        if pending.is_empty() {
            return;
        }

        if let Ok(conn) = self.get_or_connect(peer_mac).await {
            for msg in pending {
                let env = MessageEnvelope {
                    version: 1,
                    msg_id: msg.msg_id.clone(),
                    sender_mac: self.self_mac.clone(),
                    payload: MessagePayload::ChatMessage { content: msg.content },
                };

                if conn.send_envelope(&env).await.is_err() {
                    self.connections.write().await.remove(peer_mac);
                    break;
                }
            }
        }
    }

    /// Integrated mDNS discovery and outbox flush loop
    pub async fn run_scan_and_flush_cycle(&self) {
        if let Ok(discovered_peers) = crate::api::discovery::scan_lan_peers().await {
            for peer in discovered_peers {
                let mac = peer.mac_address.clone();
                let record = PeerRecord {
                    mac_address: mac.clone(),
                    last_known_ip: peer.ip,
                    port: ProtocolConfig::SERVICE_PORT,
                    device_name: peer.device_name,
                    last_seen: chrono_now_timestamp(),
                };

                self.storage.upsert_peer(record).await;
                self.flush_outbox(&mac).await;
            }
            self.emit_event("PEER_LIST_UPDATED".to_string()).await;
        }
    }

    /// Starts TCP server and registers active FRB notification stream
    pub async fn start_listener(&self, port: u16, notify_sink: StreamSink<String>) -> Result<(), String> {
        self.register_notify_sink(notify_sink).await;

        let engine = self.clone();
        let addr = format!("0.0.0.0:{}", port);

        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| format!("Failed to bind to {addr}: {e}"))?;

        tokio::spawn(async move {
            loop {
                if let Ok((stream, _)) = listener.accept().await {
                    let engine_clone = engine.clone();
                    tokio::spawn(async move {
                        engine_clone.handle_incoming_stream(stream).await;
                    });
                }
            }
        });

        Ok(())
    }

    async fn handle_incoming_stream(&self, stream: TcpStream) {
        let conn = Arc::new(PeerConnection {
            remote_mac: String::new(),
            stream: Mutex::new(stream),
        });

        self.handle_connection_loop(conn, String::new()).await;
    }

    async fn handle_connection_loop(&self, conn: Arc<PeerConnection>, mut current_peer_mac: String) {
        loop {
            match conn.read_envelope().await {
                Ok(env) => match env.payload {
                    MessagePayload::Handshake { sender_mac } => {
                        current_peer_mac = sender_mac.clone();
                        self.connections.write().await.insert(sender_mac, Arc::clone(&conn));
                    }
                    MessagePayload::ChatMessage { content } => {
                        let msg = PersistentMessage {
                            msg_id: env.msg_id.clone(),
                            peer_mac: env.sender_mac.clone(),
                            is_outgoing: false,
                            content,
                            timestamp: chrono_now_timestamp(),
                            status: MessageStatus::Acked,
                        };

                        self.storage.append_message(env.sender_mac.clone(), msg).await;

                        let ack = MessageEnvelope {
                            version: 1,
                            msg_id: Uuid::new_v4().to_string(),
                            sender_mac: self.self_mac.clone(),
                            payload: MessagePayload::Ack { target_msg_id: env.msg_id },
                        };
                        let _ = conn.send_envelope(&ack).await;

                        self.emit_event(format!("NEW_MSG:{}", env.sender_mac)).await;
                    }
                    MessagePayload::Ack { target_msg_id } => {
                        let peer_mac = if env.sender_mac.is_empty() {
                            &current_peer_mac
                        } else {
                            &env.sender_mac
                        };

                        self.storage.mark_acked(peer_mac, &target_msg_id).await;
                        self.emit_event(format!("ACK:{}", peer_mac)).await;
                    }
                },
                Err(_) => {
                    if !current_peer_mac.is_empty() {
                        self.connections.write().await.remove(&current_peer_mac);
                    }
                    break;
                }
            }
        }
    }
}

pub fn chrono_now_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
