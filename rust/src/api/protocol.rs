use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use flutter_rust_bridge::frb;
use gethostname::gethostname;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use crate::api::network::{get_device_identifier, ProtocolConfig};
use crate::api::protocol::MessagePayload::{ChatMessage, Handshake};
use crate::api::protocol::SessionType::Client;
use crate::frb_generated::StreamSink;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageEnvelope {
    pub version: u8,
    pub msg_id: String,
    pub sender_id: String,
    pub timestamp: i64,
    pub payload: MessagePayload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "data")]
pub enum MessagePayload {
    Handshake {
        client_version: u8,
        public_key: Option<String>,
        peer_id: String,
    },
    ChatMessage {
        content: String,
    },
    Ack {
        target_msg_id: String,
        status: String,
    },
    Ping, // TODO
    Pong, // TODO
}

#[derive(Clone, Debug)]
pub enum SessionType {
    Client,
    Server
}

#[derive(Debug, Clone)]
#[frb(ignore)]
pub struct PeerSession {
    pub peer_id: String,
    pub peer_ip: String,
    pub sender_id: String,
    pub session_type: SessionType,
    // 1. 将读写 Half 分开存储，互不阻塞
    read_stream: Arc<Mutex<Option<OwnedReadHalf>>>,
    write_stream: Arc<Mutex<Option<OwnedWriteHalf>>>,
}


#[frb(ignore)]
impl PeerSession {
    pub async fn connect(peer_manager: PeerManager,
                         peer_ip: String,
                         sender_id: String,
                         port: u16) -> Result<Arc<PeerSession>, String> {
        let addr = format!("{}:{}", peer_ip, port);
        info!(addr = %addr, "Connecting to remote peer...");

        let stream = match TcpStream::connect(&addr).await {
            Ok(s) => {
                info!(addr = %addr, "Successfully connected to peer");
                s
            }
            Err(e) => {
                error!(addr = %addr, error = %e, "Failed to connect to peer");
                return Err(format!("Failed to connect to {}: {}", addr, e));
            }
        };

        // 2. 将 TCPStream 拆分为独立读写端
        let (read_half, write_half) = stream.into_split();

        let mut ret = Self {
            peer_id: "".to_string(),
            peer_ip,
            sender_id,
            session_type: Client,
            read_stream: Arc::new(Mutex::new(Some(read_half))),
            write_stream: Arc::new(Mutex::new(Some(write_half))),
        };

        ret.handshake().await?;

        let ret_arc = Arc::new(ret);

        peer_manager.add_session(Arc::clone(&ret_arc)).await;

        Ok(ret_arc)
    }

    #[frb(ignore)]
    pub async fn new_incoming(
        peer_ip: String,
        sender_id: String,
        stream: TcpStream
    ) -> Result<Self, String> {
        let (read_half, write_half) = stream.into_split();
        let mut ret = Self {
            peer_id: "".to_string(),
            peer_ip,
            sender_id,
            session_type: SessionType::Server,
            read_stream: Arc::new(Mutex::new(Some(read_half))),
            write_stream: Arc::new(Mutex::new(Some(write_half))),
        };
        ret.handshake().await?;
        Ok(ret)
    }

    pub async fn handshake(&mut self) -> Result<String, String> {
        let msg_id = Uuid::new_v4().to_string();
        debug!(peer_id = %self.peer_id, msg_id = %msg_id, "Preparing handshake envelope...");

        let envelope = MessageEnvelope {
            version: 1,
            msg_id: msg_id.clone(),
            sender_id: self.sender_id.clone(),
            timestamp: chrono_now_timestamp(),
            payload: Handshake {
                client_version: 1,
                public_key: None, // TODO
                peer_id: self.sender_id.clone(),
            },
        };

        self.send_envelope(&envelope).await?;
        let handshake = self.read_envelope().await?.payload;
        match handshake {
            Handshake { client_version, public_key, peer_id } => {
                debug!("Received handshake from peer, client_version: {client_version}, peer_id: {peer_id}");
                self.peer_id = peer_id;
            },
            _ => {
            }
        }

        Ok(msg_id)
    }

    pub async fn send_chat_message(&self, content: String) -> Result<String, String> {
        let msg_id = Uuid::new_v4().to_string();
        debug!(peer_id = %self.peer_id, msg_id = %msg_id, "Preparing chat message envelope");

        let envelope = MessageEnvelope {
            version: 1,
            msg_id: msg_id.clone(),
            sender_id: self.sender_id.clone(),
            timestamp: chrono_now_timestamp(),
            payload: ChatMessage { content },
        };

        self.send_envelope(&envelope).await?;
        Ok(msg_id)
    }

    /// Helper to send framed JSON over TCP: [4-byte length][JSON bytes]
    pub async fn send_envelope(&self, envelope: &MessageEnvelope) -> Result<(), String> {
        debug!("Called send_envelope!");

        let json_bytes = serde_json::to_vec(envelope).map_err(|e| {
            error!(peer_id = %self.peer_id, msg_id = %envelope.msg_id, error = %e, "Failed to serialize envelope");
            format!("Failed to serialize envelope: {e}")
        })?;

        let length = json_bytes.len() as u32;

        // 3. 只竞争写入锁，不再受 read_envelope 影响！
        let mut guard = self.write_stream.lock().await;

        debug!("Sending envelope!");

        if let Some(ref mut stream) = *guard {
            let length_bytes = length.to_be_bytes();

            if let Err(e) = stream.write_all(&length_bytes).await {
                error!(peer_id = %self.peer_id, msg_id = %envelope.msg_id, error = %e, "Failed to write length prefix");
                return Err(e.to_string());
            }

            if let Err(e) = stream.write_all(&json_bytes).await {
                error!(peer_id = %self.peer_id, msg_id = %envelope.msg_id, error = %e, "Failed to write payload bytes");
                return Err(e.to_string());
            }

            if let Err(e) = stream.flush().await {
                error!(peer_id = %self.peer_id, msg_id = %envelope.msg_id, error = %e, "Failed to flush TCP stream");
                return Err(e.to_string());
            }

            info!(
                peer_id = %self.peer_id,
                msg_id = %envelope.msg_id,
                payload_len = length,
                "Envelope successfully sent"
            );
            Ok(())
        } else {
            warn!(peer_id = %self.peer_id, "Attempted to send envelope on a closed TCP connection");
            Err("TCP Connection is closed".into())
        }
    }

    /// Read the next incoming MessageEnvelope from the TCP stream
    pub async fn read_envelope(&self) -> Result<MessageEnvelope, String> {
        // 4. 只竞争读取锁，读写互不干涉
        let mut guard = self.read_stream.lock().await;

        if let Some(ref mut stream) = *guard {
            debug!(peer_id = %self.peer_id, "Waiting to read frame length prefix...");

            let length = match stream.read_u32().await {
                Ok(len) => len as usize,
                Err(e) => {
                    warn!(peer_id = %self.peer_id, error = %e, "Failed to read length prefix (peer disconnected or socket error)");
                    return Err(format!("Read length failed: {e}"));
                }
            };

            let mut buffer = vec![0u8; length];
            if let Err(e) = stream.read_exact(&mut buffer).await {
                error!(peer_id = %self.peer_id, payload_len = length, error = %e, "Failed to read full payload bytes");
                return Err(format!("Read payload failed: {e}"));
            }

            let envelope: MessageEnvelope = serde_json::from_slice(&buffer).map_err(|e| {
                error!(peer_id = %self.peer_id, error = %e, "Failed to parse incoming JSON envelope");
                format!("Failed to parse JSON envelope: {e}")
            })?;

            // Ack if it's a message
            if let ChatMessage { .. } = &envelope.payload {
                let msg_id = Uuid::new_v4().to_string();
                self.send_envelope(&MessageEnvelope {
                    version: 1,
                    msg_id,
                    sender_id: self.sender_id.clone(),
                    timestamp: chrono_now_timestamp(),
                    payload: MessagePayload::Ack {
                        target_msg_id: envelope.msg_id.clone(),
                        status: "Ok".to_string(),
                    },
                }).await?;
            }

            info!(
                peer_id = %self.peer_id,
                msg_id = %envelope.msg_id,
                sender_id = %envelope.sender_id,
                "Successfully received and parsed MessageEnvelope"
            );

            Ok(envelope)
        } else {
            warn!(peer_id = %self.peer_id, "Attempted to read from a closed TCP connection");
            Err("TCP Connection is closed".into())
        }
    }

    /// Start the reception loop
    pub async fn start_reception_loop(&self, sink: StreamSink<MessageEnvelope>, peer_manager: PeerManager) {
        let session = self.clone();
        tokio::spawn(async move {
            loop {
                match session.read_envelope().await {
                    Ok(envelope) => {
                        if sink.add(envelope).is_err() {
                            break;
                        }
                    },
                    Err(e) => {
                        debug!("Session {} ended: {}", session.peer_id, e);
                        break;
                    }
                }
            }
            peer_manager.remove_session(&session.peer_id).await;
        });
    }

    /// Close the session socket
    pub async fn close(&self) {
        info!(peer_id = %self.peer_id, "Closing peer session...");

        // 释放写端
        let mut write_guard = self.write_stream.lock().await;
        if let Some(mut stream) = write_guard.take() {
            let _ = stream.shutdown().await;
        }

        // 释放读端
        let mut read_guard = self.read_stream.lock().await;
        read_guard.take();

        info!(peer_id = %self.peer_id, "TCP stream gracefully shut down");
    }
}

#[derive(Clone)]
pub struct PeerManager {
    sessions: Arc<RwLock<HashMap<String, Arc<PeerSession>>>>,
}

impl PeerManager {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[frb(ignore)]
    pub async fn add_session(&self, session: Arc<PeerSession>) {
        let mut lock = self.sessions.write().await;
        lock.insert(session.peer_id.clone(), session);
    }

    #[frb(ignore)]
    pub async fn remove_session(&self, peer_id: &str) -> Option<Arc<PeerSession>> {
        let mut lock = self.sessions.write().await;
        lock.remove(peer_id)
    }

    #[frb(ignore)]
    pub async fn get_session(&self, peer_id: &str) -> Option<Arc<PeerSession>> {
        let lock = self.sessions.read().await;
        lock.get(peer_id).cloned()
    }

    /// Connect to a remote peer and return its peer_id
    pub async fn connect_peer(
        &self,
        peer_ip: String,
        sender_id: String,
        port: u16,
    ) -> Result<String, String> {
        let session = PeerSession::connect(self.clone(), peer_ip, sender_id, port).await?;
        Ok(session.peer_id.clone())
    }

    /// Send chat message by peer_id
    pub async fn send_chat_message(&self, peer_id: &str, content: String) -> Result<String, String> {
        if let Some(session) = self.get_session(peer_id).await {
            session.send_chat_message(content).await
        } else {
            Err(format!("Session with peer_id '{peer_id}' does not exist"))
        }
    }

    /// Close session by peer_id
    pub async fn disconnect_peer(&self, peer_id: &str) -> Result<(), String> {
        if let Some(session) = self.remove_session(peer_id).await {
            session.close().await;
        }
        Ok(())
    }

    pub async fn send_message_with(&self, peer_id: &str, content: String) -> Result<(), String> {
        if let Some(session) = self.get_session(peer_id).await {
            session.send_chat_message(content).await?;
            Ok(())
        } else {
            Err(format!("Failed to get session with peer_id: {peer_id}"))
        }
    }

    pub async fn add_reception_handler_for(&self, sink: StreamSink<MessageEnvelope>, peer_id: &str) -> Result<(), String> {
        if let Some(session) = self.get_session(peer_id).await {
            session.start_reception_loop(sink, self.clone()).await;
            Ok(())
        } else {
            Err(format!("Failed to get session with peer_id: {peer_id}"))
        }
    }

    pub async fn create_peer_listener(&self, sink: StreamSink<String>, sender_id: String) {
        let manager = self.clone();
        tokio::spawn(async move {
            let bind_addr = format!("0.0.0.0:{}", ProtocolConfig::SERVICE_PORT);
            match TcpListener::bind(&bind_addr).await {
                Ok(listener) => loop {
                    match listener.accept().await {
                        Ok((stream, remote_addr)) => {
                            let session = PeerSession::new_incoming(
                                remote_addr.ip().to_string(),
                                sender_id.clone(),
                                stream,
                            ).await;
                            match session {
                                Ok(s) => {
                                    let arc_session = Arc::new(s);
                                    let arc_cloned = Arc::clone(&arc_session);
                                    manager.add_session(arc_cloned).await;
                                    if sink.add(arc_session.peer_id.clone()).is_err() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    debug!("Error when handling connection from peers: {e}");
                                }
                            }

                        },
                        Err(e) => {
                            debug!("Error when listening for peers: {e}");
                        }
                    }
                },
                Err(e) => {
                    debug!("Error when acquiring listener: {e}");
                }
            }
        });
    }
}

impl Default for PeerManager {
    fn default() -> Self {
        Self::new()
    }
}

fn chrono_now_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub fn get_sender_id() -> String {
    // TODO Custom ones...
    get_device_identifier()
}

pub fn get_service_port() -> u16 {
    ProtocolConfig::SERVICE_PORT
}
