use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageEnvelope {
    pub version: u8,
    pub msg_id: String,
    pub sender_id: String,
    pub sender_name: String,
    pub timestamp: i64,
    pub payload: MessagePayload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "data")]
pub enum MessagePayload {
    Handshake {
        client_version: String,
        public_key: Option<String>,
    },
    ChatMessage {
        content: String,
    },
    Ack {
        target_msg_id: String,
        status: String,
    },
    Ping,
    Pong,
}

pub struct PeerSession {
    pub peer_id: String,
    pub peer_ip: String,
    // Stream wrapped in Arc<Mutex<>> so it can be shared safely across async calls
    stream: Arc<Mutex<Option<TcpStream>>>,
}

impl PeerSession {
    /// Connect to a remote peer via IP and port
    pub async fn connect(peer_id: String, peer_ip: String, port: u16) -> Result<Self, String> {
        let addr = format!("{}:{}", peer_ip, port);
        info!(peer_id = %peer_id, addr = %addr, "Connecting to remote peer...");

        let stream = match TcpStream::connect(&addr).await {
            Ok(s) => {
                info!(peer_id = %peer_id, addr = %addr, "Successfully connected to peer");
                s
            }
            Err(e) => {
                error!(peer_id = %peer_id, addr = %addr, error = %e, "Failed to connect to peer");
                return Err(format!("Failed to connect to {}: {}", addr, e));
            }
        };

        Ok(Self {
            peer_id,
            peer_ip,
            stream: Arc::new(Mutex::new(Some(stream))),
        })
    }

    /// Send a Chat Message envelope over the framing protocol
    pub async fn send_chat_message(&self, sender_id: String, sender_name: String, content: String) -> Result<String, String> {
        let msg_id = Uuid::new_v4().to_string();
        debug!(peer_id = %self.peer_id, msg_id = %msg_id, sender_id = %sender_id, "Preparing chat message envelope");

        let envelope = MessageEnvelope {
            version: 1,
            msg_id: msg_id.clone(),
            sender_id,
            sender_name,
            timestamp: chrono_now_timestamp(),
            payload: MessagePayload::ChatMessage { content },
        };

        self.send_envelope(&envelope).await?;
        Ok(msg_id)
    }

    /// Helper to send framed JSON over TCP: [4-byte length][JSON bytes]
    pub async fn send_envelope(&self, envelope: &MessageEnvelope) -> Result<(), String> {
        let json_bytes = serde_json::to_vec(envelope).map_err(|e| {
            error!(peer_id = %self.peer_id, msg_id = %envelope.msg_id, error = %e, "Failed to serialize envelope");
            format!("Failed to serialize envelope: {e}")
        })?;

        let length = json_bytes.len() as u32;
        let mut guard = self.stream.lock().await;

        if let Some(ref mut stream) = *guard {
            let length_bytes = length.to_be_bytes();

            debug!(
                peer_id = %self.peer_id,
                msg_id = %envelope.msg_id,
                payload_len = length,
                "Sending framed envelope over TCP"
            );

            // Write 4-byte Big Endian length prefix
            if let Err(e) = stream.write_all(&length_bytes).await {
                error!(peer_id = %self.peer_id, msg_id = %envelope.msg_id, error = %e, "Failed to write length prefix");
                return Err(e.to_string());
            }

            // Write payload bytes
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
        let mut guard = self.stream.lock().await;

        if let Some(ref mut stream) = *guard {
            debug!(peer_id = %self.peer_id, "Waiting to read frame length prefix...");

            // Read 4-byte length prefix
            let length = match stream.read_u32().await {
                Ok(len) => len as usize,
                Err(e) => {
                    warn!(peer_id = %self.peer_id, error = %e, "Failed to read length prefix (peer disconnected or socket error)");
                    return Err(format!("Read length failed: {e}"));
                }
            };

            debug!(peer_id = %self.peer_id, payload_len = length, "Reading payload bytes...");

            // Read payload bytes
            let mut buffer = vec![0u8; length];
            if let Err(e) = stream.read_exact(&mut buffer).await {
                error!(peer_id = %self.peer_id, payload_len = length, error = %e, "Failed to read full payload bytes");
                return Err(format!("Read payload failed: {e}"));
            }

            let envelope: MessageEnvelope = serde_json::from_slice(&buffer).map_err(|e| {
                error!(peer_id = %self.peer_id, error = %e, "Failed to parse incoming JSON envelope");
                format!("Failed to parse JSON envelope: {e}")
            })?;

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

    /// Close the session socket
    pub async fn close(&self) {
        info!(peer_id = %self.peer_id, "Closing peer session...");
        let mut guard = self.stream.lock().await;

        if let Some(mut stream) = guard.take() {
            if let Err(e) = stream.shutdown().await {
                warn!(peer_id = %self.peer_id, error = %e, "Error shutting down TCP stream");
            } else {
                info!(peer_id = %self.peer_id, "TCP stream gracefully shut down");
            }
        } else {
            debug!(peer_id = %self.peer_id, "Session was already closed");
        }
    }
}

fn chrono_now_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
