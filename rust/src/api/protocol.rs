use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
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
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("Failed to connect to {}: {}", addr, e))?;

        Ok(Self {
            peer_id,
            peer_ip,
            stream: Arc::new(Mutex::new(Some(stream))),
        })
    }

    /// Send a Chat Message envelope over the framing protocol
    pub async fn send_chat_message(&self, sender_id: String, sender_name: String, content: String) -> Result<String, String> {
        let msg_id = Uuid::new_v4().to_string();
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
        let json_bytes = serde_json::to_vec(envelope)
            .map_err(|e| format!("Failed to serialize envelope: {e}"))?;

        let length = json_bytes.len() as u32;
        let mut guard = self.stream.lock().await;

        if let Some(ref mut stream) = *guard {
            let length_bytes = length.to_be_bytes();

            // Write 4-byte Big Endian length prefix
            stream.write_all(&length_bytes).await.map_err(|e| e.to_string())?;
            // Write payload bytes
            stream.write_all(&json_bytes).await.map_err(|e| e.to_string())?;
            stream.flush().await.map_err(|e| e.to_string())?;
            Ok(())
        } else {
            Err("TCP Connection is closed".into())
        }
    }

    /// Read the next incoming MessageEnvelope from the TCP stream
    pub async fn read_envelope(&self) -> Result<MessageEnvelope, String> {
        let mut guard = self.stream.lock().await;

        if let Some(ref mut stream) = *guard {
            // Read 4-byte length prefix
            let length = stream.read_u32().await.map_err(|e| format!("Read length failed: {e}"))? as usize;

            // Read payload bytes
            let mut buffer = vec![0u8; length];
            stream.read_exact(&mut buffer).await.map_err(|e| format!("Read payload failed: {e}"))?;

            let envelope: MessageEnvelope = serde_json::from_slice(&buffer)
                .map_err(|e| format!("Failed to parse JSON envelope: {e}"))?;

            Ok(envelope)
        } else {
            Err("TCP Connection is closed".into())
        }
    }

    /// Close the session socket
    pub async fn close(&self) {
        let mut guard = self.stream.lock().await;
        if let Some(mut stream) = guard.take() {
            let _ = stream.shutdown().await;
        }
    }
}

fn chrono_now_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
