use async_trait::async_trait;
use std::sync::Arc;
use flutter_rust_bridge::frb;
use crate::api::connection::manager::PeerConnection;
use crate::api::models::{MessageEnvelope, MessagePayload};

#[frb(ignore)]
#[async_trait]
pub trait ConnectionManager: Send + Sync {
    async fn get_or_connect(&self, peer_mac: &str) -> Result<Arc<PeerConnection>, String>;
    async fn remove_connection(&self, peer_mac: &str);
    async fn has_connection(&self, peer_mac: &str) -> bool;
    async fn handle_message_payload(&self, conn: &Arc<PeerConnection>, current_peer_mac: &mut String, env: &MessageEnvelope) -> Option<String>;
}

// 1. 包装 PeerConnection 为 FRB 兼容的不透明句柄
pub struct PeerConnectionHandle {
    pub(crate) inner: Arc<PeerConnection>,
}

impl PeerConnectionHandle {
    pub async fn send_envelope(&self, envelope: MessageEnvelope) -> Result<(), String> {
        self.inner.send_envelope(&envelope).await
    }

    pub fn remote_ip(&self) -> String {
        self.inner.remote_ip.clone()
    }
}
