use async_trait::async_trait;
use std::sync::Arc;
use flutter_rust_bridge::frb;
use crate::api::connection::manager::PeerConnection;
use crate::api::models::MessageEnvelope;

#[frb(ignore)]
#[async_trait]
pub trait MessagingService: Send + Sync {
    async fn send_message(&self, recipient_mac: String, content: String) -> Result<(), String>;
    async fn flush_outbox(&self, peer_mac: &str);

    async fn handle_message_payload(&self, conn: &Arc<PeerConnection>, current_peer_mac: &str, env: &MessageEnvelope) -> Option<String>;
}

