use async_trait::async_trait;
use std::sync::Arc;
use flutter_rust_bridge::frb;
use crate::api::connection::manager::PeerConnection;
use crate::api::models::MessageEnvelope;

#[frb(ignore)]
#[async_trait]
pub trait FileTransferService: Send + Sync {
    async fn pre_send_file(&self, recipient_mac: String, ori_path: String) -> Result<String, String>;
    async fn send_file(&self, transfer_id: String, file_path_str: String) -> Result<(), String>;
    async fn dispatch_file_chunks(&self, recipient_mac: &str, transfer_id: &str) -> Result<(), String>;
    async fn resume_pending_file_transfers(&self, peer_mac: &str);
    async fn handle_message_payload(&self, current_peer_mac: &str, conn: &Arc<PeerConnection>, env: &MessageEnvelope) -> Option<String>;
}
