use crate::api::models::{FileTransferRecord, MessageStatus, PeerRecord, PersistentMessage};
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use flutter_rust_bridge::frb;

#[frb(ignore)]
#[async_trait]
pub trait StorageRepository: Send + Sync {
    // Peer 操作
    async fn upsert_peer(&self, record: PeerRecord);
    async fn set_peer_online_status(&self, mac: &str, is_online: bool);
    async fn get_all_peers(&self) -> Vec<PeerRecord>;
    async fn get_peer(&self, mac: &str) -> Option<PeerRecord>;

    // Message 操作
    async fn has_message(&self, msg_id: &str) -> bool;
    async fn append_message(&self, peer_mac: String, msg: PersistentMessage);
    async fn mark_acked(&self, peer_mac: &str, target_msg_id: &str) -> bool;
    async fn get_messages_for_peer(&self, peer_mac: &str) -> Vec<PersistentMessage>;
    async fn get_pending_messages(&self, peer_mac: &str) -> Vec<PersistentMessage>;
    async fn update_message_progress(&self, msg_id: &str, sent_chunks: u32);

    // FileTransfer 操作
    async fn insert_file_transfer(&self, record: FileTransferRecord);
    async fn record_chunk_received(&self, transfer_id: String, chunk_index: u32) -> Result<u32, String>;
    async fn record_outbound_chunk_ack(&self, transfer_id: String, chunk_index: u32);
    async fn get_acked_outbound_chunks(&self, transfer_id: &str) -> HashSet<u32>;
    async fn clear_file_transfer_trackers(&self, transfer_id: &str);
    async fn mark_transfer_completed(&self, transfer_id: &str);
    async fn get_pending_outbound_transfers(&self, peer_mac: &str) -> Vec<FileTransferRecord>;
    async fn get_file_transfer(&self, transfer_id: &str) -> Option<FileTransferRecord>;
    async fn update_transfer_progress(&self, transfer_id: &str, received_chunks: u32) -> Result<(), String>;

    async fn get_transfer_progress(&self, transfer_id: &str) -> Option<(u32, u32)>; // (received_chunks, total_chunks)
}

// =========================================================================
// FRB Opaque 包装层（供 Dart 端安全的生成和调用）
// =========================================================================

pub struct StorageRepositoryHandle {
    pub(crate) inner: Arc<dyn StorageRepository>,
}

impl StorageRepositoryHandle {
    pub fn new(inner: Arc<dyn StorageRepository>) -> Self {
        Self { inner }
    }

    // Peer 操作
    pub async fn upsert_peer(&self, record: PeerRecord) {
        self.inner.upsert_peer(record).await;
    }

    pub async fn set_peer_online_status(&self, mac: String, is_online: bool) {
        self.inner.set_peer_online_status(&mac, is_online).await;
    }

    pub async fn get_all_peers(&self) -> Vec<PeerRecord> {
        self.inner.get_all_peers().await
    }

    pub async fn get_peer(&self, mac: String) -> Option<PeerRecord> {
        self.inner.get_peer(&mac).await
    }

    // Message 操作
    pub async fn has_message(&self, msg_id: String) -> bool {
        self.inner.has_message(&msg_id).await
    }

    pub async fn append_message(&self, peer_mac: String, msg: PersistentMessage) {
        self.inner.append_message(peer_mac, msg).await;
    }

    pub async fn mark_acked(&self, peer_mac: String, target_msg_id: String) -> bool {
        self.inner.mark_acked(&peer_mac, &target_msg_id).await
    }

    pub async fn get_messages_for_peer(&self, peer_mac: String) -> Vec<PersistentMessage> {
        self.inner.get_messages_for_peer(&peer_mac).await
    }

    pub async fn get_pending_messages(&self, peer_mac: String) -> Vec<PersistentMessage> {
        self.inner.get_pending_messages(&peer_mac).await
    }

    // FileTransfer 操作
    pub async fn insert_file_transfer(&self, record: FileTransferRecord) {
        self.inner.insert_file_transfer(record).await;
    }

    pub async fn record_chunk_received(&self, transfer_id: String, chunk_index: u32) -> Result<u32, String> {
        self.inner.record_chunk_received(transfer_id, chunk_index).await
    }

    pub async fn record_outbound_chunk_ack(&self, transfer_id: String, chunk_index: u32) {
        self.inner.record_outbound_chunk_ack(transfer_id, chunk_index).await;
    }

    pub async fn get_acked_outbound_chunks(&self, transfer_id: String) -> Vec<u32> {
        self.inner.get_acked_outbound_chunks(&transfer_id).await.into_iter().collect()
    }

    pub async fn clear_file_transfer_trackers(&self, transfer_id: String) {
        self.inner.clear_file_transfer_trackers(&transfer_id).await;
    }

    pub async fn mark_transfer_completed(&self, transfer_id: String) {
        self.inner.mark_transfer_completed(&transfer_id).await;
    }

    pub async fn get_pending_outbound_transfers(&self, peer_mac: String) -> Vec<FileTransferRecord> {
        self.inner.get_pending_outbound_transfers(&peer_mac).await
    }

    pub async fn get_file_transfer(&self, transfer_id: String) -> Option<FileTransferRecord> {
        self.inner.get_file_transfer(&transfer_id).await
    }
}
