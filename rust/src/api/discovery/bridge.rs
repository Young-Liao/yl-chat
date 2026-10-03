use crate::api::protocol::{chrono_now_timestamp, MessageStatus, NetworkEngine};
use crate::api::protocol::{PersistentMessage, PeerRecord};
use crate::frb_generated::StreamSink;

/// Periodic scan tick called by Flutter timer
pub async fn scan_and_flush(engine: NetworkEngine) {
    engine.run_scan_and_flush_cycle().await;
}

impl NetworkEngine {
    /// Exposes peer insertion directly to Dart
    pub async fn upsert_peer(&self, record: PeerRecord) {
        self.storage.upsert_peer(record).await;
    }

    /// Exposes peer fetching directly to Dart
    pub async fn get_peers(&self) -> Vec<PeerRecord> {
        self.storage.get_all_peers().await
    }

    /// Exposes chat retrieval directly to Dart
    pub async fn get_messages(&self, peer_mac: String) -> Vec<PersistentMessage> {
        self.storage.get_messages_for_peer(&peer_mac).await
    }
}
