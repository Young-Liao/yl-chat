use crate::api::protocol::{chrono_now_timestamp, MessageStatus, NetworkEngine};
use crate::api::protocol::{PersistentMessage, PeerRecord};
use crate::frb_generated::StreamSink;

/// Send message from UI: saves locally, notifies UI, then attempts flush
pub async fn send_message(
    engine: NetworkEngine,
    recipient_mac: String,
    content: String,
) -> Result<(), String> {
    let msg = PersistentMessage {
        msg_id: uuid::Uuid::new_v4().to_string(),
        peer_mac: recipient_mac.clone(),
        is_outgoing: true,
        content,
        timestamp: chrono_now_timestamp(),
        status: MessageStatus::Pending,
    };

    engine.storage.append_message(recipient_mac.clone(), msg).await;
    engine.flush_outbox(&recipient_mac).await;
    Ok(())
}

/// Called by Flutter to pull chat history for a specific conversation peer
pub async fn get_messages(engine: NetworkEngine, peer_mac: String) -> Vec<PersistentMessage> {
    engine.storage.get_messages_for_peer(&peer_mac).await
}

/// Called by Flutter to get all active peer records
pub async fn get_peers(engine: NetworkEngine) -> Vec<PeerRecord> {
    engine.storage.get_all_peers().await
}

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
