use crate::api::protocol::{chrono_now_timestamp, MessageStatus, NetworkEngine};
use crate::api::protocol::{PersistentMessage, PeerRecord};
use crate::frb_generated::StreamSink;

/// Periodic scan tick called by Flutter timer
pub async fn scan_and_flush(engine: NetworkEngine) {
    engine.run_scan_and_flush_cycle().await;
}

