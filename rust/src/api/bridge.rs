use crate::api::engine::NetworkEngineHandle;

/// Periodic scan tick called by Flutter timer
pub async fn scan_and_flush(engine: NetworkEngineHandle) {
    engine.run_scan_and_flush_cycle().await;
}

