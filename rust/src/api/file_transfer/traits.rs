use async_trait::async_trait;
use std::sync::Arc;
use flutter_rust_bridge::frb;

#[frb(ignore)]
#[async_trait]
pub trait FileTransferService: Send + Sync {
    async fn send_file(&self, recipient_mac: String, file_path_str: String) -> Result<String, String>;
    async fn dispatch_file_chunks(&self, recipient_mac: &str, transfer_id: &str) -> Result<(), String>;
    async fn resume_pending_file_transfers(&self, peer_mac: &str);
}

// =========================================================================
// FRB Opaque 包装层（供 Dart 端安全的生成和调用）
// =========================================================================

pub struct FileTransferServiceHandle {
    pub(crate) inner: Arc<dyn FileTransferService>,
}

impl FileTransferServiceHandle {
    pub fn new(inner: Arc<dyn FileTransferService>) -> Self {
        Self { inner }
    }

    pub async fn send_file(&self, recipient_mac: String, file_path_str: String) -> Result<String, String> {
        self.inner.send_file(recipient_mac, file_path_str).await
    }

    pub async fn dispatch_file_chunks(&self, recipient_mac: String, transfer_id: String) -> Result<(), String> {
        self.inner.dispatch_file_chunks(&recipient_mac, &transfer_id).await
    }

    pub async fn resume_pending_file_transfers(&self, peer_mac: String) {
        self.inner.resume_pending_file_transfers(&peer_mac).await;
    }
}
