use async_trait::async_trait;
use std::sync::Arc;
use flutter_rust_bridge::frb;

#[frb(ignore)]
#[async_trait]
pub trait MessagingService: Send + Sync {
    async fn send_message(&self, recipient_mac: String, content: String) -> Result<(), String>;
    async fn flush_outbox(&self, peer_mac: &str);
}

// =========================================================================
// FRB Opaque 包装层（供 Dart 端安全的生成和调用）
// =========================================================================

pub struct MessagingServiceHandle {
    pub(crate) inner: Arc<dyn MessagingService>,
}

impl MessagingServiceHandle {
    pub fn new(inner: Arc<dyn MessagingService>) -> Self {
        Self { inner }
    }

    pub async fn send_message(&self, recipient_mac: String, content: String) -> Result<(), String> {
        self.inner.send_message(recipient_mac, content).await
    }

    pub async fn flush_outbox(&self, peer_mac: String) {
        self.inner.flush_outbox(&peer_mac).await;
    }
}
