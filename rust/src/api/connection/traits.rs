use async_trait::async_trait;
use std::sync::Arc;
use flutter_rust_bridge::frb;
use crate::api::connection::manager::PeerConnection;
use crate::api::models::MessageEnvelope;

#[frb(ignore)]
#[async_trait]
pub trait ConnectionManager: Send + Sync {
    async fn get_or_connect(&self, peer_mac: &str) -> Result<Arc<PeerConnection>, String>;
    async fn remove_connection(&self, peer_mac: &str);
    async fn has_connection(&self, peer_mac: &str) -> bool;
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

// 2. 包装 ConnectionManager Trait Object 为 FRB 兼容的不透明句柄
pub struct ConnectionManagerHandle {
    pub(crate) inner: Arc<dyn ConnectionManager>,
}

impl ConnectionManagerHandle {
    pub fn new(inner: Arc<dyn ConnectionManager>) -> Self {
        Self { inner }
    }

    pub async fn get_or_connect(&self, peer_mac: String) -> Result<PeerConnectionHandle, String> {
        let conn = self.inner.get_or_connect(&peer_mac).await?;
        Ok(PeerConnectionHandle { inner: conn })
    }

    pub async fn remove_connection(&self, peer_mac: String) {
        self.inner.remove_connection(&peer_mac).await;
    }

    pub async fn has_connection(&self, peer_mac: String) -> bool {
        self.inner.has_connection(&peer_mac).await
    }
}
