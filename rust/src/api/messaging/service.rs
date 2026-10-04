use super::traits::{MessagingService, MessagingServiceHandle};
use crate::api::connection::traits::{ConnectionManager, ConnectionManagerHandle};
use crate::api::models::{FileType, MessageEnvelope, MessagePayload, MessageStatus, PersistentMessage};
use crate::api::storage::traits::{StorageRepository, StorageRepositoryHandle};
use crate::api::utils::chrono_now_timestamp;
use async_trait::async_trait;
use std::sync::Arc;
use flutter_rust_bridge::frb;
use tracing::{error, info, warn};
use uuid::Uuid;

#[frb(ignore)]
pub struct DefaultMessagingService {
    self_mac: String,
    storage: Arc<dyn StorageRepository>,
    connection_manager: Arc<dyn ConnectionManager>,
    event_emitter: Arc<dyn Fn(String) + Send + Sync>,
}

impl DefaultMessagingService {
    pub fn new(
        self_mac: String,
        storage: Arc<dyn StorageRepository>,
        connection_manager: Arc<dyn ConnectionManager>,
        event_emitter: Arc<dyn Fn(String) + Send + Sync>,
    ) -> Self {
        Self {
            self_mac,
            storage,
            connection_manager,
            event_emitter,
        }
    }
}

#[frb(ignore)]
#[async_trait]
impl MessagingService for DefaultMessagingService {
    async fn send_message(&self, recipient_mac: String, content: String) -> Result<(), String> {
        let msg_id = Uuid::new_v4().to_string();
        info!(msg_id = %msg_id, recipient = %recipient_mac, "Queueing outgoing chat message");

        let msg = PersistentMessage {
            msg_id,
            peer_mac: recipient_mac.clone(),
            is_outgoing: true,
            content,
            timestamp: chrono_now_timestamp(),
            status: MessageStatus::Pending,
            file_type: FileType::None,
            file_name: None,
            file_size: None,
            file_hash: None,
            file_path: None,
        };

        self.storage
            .append_message(recipient_mac.clone(), msg)
            .await;

        let messaging = Arc::new(self.clone_service());
        let recipient = recipient_mac.clone();
        tokio::spawn(async move {
            messaging.flush_outbox(&recipient).await;
        });

        Ok(())
    }

    async fn flush_outbox(&self, peer_mac: &str) {
        let pending = self.storage.get_pending_messages(peer_mac).await;

        if pending.is_empty() {
            return;
        }

        info!(peer_mac = %peer_mac, count = pending.len(), "Flushing pending outbox messages");

        match self.connection_manager.get_or_connect(peer_mac).await {
            Ok(conn) => {
                for msg in pending {
                    let env = MessageEnvelope {
                        version: 1,
                        msg_id: msg.msg_id.clone(),
                        sender_mac: self.self_mac.clone(),
                        payload: MessagePayload::ChatMessage {
                            content: msg.content.clone(),
                        },
                    };

                    if let Err(e) = conn.send_envelope(&env).await {
                        error!(msg_id = %msg.msg_id, error = %e, "Failed to send message over socket");
                        self.connection_manager.remove_connection(peer_mac).await;
                        self.storage.set_peer_online_status(peer_mac, false).await;
                        (self.event_emitter)("PEER_LIST_UPDATED".to_string());
                        break;
                    } else {
                        (self.event_emitter)(format!("MSG_SENT:{}", peer_mac));
                    }
                }
            }
            Err(e) => {
                warn!(peer_mac = %peer_mac, error = %e, "Unable to flush outbox: connection failed");
            }
        }
    }
}

impl DefaultMessagingService {
    fn clone_service(&self) -> Self {
        Self {
            self_mac: self.self_mac.clone(),
            storage: Arc::clone(&self.storage),
            connection_manager: Arc::clone(&self.connection_manager),
            event_emitter: Arc::clone(&self.event_emitter),
        }
    }
}

// =========================================================================
// FRB Opaque 包装层（供 Dart 端安全的生成和调用）
// =========================================================================

pub struct DefaultMessagingServiceHandle {
    pub(crate) inner: Arc<DefaultMessagingService>,
}

impl DefaultMessagingServiceHandle {
    pub fn new(
        self_mac: String,
        storage_handle: &StorageRepositoryHandle,
        conn_manager_handle: &ConnectionManagerHandle,
    ) -> Self {
        let service = Arc::new(DefaultMessagingService::new(
            self_mac,
            Arc::clone(&storage_handle.inner),
            Arc::clone(&conn_manager_handle.inner),
            Arc::new(|_event| {
                // 默认空事件发射器，如需向 Dart 推送事件建议使用 FRB 的 StreamSink
            }),
        ));
        Self { inner: service }
    }

    pub fn as_messaging_service(&self) -> MessagingServiceHandle {
        MessagingServiceHandle {
            inner: Arc::clone(&self.inner) as Arc<dyn MessagingService>,
        }
    }

    pub async fn send_message(&self, recipient_mac: String, content: String) -> Result<(), String> {
        self.inner.send_message(recipient_mac, content).await
    }

    pub async fn flush_outbox(&self, peer_mac: String) {
        self.inner.flush_outbox(&peer_mac).await;
    }
}
