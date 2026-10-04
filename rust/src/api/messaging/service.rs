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
            sent_chunks: 0,
            total_chunks: 0,
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