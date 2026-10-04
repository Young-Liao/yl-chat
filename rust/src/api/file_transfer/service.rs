use super::traits::{FileTransferService, FileTransferServiceHandle};
use crate::api::connection::traits::{ConnectionManager, ConnectionManagerHandle};
use crate::api::models::{ChunkHeader, FileType, FileTransferRecord, MessageEnvelope, MessagePayload, MessageStatus, PersistentMessage};
use crate::api::storage::traits::{StorageRepository, StorageRepositoryHandle};
use crate::api::utils::chrono_now_timestamp;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use flutter_rust_bridge::frb;
use tracing::{info, warn};
use uuid::Uuid;


#[frb(ignore)]
pub struct DefaultFileTransferService {
    self_mac: String,
    storage: Arc<dyn StorageRepository>,
    connection_manager: Arc<dyn ConnectionManager>,
    event_emitter: Arc<dyn Fn(String) + Send + Sync>,
}

impl DefaultFileTransferService {
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
impl FileTransferService for DefaultFileTransferService {
    async fn send_file(&self, recipient_mac: String, file_path_str: String) -> Result<String, String> {
        let path = PathBuf::from(&file_path_str);
        if !path.exists() {
            return Err(format!("File does not exist: {}", file_path_str));
        }

        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown_file")
            .to_string();

        let metadata = fs::metadata(&path).map_err(|e| format!("Failed to read file metadata: {e}"))?;
        let file_size = metadata.len();

        let file_bytes = fs::read(&path).map_err(|e| format!("Failed to read file contents: {e}"))?;
        let mut hasher = Sha256::new();
        hasher.update(&file_bytes);
        let result = hasher.finalize();
        let file_hash: String = result.iter().map(|b| format!("{:02x}", b)).collect();

        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
        let file_type = match ext.as_str() {
            "jpg" | "jpeg" | "png" | "gif" | "webp" => FileType::Image,
            "mp4" | "mkv" | "mov" | "avi" => FileType::Video,
            _ => FileType::Generic,
        };

        const CHUNK_SIZE: usize = 64 * 1024;
        let total_chunks = if file_size == 0 {
            1
        } else {
            ((file_size as usize + CHUNK_SIZE - 1) / CHUNK_SIZE) as u32
        };

        let transfer_id = Uuid::new_v4().to_string();

        let record = FileTransferRecord {
            transfer_id: transfer_id.clone(),
            peer_mac: recipient_mac.clone(),
            file_type: file_type.clone(),
            file_name: file_name.clone(),
            file_size,
            total_chunks,
            received_chunks: 0,
            file_hash: file_hash.clone(),
            save_path: file_path_str.clone(),
            is_completed: false,
            is_outgoing: true,
        };
        self.storage.insert_file_transfer(record).await;

        let msg = PersistentMessage {
            msg_id: transfer_id.clone(),
            peer_mac: recipient_mac.clone(),
            is_outgoing: true,
            content: format!("[File: {}]", file_name),
            timestamp: chrono_now_timestamp(),
            status: MessageStatus::Pending,
            file_type: file_type.clone(),
            file_name: Some(file_name.clone()),
            file_size: Some(file_size),
            file_hash: Some(file_hash.clone()),
            file_path: Some(file_path_str),
        };
        self.storage.append_message(recipient_mac.clone(), msg).await;

        let ft = Arc::new(Self {
            self_mac: self.self_mac.clone(),
            storage: Arc::clone(&self.storage),
            connection_manager: Arc::clone(&self.connection_manager),
            event_emitter: Arc::clone(&self.event_emitter),
        });

        let recipient = recipient_mac.clone();
        let tid = transfer_id.clone();
        tokio::spawn(async move {
            if let Err(e) = ft.dispatch_file_chunks(&recipient, &tid).await {
                warn!(transfer_id = %tid, error = %e, "Initial file chunk dispatch failed (queued in outbox)");
            }
        });

        Ok(transfer_id)
    }

    async fn dispatch_file_chunks(&self, recipient_mac: &str, transfer_id: &str) -> Result<(), String> {
        let record = self
            .storage
            .get_file_transfer(transfer_id)
            .await
            .ok_or_else(|| format!("Transfer ID {} not found", transfer_id))?;

        let conn = self.connection_manager.get_or_connect(recipient_mac).await?;

        let init_env = MessageEnvelope {
            version: 1,
            msg_id: Uuid::new_v4().to_string(),
            sender_mac: self.self_mac.clone(),
            payload: MessagePayload::FileTransferInit {
                transfer_id: transfer_id.to_string(),
                file_type: record.file_type,
                file_name: record.file_name,
                file_size: record.file_size,
                total_chunks: record.total_chunks,
                file_hash: record.file_hash.clone(),
            },
        };
        conn.send_envelope(&init_env).await?;

        let acked_chunks = self.storage.get_acked_outbound_chunks(transfer_id).await;
        let path = PathBuf::from(&record.save_path);
        let file_bytes = fs::read(&path).map_err(|e| format!("Failed to read file contents: {e}"))?;

        const CHUNK_SIZE: usize = 64 * 1024;
        let mut offset: u64;

        for chunk_index in 0..record.total_chunks {
            let start = (chunk_index as usize) * CHUNK_SIZE;
            let end = std::cmp::min(start + CHUNK_SIZE, file_bytes.len());
            let chunk_data = file_bytes[start..end].to_vec();
            offset = start as u64;

            if acked_chunks.contains(&chunk_index) {
                continue;
            }

            let mut chunk_hasher = Sha256::new();
            chunk_hasher.update(&chunk_data);
            let result = chunk_hasher.finalize();
            let chunk_hash: String = result.iter().map(|b| format!("{:02x}", b)).collect();

            let chunk_header = ChunkHeader {
                transfer_id: transfer_id.to_string(),
                chunk_uuid: Uuid::new_v4().to_string(),
                chunk_index,
                total_chunks: record.total_chunks,
                offset,
                chunk_hash,
                file_hash: record.file_hash.clone(),
            };

            let chunk_env = MessageEnvelope {
                version: 1,
                msg_id: Uuid::new_v4().to_string(),
                sender_mac: self.self_mac.clone(),
                payload: MessagePayload::FileChunk {
                    header: chunk_header,
                    data: chunk_data,
                },
            };

            if let Err(e) = conn.send_envelope(&chunk_env).await {
                warn!(transfer_id = %transfer_id, chunk_index = chunk_index, error = %e, "Disconnect during chunk upload");
                return Err(e);
            }

            (self.event_emitter)(format!(
                "FILE_SEND_PROGRESS:{}:{}",
                transfer_id,
                ((chunk_index + 1) as f32 / record.total_chunks as f32 * 100.0) as u32
            ));
        }

        Ok(())
    }

    async fn resume_pending_file_transfers(&self, peer_mac: &str) {
        let pending_transfers = self.storage.get_pending_outbound_transfers(peer_mac).await;
        for record in pending_transfers {
            info!(transfer_id = %record.transfer_id, peer_mac = %peer_mac, "Resuming file transfer task...");
            let _ = self.dispatch_file_chunks(peer_mac, &record.transfer_id).await;
        }
    }
}

// =========================================================================
// FRB Opaque 包装层（供 Dart 端安全的生成和调用）
// =========================================================================

pub struct DefaultFileTransferServiceHandle {
    pub(crate) inner: Arc<DefaultFileTransferService>,
}

impl DefaultFileTransferServiceHandle {
    pub fn new(
        self_mac: String,
        storage_handle: &StorageRepositoryHandle,
        conn_manager_handle: &ConnectionManagerHandle,
    ) -> Self {
        let service = Arc::new(DefaultFileTransferService::new(
            self_mac,
            Arc::clone(&storage_handle.inner),
            Arc::clone(&conn_manager_handle.inner),
            Arc::new(|_event| {
                // 默认空事件发射器，如需向 Dart 推送事件建议使用 FRB 的 StreamSink
            }),
        ));
        Self { inner: service }
    }

    pub fn as_file_transfer_service(&self) -> FileTransferServiceHandle {
        FileTransferServiceHandle {
            inner: Arc::clone(&self.inner) as Arc<dyn FileTransferService>,
        }
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
