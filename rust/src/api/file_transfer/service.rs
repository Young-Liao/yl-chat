use super::traits::{FileTransferService, FileTransferServiceHandle};
use crate::api::connection::traits::{ConnectionManager, ConnectionManagerHandle};
use crate::api::models::{
    ChunkHeader, FileType, FileTransferRecord, MessageEnvelope, MessagePayload, MessageStatus, PersistentMessage,
};
use crate::api::storage::traits::{StorageRepository, StorageRepositoryHandle};
use crate::api::utils::chrono_now_timestamp;
use async_trait::async_trait;
use flutter_rust_bridge::frb;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};
use uuid::Uuid;

/// 利用 RAII Guard 模式，确保函数退出（无论正常 return 还是 `?` 抛错退出）时都能自动释放锁
struct TransferGuard {
    transfer_id: String,
    active_transfers: Arc<Mutex<HashSet<String>>>,
}

impl Drop for TransferGuard {
    fn drop(&mut self) {
        let active_transfers = Arc::clone(&self.active_transfers);
        let transfer_id = self.transfer_id.clone();
        // 在异步任务中锁住 HashSet 并移除 transfer_id
        tokio::spawn(async move {
            let mut active = active_transfers.lock().await;
            active.remove(&transfer_id);
        });
    }
}

#[frb(ignore)]
pub struct DefaultFileTransferService {
    self_mac: String,
    storage: Arc<dyn StorageRepository>,
    connection_manager: Arc<dyn ConnectionManager>,
    event_emitter: Arc<dyn Fn(String) + Send + Sync>,
    /// 记录当前正在传输中的 transfer_id 集合，防止并发重复调度
    active_transfers: Arc<Mutex<HashSet<String>>>,
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
            active_transfers: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// 复制 Service 实例（便于在 tokio::spawn 中异步调用）
    fn clone_service(&self) -> Self {
        Self {
            self_mac: self.self_mac.clone(),
            storage: Arc::clone(&self.storage),
            connection_manager: Arc::clone(&self.connection_manager),
            event_emitter: Arc::clone(&self.event_emitter),
            active_transfers: Arc::clone(&self.active_transfers),
        }
    }
}

#[frb(ignore)]
#[async_trait]
impl FileTransferService for DefaultFileTransferService {
    /// 1. 发送文件入口：仅做本地持久化（落盘），随后触发 Outbox 刷新
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

        const CHUNK_SIZE: usize = 64 * 1024; // 64KB
        let total_chunks = if file_size == 0 {
            1
        } else {
            ((file_size as usize + CHUNK_SIZE - 1) / CHUNK_SIZE) as u32
        };

        let transfer_id = Uuid::new_v4().to_string();

        // A. 持久化 FileTransferRecord
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

        // B. 持久化 PersistentMessage (Status 为 Pending，sent_chunks 为 0)
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
            sent_chunks: 0, // 初始为 0
            total_chunks,
        };
        self.storage.append_message(recipient_mac.clone(), msg).await;

        // C. Outbox 即时异步触发刷新
        let ft = Arc::new(self.clone_service());
        let recipient = recipient_mac.clone();
        let tid = transfer_id.clone();

        tokio::spawn(async move {
            if let Err(e) = ft.dispatch_file_chunks(&recipient, &tid).await {
                warn!(
                    transfer_id = %tid,
                    error = %e,
                    "Initial file chunk dispatch failed, remaining chunks queued in Outbox"
                );
            }
        });

        Ok(transfer_id)
    }

    /// 2. Outbox 核心分片推送与重传机制（支持断点续传与消息进度更新）
    async fn dispatch_file_chunks(&self, recipient_mac: &str, transfer_id: &str) -> Result<(), String> {
        // --- 核心防重逻辑开始 ---
        {
            let mut active = self.active_transfers.lock().await;
            if active.contains(transfer_id) {
                info!(
                    transfer_id = %transfer_id,
                    "File transfer task is already running in another thread, skipping redundant dispatch"
                );
                return Ok(());
            }
            active.insert(transfer_id.to_string());
        }

        // 绑定 Guard，当前函数无论是正常 return 还是 Err 退出，都会自动解除锁定
        let _guard = TransferGuard {
            transfer_id: transfer_id.to_string(),
            active_transfers: Arc::clone(&self.active_transfers),
        };
        // --- 核心防重逻辑结束 ---

        let record = self
            .storage
            .get_file_transfer(transfer_id)
            .await
            .ok_or_else(|| format!("Transfer ID {} not found", transfer_id))?;

        if record.is_completed {
            info!(transfer_id = %transfer_id, "File transfer already completed, skipping");
            return Ok(());
        }

        // 尝试建立或获取连接，若失败则保留在 Outbox 等待下一次周期刷新
        let conn = self.connection_manager.get_or_connect(recipient_mac).await?;

        // 步骤 1：发送初始化协议包 FileTransferInit
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

        // 步骤 2：从数据库查询已获得对方 Ack 确认的分片集合
        let acked_chunks = self.storage.get_acked_outbound_chunks(transfer_id).await;

        let path = PathBuf::from(&record.save_path);
        let file_bytes = fs::read(&path).map_err(|e| format!("Failed to read file contents: {e}"))?;

        const CHUNK_SIZE: usize = 64 * 1024;

        // 步骤 3：遍历所有 Chunk，自动跳过已 Ack 的分片
        for chunk_index in 0..record.total_chunks {
            if acked_chunks.contains(&chunk_index) {
                continue;
            }

            let start = (chunk_index as usize) * CHUNK_SIZE;
            let end = std::cmp::min(start + CHUNK_SIZE, file_bytes.len());
            let chunk_data = file_bytes[start..end].to_vec();
            let offset = start as u64;

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

            // 发送网络数据包
            if let Err(e) = conn.send_envelope(&chunk_env).await {
                warn!(
                    transfer_id = %transfer_id,
                    chunk_index = chunk_index,
                    error = %e,
                    "Disconnect during chunk upload"
                );
                // 网络异常，退出本次 flush，已发出的分片会在下次连接恢复时跳过
                self.connection_manager.remove_connection(recipient_mac).await;
                self.storage.set_peer_online_status(recipient_mac, false).await;
                (self.event_emitter)("PEER_LIST_UPDATED".to_string());
                return Err(e);
            }

            // 更新数据记录：不仅更新传输表，同时更新 PersistentMessage 表的发送进度 sent_chunks
            let updated_sent_count = chunk_index + 1;

            // 向 Flutter / UI 发送发送进度通知
            let progress_pct = ((updated_sent_count as f32 / record.total_chunks as f32) * 100.0) as u32;
            (self.event_emitter)(format!("FILE_SEND_PROGRESS:{}:{}", transfer_id, progress_pct));
        }

        Ok(())
    }

    /// 3. Outbox 轮询/恢复接口：对指定 Peer 的所有未完成发送任务进行批量刷新
    async fn resume_pending_file_transfers(&self, peer_mac: &str) {
        let pending_transfers = self.storage.get_pending_outbound_transfers(peer_mac).await;

        if pending_transfers.is_empty() {
            return;
        }

        info!(
            peer_mac = %peer_mac,
            count = pending_transfers.len(),
            "Flushing pending outbound file transfers"
        );

        for record in pending_transfers {
            info!(transfer_id = %record.transfer_id, peer_mac = %peer_mac, "Resuming file transfer task...");
            if let Err(e) = self.dispatch_file_chunks(peer_mac, &record.transfer_id).await {
                error!(transfer_id = %record.transfer_id, error = %e, "Failed to resume file transfer");
                break; // 如果网络失败则中断后续处理
            }
        }
    }
}
