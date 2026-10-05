use crate::api::connection::manager::{PeerConnection, TcpConnectionManager};
use crate::api::connection::traits::ConnectionManager;
use crate::api::file_transfer::service::{DefaultFileTransferService};
use crate::api::file_transfer::traits::{FileTransferService};
use crate::api::messaging::service::{DefaultMessagingService};
use crate::api::messaging::traits::{MessagingService};
use crate::api::models::{
    FileType, FileTransferRecord, MessageEnvelope, MessagePayload, MessageStatus, PeerRecord,
    PersistentMessage,
};
use crate::api::storage::sqlite::{LocalStorage};
use crate::api::storage::traits::{StorageRepository};
use crate::api::utils::chrono_now_timestamp;

use crate::api::discovery::ProtocolConfig;
use crate::frb_generated::StreamSink;
use flutter_rust_bridge::frb;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{debug, error, info};
use uuid::Uuid;

#[frb(ignore)]
#[derive(Clone)]
pub struct NetworkEngine {
    pub self_mac: String,
    pub storage: Arc<dyn StorageRepository>,
    pub connection_manager: Arc<TcpConnectionManager>,
    pub messaging_service: Arc<dyn MessagingService>,
    pub file_transfer_service: Arc<dyn FileTransferService>,
    notify_sink: Arc<RwLock<Option<StreamSink<String>>>>,
}

impl NetworkEngine {
    pub async fn new(
        self_mac: String,
        db_path_str: String,
    ) -> Result<Arc<Self>, String> {
        let db_path = PathBuf::from(db_path_str);

        info!(
            self_mac = %self_mac,
            db_path = ?db_path,
            "Initializing Modular NetworkEngine"
        );

        // 1. Storage
        let storage = Arc::new(
            LocalStorage::open(db_path).await?
        );

        // 2. Notification sink
        let notify_sink =
            Arc::new(RwLock::new(None::<StreamSink<String>>));

        // 3. Event emitter
        let notify_sink_clone = Arc::clone(&notify_sink);

        let event_emitter = Arc::new(move |event: String| {
            let sink_guard = Arc::clone(&notify_sink_clone);

            tokio::spawn(async move {
                if let Some(ref sink) = *sink_guard.read().await {
                    let _ = sink.add(event);
                }
            });
        });

        // 4. Connection manager
        let connection_manager = Arc::new(
            TcpConnectionManager::new(
                self_mac.clone(),
                Arc::clone(&storage) as Arc<dyn StorageRepository>,
            )
        );

        // 5. Messaging service
        let messaging_service = Arc::new(
            DefaultMessagingService::new(
                self_mac.clone(),
                Arc::clone(&storage) as Arc<dyn StorageRepository>,
                Arc::clone(&connection_manager) as Arc<dyn ConnectionManager>,
                event_emitter.clone(),
            )
        );

        // 6. File transfer service
        let file_transfer_service = Arc::new(
            DefaultFileTransferService::new(
                self_mac.clone(),
                Arc::clone(&storage) as Arc<dyn StorageRepository>,
                Arc::clone(&connection_manager) as Arc<dyn ConnectionManager>,
                event_emitter.clone(),
            )
        );

        // 7. 创建真正长期存在的 Arc<NetworkEngine>
        //
        // 后面所有 Weak<NetworkEngine>
        // 都必须指向这个 Arc。
        let engine = Arc::new(Self {
            self_mac,
            storage,
            connection_manager: Arc::clone(&connection_manager),
            messaging_service,
            file_transfer_service,
            notify_sink,
        });

        // 8. 给 ConnectionManager 保存同一个 Arc 的 Weak
        //
        // 注意：
        // 这里不能创建一个临时 Arc，然后最后 clone 出一个普通
        // NetworkEngine 再重新 Arc::new。
        connection_manager
            .set_engine(Arc::downgrade(&engine))
            .await;

        // 9. 直接返回这个 Arc
        Ok(engine)
    }

    pub async fn register_notify_sink(&self, sink: StreamSink<String>) {
        info!("Registering StreamSink for Flutter events");
        let mut guard = self.notify_sink.write().await;
        *guard = Some(sink);
    }

    pub async fn emit_event(&self, event: String) {
        if let Some(ref sink) = *self.notify_sink.read().await {
            debug!(event = %event, "Emitting event to Dart");
            let _ = sink.add(event);
        }
    }

    // 委派核心 API 接口
    pub async fn upsert_peer(&self, record: PeerRecord) {
        self.storage.upsert_peer(record).await;
    }

    pub async fn get_peers(&self) -> Vec<PeerRecord> {
        self.storage.get_all_peers().await
    }

    pub async fn get_messages(&self, peer_mac: String) -> Vec<PersistentMessage> {
        self.storage.get_messages_for_peer(&peer_mac).await
    }

    pub async fn send_message(&self, recipient_mac: String, content: String) -> Result<(), String> {
        self.messaging_service.send_message(recipient_mac, content).await
    }

    pub async fn pre_send_file(&self, recipient_mac: String, ori_path: String) -> Result<String, String> {
        self.file_transfer_service.pre_send_file(recipient_mac, ori_path).await
    }

    pub async fn send_file(&self, transfer_id: String, file_path_str: String) -> Result<(), String> {
        self.file_transfer_service.send_file(transfer_id, file_path_str).await
    }

    pub async fn run_scan_and_flush_cycle(&self) {
        info!("Starting peer status refresh and outbox flush cycle");

        let historical_peers = self.storage.get_all_peers().await;

        let discovered_peers = crate::api::discovery::scan_lan_peers(self.self_mac.clone())
            .await
            .unwrap_or_else(|e| {
                error!(error = ?e, "mDNS scan failed, continuing with cached records");
                Vec::new()
            });

        let discovered_map: HashMap<String, _> = discovered_peers
            .into_iter()
            .map(|p| (p.mac_address.clone(), p))
            .collect();

        for mut peer_record in historical_peers {
            let mac = peer_record.mac_address.clone();

            if let Some(online_peer) = discovered_map.get(&mac) {
                peer_record.last_known_ip = online_peer.ip.clone();
                peer_record.device_name = online_peer.device_name.clone();
                peer_record.last_seen = chrono_now_timestamp();
                peer_record.is_online = true;
                self.storage.upsert_peer(peer_record).await;
                self.file_transfer_service.resume_pending_file_transfers(&mac).await;
                self.messaging_service.flush_outbox(&mac).await;
            } else {
                peer_record.is_online = false;
                self.storage.upsert_peer(peer_record).await;
            }
        }

        for (mac, online_peer) in discovered_map {
            if self.storage.get_peer(&mac).await.is_none() {
                let new_record = PeerRecord {
                    mac_address: mac.clone(),
                    last_known_ip: online_peer.ip,
                    port: ProtocolConfig::SERVICE_PORT,
                    device_name: online_peer.device_name,
                    last_seen: chrono_now_timestamp(),
                    is_online: true,
                };
                self.storage.upsert_peer(new_record).await;
                self.file_transfer_service.resume_pending_file_transfers(&mac).await;
                self.messaging_service.flush_outbox(&mac).await;
            }
        }

        self.emit_event("PEER_LIST_UPDATED".to_string()).await;
    }

    pub async fn start_listener(
        &self,
        port: u16,
        notify_sink: StreamSink<String>,
    ) -> Result<(), String> {
        self.register_notify_sink(notify_sink).await;

        let engine = self.clone();
        let addr = format!("0.0.0.0:{}", port);
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| format!("Bind error: {e}"))?;

        info!(address = %addr, "TCP listener bound successfully");

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer_addr)) => {
                        let ip = peer_addr.ip().to_string();
                        let engine_clone = engine.clone();
                        tokio::spawn(async move {
                            engine_clone.handle_incoming_stream(stream, ip).await;
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "TCP accept failed");
                    }
                }
            }
        });

        Ok(())
    }

    async fn handle_incoming_stream(&self, stream: tokio::net::TcpStream, ip: String) {
        let (read_half, mut write_half) = stream.into_split();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<MessageEnvelope>(100);

        let conn = Arc::new(PeerConnection {
            remote_mac: RwLock::new(String::new()),
            remote_ip: ip.clone(),
            tx: tx.clone(),
        });

        let ip_writer = ip.clone();
        // 启动写入协程
        tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                let bytes = match serde_json::to_vec(&envelope) {
                    Ok(b) => b,
                    Err(e) => {
                        error!(error = %e, "Failed to serialize envelope");
                        continue;
                    }
                };

                let len = (bytes.len() as u32).to_be_bytes();
                if write_half.write_all(&len).await.is_err()
                    || write_half.write_all(&bytes).await.is_err()
                    || write_half.flush().await.is_err()
                {
                    error!(ip = %ip_writer, "Socket write error");
                    break;
                }
            }
        });

        // 启动真正的读取与数据报文分发循环
        let engine = self.clone();
        tokio::spawn(async move {
            engine
                .handle_read_loop(read_half, conn, String::new(), ip)
                .await;
        });
    }

    pub(crate) async fn handle_read_loop(
        &self,
        mut read_half: tokio::net::tcp::OwnedReadHalf,
        conn: Arc<PeerConnection>,
        mut current_peer_mac: String,
        current_peer_ip: String,
    ) {
        loop {
            let mut len_bytes = [0u8; 4];
            if read_half.read_exact(&mut len_bytes).await.is_err() {
                break;
            }

            let len = u32::from_be_bytes(len_bytes) as usize;
            let mut buffer = vec![0u8; len];
            if read_half.read_exact(&mut buffer).await.is_err() {
                break;
            }

            let env: MessageEnvelope = match serde_json::from_slice(&buffer) {
                Ok(env) => env,
                Err(e) => {
                    error!(error = %e, "Deserialization failed");
                    continue;
                }
            };

            let message = match env.payload {
                MessagePayload::Handshake { .. }  => self.connection_manager.handle_message_payload(&conn, &mut current_peer_mac, &env).await,
                MessagePayload::ChatMessage { .. } => {
                    if current_peer_mac.is_empty() {
                        current_peer_mac = env.sender_mac.clone();
                        *conn.remote_mac.write().await = env.sender_mac.clone();
                        self.connection_manager.insert_connection(env.sender_mac.clone(), Arc::clone(&conn)).await;
                        self.storage.set_peer_online_status(&env.sender_mac, true).await;
                        self.emit_event("PEER_LIST_UPDATED".to_string()).await;
                    }
                    self.messaging_service.handle_message_payload(&conn, &current_peer_mac, &env).await
                }
                MessagePayload::Ack { .. } => self.messaging_service.handle_message_payload(&conn, &current_peer_mac, &env).await,
                MessagePayload::FileTransferInit { .. } |
                MessagePayload::FileChunk { .. } |
                MessagePayload::ChunkAck { .. } |
                MessagePayload::FileCompleteAck { .. } => self.file_transfer_service.handle_message_payload(&current_peer_mac, &conn, &env).await
            };

            if let Some(message) = message {
                self.emit_event(message).await;
            }
        }

        if !current_peer_mac.is_empty() {
            self.connection_manager.remove_connection(&current_peer_mac).await;
            self.storage
                .set_peer_online_status(&current_peer_mac, false)
                .await;
            self.emit_event("PEER_LIST_UPDATED".to_string()).await;
            info!(peer_mac = %current_peer_mac, ip = %current_peer_ip, "Closed peer connection removed and marked offline");
        }
    }

    pub async fn flush_outbox(&self, peer_mac: String) {
        self.messaging_service.flush_outbox(&peer_mac).await;
        self.file_transfer_service.resume_pending_file_transfers(&peer_mac).await;
    }
}

// =========================================================================
// FRB Opaque 包装层（供 Dart 端安全的生成和调用）
// =========================================================================

pub struct NetworkEngineHandle {
    pub(crate) inner: Arc<NetworkEngine>,
}

impl NetworkEngineHandle {
    pub async fn new(
        self_mac: String,
        db_path_str: String,
    ) -> Result<Self, String> {
        let engine = NetworkEngine::new(
            self_mac,
            db_path_str,
        )
            .await?;

        Ok(Self {
            inner: engine,
        })
    }

    pub async fn register_notify_sink(&self, sink: StreamSink<String>) {
        self.inner.register_notify_sink(sink).await;
    }

    pub async fn emit_event(&self, event: String) {
        self.inner.emit_event(event).await;
    }

    pub async fn upsert_peer(&self, record: PeerRecord) {
        self.inner.upsert_peer(record).await;
    }

    pub async fn get_peers(&self) -> Vec<PeerRecord> {
        self.inner.get_peers().await
    }

    pub async fn get_messages(&self, peer_mac: String) -> Vec<PersistentMessage> {
        self.inner.get_messages(peer_mac).await
    }

    pub async fn send_message(&self, recipient_mac: String, content: String) -> Result<(), String> {
        self.inner.send_message(recipient_mac, content).await
    }

    pub async fn pre_send_file(&self, recipient_mac: String, ori_path: String) -> Result<String, String> {
        self.inner.pre_send_file(recipient_mac, ori_path).await
    }

    pub async fn send_file(&self, transfer_id: String, file_path_str: String) -> Result<(), String> {
        self.inner.send_file(transfer_id, file_path_str).await
    }

    pub async fn run_scan_and_flush_cycle(&self) {
        self.inner.run_scan_and_flush_cycle().await;
    }

    pub async fn start_listener(&self, port: u16, notify_sink: StreamSink<String>) -> Result<(), String> {
        self.inner.start_listener(port, notify_sink).await
    }

    pub async fn flush_outbox(&self, peer_mac: String) {
        self.inner.flush_outbox(peer_mac).await;
    }
}

// TODO: pre send file