use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use flutter_rust_bridge::frb;
use crate::api::discovery::ProtocolConfig;
use crate::frb_generated::StreamSink;
use rusqlite::{params};
use std::path::PathBuf;
use tokio_rusqlite::Connection as AsyncConnection;

// ==========================================
// 1. Models
// ==========================================

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MessageStatus {
    Pending,
    Acked,
}

impl MessageStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageStatus::Pending => "PENDING",
            MessageStatus::Acked => "ACKED",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "ACKED" => MessageStatus::Acked,
            _ => MessageStatus::Pending,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistentMessage {
    pub msg_id: String,
    pub peer_mac: String,
    pub is_outgoing: bool,
    pub content: String,
    pub timestamp: i64,
    pub status: MessageStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerRecord {
    pub mac_address: String,
    pub last_known_ip: String,
    pub port: u16,
    pub device_name: String,
    pub last_seen: i64,
}

// ==========================================
// 2. Disk Storage Engine (SQLite)
// ==========================================

pub struct LocalStorage {
    db: AsyncConnection,
}

impl LocalStorage {
    /// 初始化磁盘数据库（传入本地 sqlite 文件路径，例如 Flutter 导出的 app_doc_dir + "/chat.db"）
    pub async fn open(db_path: PathBuf) -> Result<Self, String> {
        info!(path = ?db_path, "Opening SQLite database on disk");

        let db = AsyncConnection::open(db_path)
            .await
            .map_err(|e| format!("Failed to open sqlite database: {e}"))?;

        // 建表并配置 WAL 模式（提高并发读写性能）
        db.call(|conn| {
            conn.execute_batch(
                "
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;

                CREATE TABLE IF NOT EXISTS peers (
                    mac_address TEXT PRIMARY KEY,
                    last_known_ip TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    device_name TEXT NOT NULL,
                    last_seen INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS messages (
                    msg_id TEXT PRIMARY KEY,
                    peer_mac TEXT NOT NULL,
                    is_outgoing INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    status TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_messages_peer_mac ON messages(peer_mac);
                ",
            )?;
            Ok(())
        })
            .await
            .map_err(|e| format!("Failed to initialize database tables: {e}"))?;

        Ok(Self { db })
    }

    pub async fn upsert_peer(&self, record: PeerRecord) {
        debug!(
            mac = %record.mac_address,
            ip = %record.last_known_ip,
            name = %record.device_name,
            "Upserting peer record to SQLite"
        );

        let res = self
            .db
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO peers (mac_address, last_known_ip, port, device_name, last_seen)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(mac_address) DO UPDATE SET
                        last_known_ip = excluded.last_known_ip,
                        port = excluded.port,
                        device_name = excluded.device_name,
                        last_seen = excluded.last_seen",
                    params![
                        record.mac_address,
                        record.last_known_ip,
                        record.port,
                        record.device_name,
                        record.last_seen
                    ],
                )?;
                Ok(())
            })
            .await;

        if let Err(e) = res {
            error!(error = %e, "Failed to upsert peer into database");
        }
    }

    pub async fn get_all_peers(&self) -> Vec<PeerRecord> {
        let res = self
            .db
            .call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT mac_address, last_known_ip, port, device_name, last_seen FROM peers",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok(PeerRecord {
                        mac_address: row.get(0)?,
                        last_known_ip: row.get(1)?,
                        port: row.get(2)?,
                        device_name: row.get(3)?,
                        last_seen: row.get(4)?,
                    })
                })?;

                let mut peers = Vec::new();
                for peer in rows {
                    peers.push(peer?);
                }
                Ok(peers)
            })
            .await;

        match res {
            Ok(peers) => {
                debug!(count = peers.len(), "Retrieved all peer records from SQLite");
                peers
            }
            Err(e) => {
                error!(error = %e, "Failed to fetch all peers");
                Vec::new()
            }
        }
    }

    pub async fn get_peer(&self, mac: &str) -> Option<PeerRecord> {
        let mac_owned = mac.to_string();
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT mac_address, last_known_ip, port, device_name, last_seen FROM peers WHERE mac_address = ?1",
                )?;
                let mut rows = stmt.query_map(params![mac_owned], |row| {
                    Ok(PeerRecord {
                        mac_address: row.get(0)?,
                        last_known_ip: row.get(1)?,
                        port: row.get(2)?,
                        device_name: row.get(3)?,
                        last_seen: row.get(4)?,
                    })
                })?;

                if let Some(peer) = rows.next() {
                    Ok(peer.ok())
                } else {
                    Ok(None)
                }
            })
            .await
            .unwrap_or(None)
    }

    pub async fn append_message(&self, peer_mac: String, msg: PersistentMessage) {
        info!(
            msg_id = %msg.msg_id,
            peer_mac = %peer_mac,
            is_outgoing = msg.is_outgoing,
            "Persisting message to SQLite"
        );

        let res = self
            .db
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO messages (msg_id, peer_mac, is_outgoing, content, timestamp, status)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(msg_id) DO UPDATE SET
                        status = excluded.status,
                        content = excluded.content",
                    params![
                        msg.msg_id,
                        peer_mac,
                        if msg.is_outgoing { 1 } else { 0 },
                        msg.content,
                        msg.timestamp,
                        msg.status.as_str()
                    ],
                )?;
                Ok(())
            })
            .await;

        if let Err(e) = res {
            error!(error = %e, "Failed to append message to SQLite");
        }
    }

    pub async fn mark_acked(&self, peer_mac: &str, target_msg_id: &str) -> bool {
        let msg_id_owned = target_msg_id.to_string();
        let status_str = MessageStatus::Acked.as_str();

        let res = self
            .db
            .call(move |conn| {
                let rows = conn.execute(
                    "UPDATE messages SET status = ?1 WHERE msg_id = ?2",
                    params![status_str, msg_id_owned],
                )?;
                Ok(rows > 0)
            })
            .await;

        match res {
            Ok(true) => {
                info!(msg_id = %target_msg_id, peer_mac = %peer_mac, "Marked message ACKED in database");
                true
            }
            _ => {
                warn!(msg_id = %target_msg_id, peer_mac = %peer_mac, "Failed to mark message ACKED in database");
                false
            }
        }
    }

    pub async fn get_messages_for_peer(&self, peer_mac: &str) -> Vec<PersistentMessage> {
        let mac_owned = peer_mac.to_string();
        let res = self
            .db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT msg_id, peer_mac, is_outgoing, content, timestamp, status
                     FROM messages WHERE peer_mac = ?1 ORDER BY timestamp ASC",
                )?;
                let rows = stmt.query_map(params![mac_owned], |row| {
                    let is_outgoing_int: i32 = row.get(2)?;
                    let status_str: String = row.get(5)?;
                    Ok(PersistentMessage {
                        msg_id: row.get(0)?,
                        peer_mac: row.get(1)?,
                        is_outgoing: is_outgoing_int == 1,
                        content: row.get(3)?,
                        timestamp: row.get(4)?,
                        status: MessageStatus::from_str(&status_str),
                    })
                })?;

                let mut list = Vec::new();
                for msg in rows {
                    list.push(msg?);
                }
                Ok(list)
            })
            .await;

        res.unwrap_or_default()
    }

    pub async fn get_pending_messages(&self, peer_mac: &str) -> Vec<PersistentMessage> {
        let mac_owned = peer_mac.to_string();
        let pending_status = MessageStatus::Pending.as_str();

        let res = self
            .db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT msg_id, peer_mac, is_outgoing, content, timestamp, status
                     FROM messages WHERE peer_mac = ?1 AND is_outgoing = 1 AND status = ?2 ORDER BY timestamp ASC",
                )?;
                let rows = stmt.query_map(params![mac_owned, pending_status], |row| {
                    let status_str: String = row.get(5)?;
                    Ok(PersistentMessage {
                        msg_id: row.get(0)?,
                        peer_mac: row.get(1)?,
                        is_outgoing: true,
                        content: row.get(3)?,
                        timestamp: row.get(4)?,
                        status: MessageStatus::from_str(&status_str),
                    })
                })?;

                let mut list = Vec::new();
                for msg in rows {
                    list.push(msg?);
                }
                Ok(list)
            })
            .await;

        res.unwrap_or_default()
    }
}

// ==========================================
// 2. Wire Protocol Envelopes
// ==========================================

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageEnvelope {
    pub version: u8,
    pub msg_id: String,
    pub sender_mac: String,
    pub payload: MessagePayload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "data")]
pub enum MessagePayload {
    Handshake { sender_mac: String },
    ChatMessage { content: String },
    Ack { target_msg_id: String },
}

#[frb(ignore)]
pub struct PeerConnection {
    pub remote_mac: RwLock<String>,
    pub remote_ip: String,
    tx: mpsc::Sender<MessageEnvelope>,
}

impl PeerConnection {
    /// 通过 Channel 异步投递待发送信封，彻底避免锁死 TCP Socket
    pub async fn send_envelope(&self, envelope: &MessageEnvelope) -> Result<(), String> {
        self.tx
            .send(envelope.clone())
            .await
            .map_err(|e| format!("Failed to queue message envelope to writer channel: {e}"))
    }
}

// ==========================================
// 3. Unified Engine
// ==========================================

#[derive(Clone)]
pub struct NetworkEngine {
    pub self_mac: String,
    pub(crate) storage: Arc<LocalStorage>,
    connections: Arc<RwLock<HashMap<String, Arc<PeerConnection>>>>,
    notify_sink: Arc<RwLock<Option<StreamSink<String>>>>,
}

impl NetworkEngine {
    pub async fn new(self_mac: String, db_path_str: String) -> Result<Self, String> {
        let db_path = PathBuf::from(db_path_str);
        info!(self_mac = %self_mac, db_path = ?db_path, "Initializing NetworkEngine with SQLite storage");
        let storage = LocalStorage::open(db_path).await?;

        Ok(Self {
            self_mac,
            storage: Arc::new(storage),
            connections: Arc::new(RwLock::new(HashMap::new())),
            notify_sink: Arc::new(RwLock::new(None)),
        })
    }

    pub async fn register_notify_sink(&self, sink: StreamSink<String>) {
        info!("Registering/Updating Flutter StreamSink for real-time notifications");
        let mut guard = self.notify_sink.write().await;
        *guard = Some(sink);
    }

    async fn emit_event(&self, event: String) {
        if let Some(ref sink) = *self.notify_sink.read().await {
            debug!(event = %event, "Emitting event sink update to Flutter UI");
            if let Err(e) = sink.add(event.clone()) {
                warn!(event = %event, error = ?e, "Failed to deliver event over StreamSink");
            }
        } else {
            warn!(
                event = %event,
                "Attempted to emit event, but StreamSink is not yet registered"
            );
        }
    }

    async fn get_or_connect(&self, peer_mac: &str) -> Result<Arc<PeerConnection>, String> {
        let peer_info = self.storage.get_peer(peer_mac).await;
        let peer_ip = peer_info.as_ref().map(|p| p.last_known_ip.as_str()).unwrap_or("unknown");

        if let Some(conn) = self.connections.read().await.get(peer_mac) {
            debug!(peer_mac = %peer_mac, ip = %conn.remote_ip, "Reusing active TCP peer connection");
            return Ok(Arc::clone(conn));
        }

        info!(peer_mac = %peer_mac, ip = %peer_ip, "No active connection found. Querying storage for peer endpoint info");
        let peer_record = peer_info.ok_or_else(|| {
            let err = format!("Peer MAC {} not found in local storage", peer_mac);
            error!(mac = %peer_mac, %err);
            err
        })?;

        let addr = format!("{}:{}", peer_record.last_known_ip, peer_record.port);
        info!(peer_mac = %peer_mac, ip = %peer_record.last_known_ip, address = %addr, "Establishing new TCP connection to peer");

        let stream = TcpStream::connect(&addr).await.map_err(|e| {
            let err = format!("Failed to connect to {addr}: {e}");
            warn!(peer_mac = %peer_mac, ip = %peer_record.last_known_ip, address = %addr, error = %e, "TCP connection attempt failed");
            err
        })?;

        info!(peer_mac = %peer_mac, ip = %peer_record.last_known_ip, address = %addr, "TCP connection established successfully");

        let (tx, conn) = self.setup_connection(stream, peer_record.last_known_ip.clone(), peer_mac.to_string()).await;

        let handshake = MessageEnvelope {
            version: 1,
            msg_id: Uuid::new_v4().to_string(),
            sender_mac: self.self_mac.clone(),
            payload: MessagePayload::Handshake {
                sender_mac: self.self_mac.clone(),
            },
        };

        info!(peer_mac = %peer_mac, ip = %peer_record.last_known_ip, "Sending outbound handshake");
        conn.send_envelope(&handshake).await?;

        self.connections
            .write()
            .await
            .insert(peer_mac.to_string(), Arc::clone(&conn));

        Ok(conn)
    }

    /// 设置分离的读写 Channel 逻辑
    async fn setup_connection(
        &self,
        stream: TcpStream,
        remote_ip: String,
        peer_mac: String,
    ) -> (mpsc::Sender<MessageEnvelope>, Arc<PeerConnection>) {
        let (read_half, mut write_half) = stream.into_split();
        let (tx, mut rx) = mpsc::channel::<MessageEnvelope>(100);

        let conn = Arc::new(PeerConnection {
            remote_mac: RwLock::new(peer_mac.clone()),
            remote_ip: remote_ip.clone(),
            tx: tx.clone(),
        });

        // 1. 独立写入任务：从 rx 通道接收 Frame，单向写到 TCP stream
        let ip_writer = remote_ip.clone();
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            while let Some(envelope) = rx.recv().await {
                let bytes = match serde_json::to_vec(&envelope) {
                    Ok(b) => b,
                    Err(e) => {
                        error!(error = %e, "Failed to serialize MessageEnvelope");
                        continue;
                    }
                };

                let len = (bytes.len() as u32).to_be_bytes();
                if let Err(e) = write_half.write_all(&len).await {
                    error!(ip = %ip_writer, error = %e, "Failed writing length to socket");
                    break;
                }
                if let Err(e) = write_half.write_all(&bytes).await {
                    error!(ip = %ip_writer, error = %e, "Failed writing payload to socket");
                    break;
                }
                if let Err(e) = write_half.flush().await {
                    error!(ip = %ip_writer, error = %e, "Failed flushing socket");
                    break;
                }
            }
        });

        // 2. 独立读取循环：从 read_half 不断读取 Frame 驱动状态机
        let engine = self.clone();
        let conn_clone = Arc::clone(&conn);
        tokio::spawn(async move {
            engine.handle_read_loop(read_half, conn_clone, peer_mac, remote_ip).await;
        });

        (tx, conn)
    }

    pub async fn send_message(&self, recipient_mac: String, content: String) -> Result<(), String> {
        let recipient_ip = self
            .storage
            .get_peer(&recipient_mac)
            .await
            .map(|p| p.last_known_ip)
            .unwrap_or_else(|| "unknown".to_string());
        let msg_id = Uuid::new_v4().to_string();
        info!(
            msg_id = %msg_id,
            recipient_mac = %recipient_mac,
            recipient_ip = %recipient_ip,
            "Queueing new outgoing chat message"
        );

        let msg = PersistentMessage {
            msg_id,
            peer_mac: recipient_mac.clone(),
            is_outgoing: true,
            content,
            timestamp: chrono_now_timestamp(),
            status: MessageStatus::Pending,
        };

        self.storage.append_message(recipient_mac.clone(), msg).await;

        let engine = self.clone();
        let mac_clone = recipient_mac.clone();
        tokio::spawn(async move {
            engine.flush_outbox(&mac_clone).await;
        });

        Ok(())
    }

    pub async fn flush_outbox(&self, peer_mac: &str) {
        let peer_ip = self.storage.get_peer(peer_mac).await.map(|p| p.last_known_ip).unwrap_or_else(|| "unknown".to_string());
        let pending = self.storage.get_pending_messages(peer_mac).await;
        if pending.is_empty() {
            debug!(peer_mac = %peer_mac, ip = %peer_ip, "No pending messages to flush");
            return;
        }

        info!(
            peer_mac = %peer_mac,
            ip = %peer_ip,
            count = pending.len(),
            "Flushing outbox pending queue"
        );

        match self.get_or_connect(peer_mac).await {
            Ok(conn) => {
                for msg in pending {
                    debug!(
                        peer_mac = %peer_mac,
                        ip = %peer_ip,
                        msg_id = %msg.msg_id,
                        "Attempting outbox message delivery"
                    );

                    let env = MessageEnvelope {
                        version: 1,
                        msg_id: msg.msg_id.clone(),
                        sender_mac: self.self_mac.clone(),
                        payload: MessagePayload::ChatMessage {
                            content: msg.content.clone(),
                        },
                    };

                    if let Err(e) = conn.send_envelope(&env).await {
                        error!(
                            peer_mac = %peer_mac,
                            ip = %peer_ip,
                            msg_id = %msg.msg_id,
                            error = %e,
                            "Failed transmitting outbox message. Evicting dead connection"
                        );
                        self.connections.write().await.remove(peer_mac);
                        break;
                    } else {
                        self.emit_event(format!("MSG_SENT:{}", peer_mac)).await;
                    }
                }
            }
            Err(e) => {
                warn!(
                    peer_mac = %peer_mac,
                    ip = %peer_ip,
                    error = %e,
                    "Cannot flush outbox: unable to connect to peer endpoint"
                );
            }
        }
    }

    pub async fn run_scan_and_flush_cycle(&self) {
        info!("Starting mDNS peer scan and outbox flush cycle");

        match crate::api::discovery::scan_lan_peers().await {
            Ok(discovered_peers) => {
                info!(
                    count = discovered_peers.len(),
                    "Discovery scan complete; processing peers"
                );

                for peer in discovered_peers {
                    let mac = peer.mac_address.clone();
                    let record = PeerRecord {
                        mac_address: mac.clone(),
                        last_known_ip: peer.ip.clone(),
                        port: ProtocolConfig::SERVICE_PORT,
                        device_name: peer.device_name,
                        last_seen: chrono_now_timestamp(),
                    };

                    self.storage.upsert_peer(record).await;
                    self.flush_outbox(&mac).await;
                }
                self.emit_event("PEER_LIST_UPDATED".to_string()).await;
            }
            Err(e) => {
                error!(error = ?e, "mDNS LAN peer discovery scan failed");
            }
        }
    }

    pub async fn start_listener(
        &self,
        port: u16,
        notify_sink: StreamSink<String>,
    ) -> Result<(), String> {
        self.register_notify_sink(notify_sink).await;

        let engine = self.clone();
        let addr = format!("0.0.0.0:{}", port);

        let listener = TcpListener::bind(&addr).await.map_err(|e| {
            let err = format!("Failed to bind listener to {addr}: {e}");
            error!(%err);
            err
        })?;

        info!(address = %addr, "TCP server listener successfully bound and listening");

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer_addr)) => {
                        let ip = peer_addr.ip().to_string();
                        info!(peer_addr = %peer_addr, ip = %ip, "Accepted incoming TCP connection");
                        let engine_clone = engine.clone();
                        tokio::spawn(async move {
                            engine_clone.handle_incoming_stream(stream, ip).await;
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "Failed accepting incoming TCP stream");
                    }
                }
            }
        });

        Ok(())
    }

    async fn handle_incoming_stream(&self, stream: TcpStream, ip: String) {
        self.setup_connection(stream, ip, String::new()).await;
    }

    async fn handle_read_loop(
        &self,
        mut read_half: tokio::net::tcp::OwnedReadHalf,
        conn: Arc<PeerConnection>,
        mut current_peer_mac: String,
        current_peer_ip: String,
    ) {
        debug!(
            initial_peer_mac = %current_peer_mac,
            ip = %current_peer_ip,
            "Entering connection event loop"
        );

        loop {
            // 从 OwnedReadHalf 循环解析 Packet
            let mut len_bytes = [0u8; 4];
            if let Err(e) = read_half.read_exact(&mut len_bytes).await {
                debug!(
                    remote_mac = %current_peer_mac,
                    ip = %current_peer_ip,
                    error = %e,
                    "TCP stream read length error (connection closed)"
                );
                break;
            }

            let len = u32::from_be_bytes(len_bytes) as usize;
            let mut buffer = vec![0u8; len];
            if let Err(e) = read_half.read_exact(&mut buffer).await {
                error!(
                    remote_mac = %current_peer_mac,
                    ip = %current_peer_ip,
                    error = %e,
                    "Failed reading payload from socket"
                );
                break;
            }

            let env: MessageEnvelope = match serde_json::from_slice(&buffer) {
                Ok(env) => env,
                Err(e) => {
                    error!(
                        remote_mac = %current_peer_mac,
                        ip = %current_peer_ip,
                        error = %e,
                        "Deserialization failed"
                    );
                    continue;
                }
            };

            match env.payload {
                MessagePayload::Handshake { sender_mac } => {
                    info!(
                        remote_mac = %sender_mac,
                        ip = %current_peer_ip,
                        "Received Handshake envelope. Registering peer connection"
                    );
                    current_peer_mac = sender_mac.clone();
                    *conn.remote_mac.write().await = sender_mac.clone();
                    self.connections
                        .write()
                        .await
                        .insert(sender_mac, Arc::clone(&conn));
                }
                MessagePayload::ChatMessage { content } => {
                    info!(
                        sender_mac = %env.sender_mac,
                        ip = %current_peer_ip,
                        msg_id = %env.msg_id,
                        "Received ChatMessage envelope"
                    );

                    if current_peer_mac.is_empty() {
                        current_peer_mac = env.sender_mac.clone();
                        *conn.remote_mac.write().await = env.sender_mac.clone();
                        self.connections
                            .write()
                            .await
                            .insert(env.sender_mac.clone(), Arc::clone(&conn));
                    }

                    let msg = PersistentMessage {
                        msg_id: env.msg_id.clone(),
                        peer_mac: env.sender_mac.clone(),
                        is_outgoing: false,
                        content,
                        timestamp: chrono_now_timestamp(),
                        status: MessageStatus::Acked,
                    };

                    self.storage.append_message(env.sender_mac.clone(), msg).await;

                    let ack = MessageEnvelope {
                        version: 1,
                        msg_id: Uuid::new_v4().to_string(),
                        sender_mac: self.self_mac.clone(),
                        payload: MessagePayload::Ack {
                            target_msg_id: env.msg_id,
                        },
                    };

                    if let Err(e) = conn.send_envelope(&ack).await {
                        warn!(
                            remote_mac = %env.sender_mac,
                            ip = %current_peer_ip,
                            error = %e,
                            "Failed sending ACK to peer"
                        );
                    }

                    self.emit_event(format!("NEW_MSG:{}", env.sender_mac)).await;
                }
                MessagePayload::Ack { target_msg_id } => {
                    let primary_target = if !current_peer_mac.is_empty() {
                        &current_peer_mac
                    } else {
                        &env.sender_mac
                    };

                    info!(
                        peer_mac = %primary_target,
                        ip = %current_peer_ip,
                        target_msg_id = %target_msg_id,
                        "Received ACK envelope for message"
                    );

                    let marked = self.storage.mark_acked(primary_target, &target_msg_id).await;
                    if marked {
                        self.emit_event(format!("ACK:{}", primary_target)).await;
                    }
                }
            }
        }

        // 清理断开连接
        if !current_peer_mac.is_empty() {
            self.connections.write().await.remove(&current_peer_mac);
            info!(
                peer_mac = %current_peer_mac,
                ip = %current_peer_ip,
                "Evicted disconnected peer from connection map"
            );
        }
    }
}

pub fn chrono_now_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
