use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tokio_rusqlite::Connection as AsyncConnection;
use rusqlite::params;
use uuid::Uuid;
use tracing::{debug, error, info, warn};
use flutter_rust_bridge::frb;

use crate::api::discovery::ProtocolConfig;
use crate::frb_generated::StreamSink;

// =============================================================================
// 1. Data Models
// =============================================================================

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
    pub is_online: bool,
}

// Wire protocol frame format
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

// =============================================================================
// 2. Storage Engine (SQLite Persistence Layer)
// =============================================================================

pub struct LocalStorage {
    db: AsyncConnection,
}

impl LocalStorage {
    pub async fn open(db_path: PathBuf) -> Result<Self, String> {
        info!(path = ?db_path, "Opening SQLite database");

        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(|e| {
                    format!("Failed to create database directory: {:?}", e)
                })?;
            }
        }

        let db = AsyncConnection::open(db_path)
            .await
            .map_err(|e| format!("Failed to open SQLite database: {e}"))?;

        let init_res = db
            .call(|conn| {
                conn.execute_batch(
                    "
                    PRAGMA journal_mode = WAL;
                    PRAGMA synchronous = NORMAL;

                    CREATE TABLE IF NOT EXISTS peers (
                        mac_address TEXT PRIMARY KEY,
                        last_known_ip TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        device_name TEXT NOT NULL,
                        last_seen INTEGER NOT NULL,
                        is_online INTEGER NOT NULL DEFAULT 0
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

                // 检查旧 peers 表中是否存在 is_online 列，如果不存在则直接修改表追加列（自动迁移）
                let mut stmt = conn.prepare("PRAGMA table_info(peers)")?;
                let mut has_is_online = false;
                let rows = stmt.query_map([], |row| {
                    let col_name: String = row.get(1)?;
                    Ok(col_name)
                })?;

                for col in rows {
                    if let Ok(name) = col {
                        if name == "is_online" {
                            has_is_online = true;
                            break;
                        }
                    }
                }

                if !has_is_online {
                    info!("Adding missing 'is_online' column to existing peers table");
                    conn.execute("ALTER TABLE peers ADD COLUMN is_online INTEGER NOT NULL DEFAULT 0", [])?;
                }

                Ok(())
            })
            .await;

        // 如果上述过程遇到严重的字段冲突或表损坏，直接强行 Drop 表重建
        if let Err(err) = init_res {
            warn!(error = %err, "Database schema incompatibility detected. Dropping tables and rebuilding...");

            db.call(|conn| {
                conn.execute_batch(
                    "
                    DROP TABLE IF EXISTS messages;
                    DROP TABLE IF EXISTS peers;

                    CREATE TABLE peers (
                        mac_address TEXT PRIMARY KEY,
                        last_known_ip TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        device_name TEXT NOT NULL,
                        last_seen INTEGER NOT NULL,
                        is_online INTEGER NOT NULL DEFAULT 0
                    );

                    CREATE TABLE messages (
                        msg_id TEXT PRIMARY KEY,
                        peer_mac TEXT NOT NULL,
                        is_outgoing INTEGER NOT NULL,
                        content TEXT NOT NULL,
                        timestamp INTEGER NOT NULL,
                        status TEXT NOT NULL
                    );

                    CREATE INDEX idx_messages_peer_mac ON messages(peer_mac);
                    ",
                )?;
                Ok(())
            })
                .await
                .map_err(|e| format!("Failed to force recreate SQLite database tables: {e}"))?;
        }

        Ok(Self { db })
    }

    pub async fn upsert_peer(&self, record: PeerRecord) {
        debug!(mac = %record.mac_address, ip = %record.last_known_ip, is_online = record.is_online, "Upserting peer record");

        let res = self
            .db
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO peers (mac_address, last_known_ip, port, device_name, last_seen, is_online)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        record.mac_address,
                        record.last_known_ip,
                        record.port,
                        record.device_name,
                        record.last_seen,
                        if record.is_online { 1 } else { 0 }
                    ],
                )?;
                Ok(())
            })
            .await;

        if let Err(e) = res {
            error!(error = %e, "Failed to upsert peer into database");
        }
    }

    pub async fn set_peer_online_status(&self, mac: &str, is_online: bool) {
        let mac_owned = mac.to_string();
        let res = self
            .db
            .call(move |conn| {
                conn.execute(
                    "UPDATE peers SET is_online = ?1 WHERE mac_address = ?2",
                    params![if is_online { 1 } else { 0 }, mac_owned],
                )?;
                Ok(())
            })
            .await;

        if let Err(e) = res {
            error!(error = %e, mac = %mac, "Failed to update peer online status");
        }
    }

    pub async fn get_all_peers(&self) -> Vec<PeerRecord> {
        let res = self
            .db
            .call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT mac_address, last_known_ip, port, device_name, last_seen, is_online FROM peers ORDER BY is_online DESC, last_seen DESC",
                )?;
                let rows = stmt.query_map([], |row| {
                    let is_online_int: i32 = row.get(5)?;
                    Ok(PeerRecord {
                        mac_address: row.get(0)?,
                        last_known_ip: row.get(1)?,
                        port: row.get(2)?,
                        device_name: row.get(3)?,
                        last_seen: row.get(4)?,
                        is_online: is_online_int == 1,
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
            Ok(peers) => peers,
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
                    "SELECT mac_address, last_known_ip, port, device_name, last_seen, is_online FROM peers WHERE mac_address = ?1",
                )?;
                let mut rows = stmt.query_map(params![mac_owned], |row| {
                    let is_online_int: i32 = row.get(5)?;
                    Ok(PeerRecord {
                        mac_address: row.get(0)?,
                        last_known_ip: row.get(1)?,
                        port: row.get(2)?,
                        device_name: row.get(3)?,
                        last_seen: row.get(4)?,
                        is_online: is_online_int == 1,
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
        info!(msg_id = %msg.msg_id, peer_mac = %peer_mac, "Persisting message to SQLite");

        let res = self
            .db
            .call(move |conn| {
                let insert_res = conn.execute(
                    "INSERT OR REPLACE INTO messages (msg_id, peer_mac, is_outgoing, content, timestamp, status)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        msg.msg_id,
                        peer_mac,
                        if msg.is_outgoing { 1 } else { 0 },
                        msg.content,
                        msg.timestamp,
                        msg.status.as_str()
                    ],
                );

                if insert_res.is_err() {
                    let _ = conn.execute("DELETE FROM messages WHERE msg_id = ?1", params![msg.msg_id]);
                    conn.execute(
                        "INSERT INTO messages (msg_id, peer_mac, is_outgoing, content, timestamp, status)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                        params![
                            msg.msg_id,
                            peer_mac,
                            if msg.is_outgoing { 1 } else { 0 },
                            msg.content,
                            msg.timestamp,
                            msg.status.as_str()
                        ],
                    )?;
                }
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

// =============================================================================
// 3. Peer Connection Handle
// =============================================================================

#[frb(ignore)]
pub struct PeerConnection {
    pub remote_mac: RwLock<String>,
    pub remote_ip: String,
    tx: mpsc::Sender<MessageEnvelope>,
}

impl PeerConnection {
    pub async fn send_envelope(&self, envelope: &MessageEnvelope) -> Result<(), String> {
        self.tx
            .send(envelope.clone())
            .await
            .map_err(|e| format!("Failed to queue message envelope: {e}"))
    }
}

// =============================================================================
// 4. Unified Communication Engine
// =============================================================================

#[derive(Clone)]
pub struct NetworkEngine {
    pub self_mac: String,
    pub(crate) storage: Arc<LocalStorage>,
    connections: Arc<RwLock<HashMap<String, Arc<PeerConnection>>>>,
    notify_sink: Arc<RwLock<Option<StreamSink<String>>>>,
}

impl NetworkEngine {
    // --- Core Lifecycle & Management ---

    pub async fn new(self_mac: String, db_path_str: String) -> Result<Self, String> {
        let db_path = PathBuf::from(db_path_str);
        info!(self_mac = %self_mac, db_path = ?db_path, "Initializing NetworkEngine");
        let storage = LocalStorage::open(db_path).await?;

        Ok(Self {
            self_mac,
            storage: Arc::new(storage),
            connections: Arc::new(RwLock::new(HashMap::new())),
            notify_sink: Arc::new(RwLock::new(None)),
        })
    }

    pub async fn register_notify_sink(&self, sink: StreamSink<String>) {
        info!("Registering StreamSink for Flutter events");
        let mut guard = self.notify_sink.write().await;
        *guard = Some(sink);
    }

    async fn emit_event(&self, event: String) {
        if let Some(ref sink) = *self.notify_sink.read().await {
            debug!(event = %event, "Emitting event to Dart");
            if let Err(e) = sink.add(event.clone()) {
                warn!(event = %event, error = ?e, "Failed to deliver event via StreamSink");
            }
        }
    }

    // --- Dart/Flutter Exported APIs ---

    pub async fn upsert_peer(&self, record: PeerRecord) {
        self.storage.upsert_peer(record).await;
    }

    pub async fn get_peers(&self) -> Vec<PeerRecord> {
        self.storage.get_all_peers().await
    }

    pub async fn get_messages(&self, peer_mac: String) -> Vec<PersistentMessage> {
        self.storage.get_messages_for_peer(&peer_mac).await
    }

    // --- Active Communication & Outbox ---

    pub async fn send_message(&self, recipient_mac: String, content: String) -> Result<(), String> {
        let msg_id = Uuid::new_v4().to_string();
        info!(msg_id = %msg_id, recipient = %recipient_mac, "Queueing outgoing chat message");

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
        tokio::spawn(async move {
            engine.flush_outbox(&recipient_mac).await;
        });

        Ok(())
    }

    pub async fn flush_outbox(&self, peer_mac: &str) {
        let pending = self.storage.get_pending_messages(peer_mac).await;
        if pending.is_empty() {
            return;
        }

        info!(peer_mac = %peer_mac, count = pending.len(), "Flushing pending outbox messages");

        match self.get_or_connect(peer_mac).await {
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
                        self.connections.write().await.remove(peer_mac);
                        self.storage.set_peer_online_status(peer_mac, false).await;
                        self.emit_event("PEER_LIST_UPDATED".to_string()).await;
                        break;
                    } else {
                        self.emit_event(format!("MSG_SENT:{}", peer_mac)).await;
                    }
                }
            }
            Err(e) => {
                warn!(peer_mac = %peer_mac, error = %e, "Unable to flush outbox: connection failed");
            }
        }
    }

    pub async fn run_scan_and_flush_cycle(&self) {
        info!("Starting peer status refresh and outbox flush cycle");

        // 1. Read historical records from SQLite
        let historical_peers = self.storage.get_all_peers().await;

        // 2. Discover active devices via mDNS
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

        // 3. Update active state for existing devices & flush pending messages
        for mut peer_record in historical_peers {
            let mac = peer_record.mac_address.clone();

            if let Some(online_peer) = discovered_map.get(&mac) {
                peer_record.last_known_ip = online_peer.ip.clone();
                peer_record.device_name = online_peer.device_name.clone();
                peer_record.last_seen = chrono_now_timestamp();
                peer_record.is_online = true;
                self.storage.upsert_peer(peer_record).await;
                self.flush_outbox(&mac).await;
            } else {
                peer_record.is_online = false;
                self.storage.upsert_peer(peer_record).await;
            }
        }

        // 4. Save newly discovered devices
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
                self.flush_outbox(&mac).await;
            }
        }

        // 5. Notify UI
        self.emit_event("PEER_LIST_UPDATED".to_string()).await;
    }

    // --- Networking Internal Logic ---

    async fn get_or_connect(&self, peer_mac: &str) -> Result<Arc<PeerConnection>, String> {
        if let Some(conn) = self.connections.read().await.get(peer_mac) {
            return Ok(Arc::clone(conn));
        }

        let peer_record = self.storage.get_peer(peer_mac).await.ok_or_else(|| {
            format!("Peer MAC {} not found in local storage", peer_mac)
        })?;

        let addr = format!("{}:{}", peer_record.last_known_ip, peer_record.port);
        info!(peer_mac = %peer_mac, address = %addr, "Connecting to peer");

        let stream = TcpStream::connect(&addr).await.map_err(|e| format!("Connect failed: {e}"))?;
        let (tx, conn) = self.setup_connection(stream, peer_record.last_known_ip.clone(), peer_mac.to_string()).await;

        let handshake = MessageEnvelope {
            version: 1,
            msg_id: Uuid::new_v4().to_string(),
            sender_mac: self.self_mac.clone(),
            payload: MessagePayload::Handshake {
                sender_mac: self.self_mac.clone(),
            },
        };

        conn.send_envelope(&handshake).await?;
        self.connections.write().await.insert(peer_mac.to_string(), Arc::clone(&conn));
        self.storage.set_peer_online_status(peer_mac, true).await;
        self.emit_event("PEER_LIST_UPDATED".to_string()).await;

        Ok(conn)
    }

    pub async fn start_listener(
        &self,
        port: u16,
        notify_sink: StreamSink<String>,
    ) -> Result<(), String> {
        self.register_notify_sink(notify_sink).await;

        let engine = self.clone();
        let addr = format!("0.0.0.0:{}", port);
        let listener = TcpListener::bind(&addr).await.map_err(|e| format!("Bind error: {e}"))?;

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

    async fn handle_incoming_stream(&self, stream: TcpStream, ip: String) {
        self.setup_connection(stream, ip, String::new()).await;
    }

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

        // Frame Writer Loop (4-byte length prefix + payload)
        let ip_writer = remote_ip.clone();
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

        // Frame Reader Loop
        let engine = self.clone();
        let conn_clone = Arc::clone(&conn);
        tokio::spawn(async move {
            engine.handle_read_loop(read_half, conn_clone, peer_mac, remote_ip).await;
        });

        (tx, conn)
    }

    async fn handle_read_loop(
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

            match env.payload {
                MessagePayload::Handshake { sender_mac } => {
                    current_peer_mac = sender_mac.clone();
                    *conn.remote_mac.write().await = sender_mac.clone();
                    self.connections.write().await.insert(sender_mac.clone(), Arc::clone(&conn));
                    self.storage.set_peer_online_status(&sender_mac, true).await;
                    self.emit_event("PEER_LIST_UPDATED".to_string()).await;
                }
                MessagePayload::ChatMessage { content } => {
                    if current_peer_mac.is_empty() {
                        current_peer_mac = env.sender_mac.clone();
                        *conn.remote_mac.write().await = env.sender_mac.clone();
                        self.connections.write().await.insert(env.sender_mac.clone(), Arc::clone(&conn));
                        self.storage.set_peer_online_status(&env.sender_mac, true).await;
                        self.emit_event("PEER_LIST_UPDATED".to_string()).await;
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

                    let _ = conn.send_envelope(&ack).await;
                    self.emit_event(format!("NEW_MSG:{}", env.sender_mac)).await;
                }
                MessagePayload::Ack { target_msg_id } => {
                    let target = if !current_peer_mac.is_empty() {
                        &current_peer_mac
                    } else {
                        &env.sender_mac
                    };

                    if self.storage.mark_acked(target, &target_msg_id).await {
                        self.emit_event(format!("ACK:{}", target)).await;
                    }
                }
            }
        }

        if !current_peer_mac.is_empty() {
            self.connections.write().await.remove(&current_peer_mac);
            self.storage.set_peer_online_status(&current_peer_mac, false).await;
            self.emit_event("PEER_LIST_UPDATED".to_string()).await;
            info!(peer_mac = %current_peer_mac, ip = %current_peer_ip, "Closed peer connection removed and marked offline");
        }
    }
}

// =============================================================================
// 5. Utility Functions
// =============================================================================

pub fn chrono_now_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
