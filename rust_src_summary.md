## File: rust/src/lib.rs

```rust
pub mod api;
mod frb_generated;

```

---

## File: rust/src/api/bridge.rs

```rust
use crate::api::engine::NetworkEngineHandle;

/// Periodic scan tick called by Flutter timer
pub async fn scan_and_flush(engine: NetworkEngineHandle) {
    engine.run_scan_and_flush_cycle().await;
}


```

---

## File: rust/src/api/connection/manager.rs

```rust
use super::traits::{ConnectionManager, ConnectionManagerHandle};
use crate::api::connection::traits::PeerConnectionHandle;
use crate::api::engine::NetworkEngine;
use crate::api::models::{MessageEnvelope, MessagePayload};
use crate::api::storage::traits::{StorageRepository, StorageRepositoryHandle};
use async_trait::async_trait;
use flutter_rust_bridge::frb;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info};
use uuid::Uuid;

pub struct PeerConnection {
    pub(crate) remote_mac: RwLock<String>,
    pub remote_ip: String,
    pub(crate) tx: mpsc::Sender<MessageEnvelope>,
}

impl PeerConnection {
    pub async fn send_envelope(&self, envelope: &MessageEnvelope) -> Result<(), String> {
        self.tx
            .send(envelope.clone())
            .await
            .map_err(|e| format!("Failed to queue message envelope: {e}"))
    }
}

#[frb(ignore)]
pub struct TcpConnectionManager {
    self_mac: String,
    storage: Arc<dyn StorageRepository>,
    connections: Arc<RwLock<HashMap<String, Arc<PeerConnection>>>>,
    engine: RwLock<Option<Weak<NetworkEngine>>>,
}

impl TcpConnectionManager {
    pub fn new(self_mac: String, storage: Arc<dyn StorageRepository>) -> Self {
        Self {
            self_mac,
            storage,
            connections: Arc::new(RwLock::new(HashMap::new())),
            engine: RwLock::new(None),
        }
    }

    /// 绑定 NetworkEngine 的弱引用，用于将读取流 (Read Half) 传回 Engine 处理
    pub async fn set_engine(&self, engine: Weak<NetworkEngine>) {
        *self.engine.write().await = Some(engine);
    }

    /// 手动/被动握手成功后直接插入连接池
    pub async fn insert_connection(&self, peer_mac: String, conn: Arc<PeerConnection>) {
        self.connections.write().await.insert(peer_mac, conn);
    }

    pub(crate) fn connections_map(&self) -> Arc<RwLock<HashMap<String, Arc<PeerConnection>>>> {
        Arc::clone(&self.connections)
    }

    pub(crate) async fn setup_connection(
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

        let ip_writer = remote_ip.clone();
        // 1. 启动 Write 循环
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

        // 2. 启动 Read 循环（如果绑定了 NetworkEngine，把 read_half 交给 handle_read_loop 处理）
        if let Some(engine_weak) = self.engine.read().await.as_ref() {
            if let Some(engine) = engine_weak.upgrade() {
                let conn_clone = Arc::clone(&conn);
                let ip_clone = remote_ip.clone();
                let peer_mac_clone = peer_mac.clone();

                tokio::spawn(async move {
                    engine
                        .handle_read_loop(read_half, conn_clone, peer_mac_clone, ip_clone)
                        .await;
                });
            } else {
                error!("NetworkEngine weak reference upgrade failed during setup_connection");
            }
        } else {
            error!("NetworkEngine weak reference not bound in TcpConnectionManager");
        }

        (tx, conn)
    }
}

#[frb(ignore)]
#[async_trait]
impl ConnectionManager for TcpConnectionManager {
    async fn get_or_connect(&self, peer_mac: &str) -> Result<Arc<PeerConnection>, String> {
        if let Some(conn) = self.connections.read().await.get(peer_mac) {
            return Ok(Arc::clone(conn));
        }

        let peer_record = self
            .storage
            .get_peer(peer_mac)
            .await
            .ok_or_else(|| format!("Peer MAC {} not found in local storage", peer_mac))?;

        let addr = format!("{}:{}", peer_record.last_known_ip, peer_record.port);
        info!(peer_mac = %peer_mac, address = %addr, "Connecting to peer");

        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("Connect failed: {e}"))?;

        let (_tx, conn) = self
            .setup_connection(stream, peer_record.last_known_ip.clone(), peer_mac.to_string())
            .await;

        let handshake = MessageEnvelope {
            version: 1,
            msg_id: Uuid::new_v4().to_string(),
            sender_mac: self.self_mac.clone(),
            payload: MessagePayload::Handshake {
                sender_mac: self.self_mac.clone(),
            },
        };

        conn.send_envelope(&handshake).await?;
        self.insert_connection(peer_mac.to_string(), Arc::clone(&conn)).await;
        self.storage.set_peer_online_status(peer_mac, true).await;

        Ok(conn)
    }

    async fn remove_connection(&self, peer_mac: &str) {
        self.connections.write().await.remove(peer_mac);
    }

    async fn has_connection(&self, peer_mac: &str) -> bool {
        self.connections.read().await.contains_key(peer_mac)
    }
}

// =========================================================================
// FRB Opaque 构造器及绑定包装层
// =========================================================================

pub struct TcpConnectionManagerHandle {
    pub(crate) inner: Arc<TcpConnectionManager>,
}

impl TcpConnectionManagerHandle {
    pub fn new(self_mac: String, storage_handle: &StorageRepositoryHandle) -> Self {
        let manager = Arc::new(TcpConnectionManager::new(
            self_mac,
            Arc::clone(&storage_handle.inner),
        ));
        Self { inner: manager }
    }

    pub fn as_connection_manager(&self) -> ConnectionManagerHandle {
        ConnectionManagerHandle {
            inner: Arc::clone(&self.inner) as Arc<dyn ConnectionManager>,
        }
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

```

---

## File: rust/src/api/connection/mod.rs

```rust
pub mod traits;
pub mod manager;
```

---

## File: rust/src/api/connection/traits.rs

```rust
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

```

---

## File: rust/src/api/protocol.rs

```rust
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use flutter_rust_bridge::frb;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tokio_rusqlite::Connection as AsyncConnection;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::api::discovery::ProtocolConfig;
use crate::frb_generated::StreamSink;

// =============================================================================
// 1. Data Models & Enums
// =============================================================================

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MessageStatus {
    Pending,
    Acked,
    Failed,
}

impl MessageStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageStatus::Pending => "PENDING",
            MessageStatus::Acked => "ACKED",
            MessageStatus::Failed => "FAILED",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "ACKED" => MessageStatus::Acked,
            "FAILED" => MessageStatus::Failed,
            _ => MessageStatus::Pending,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FileType {
    None,
    Image,
    Video,
    Generic,
}

impl FileType {
    pub fn as_str(&self) -> &'static str {
        match self {
            FileType::None => "none",
            FileType::Image => "image",
            FileType::Video => "video",
            FileType::Generic => "generic",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "image" => FileType::Image,
            "video" => FileType::Video,
            "generic" => FileType::Generic,
            _ => FileType::None,
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
    // --- 扩展：支持关联文件信息 ---
    pub file_type: FileType,
    pub file_name: Option<String>,
    pub file_size: Option<u64>,
    pub file_hash: Option<String>,
    pub file_path: Option<String>,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChunkHeader {
    pub transfer_id: String,
    pub chunk_uuid: String,
    pub chunk_index: u32,
    pub total_chunks: u32,
    pub offset: u64,
    pub chunk_hash: String,
    pub file_hash: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileTransferRecord {
    pub transfer_id: String,
    pub peer_mac: String,
    pub file_type: FileType,
    pub file_name: String,
    pub file_size: u64,
    pub total_chunks: u32,
    pub received_chunks: u32,
    pub file_hash: String,
    pub save_path: String,
    pub is_completed: bool,
    pub is_outgoing: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "data")]
pub enum MessagePayload {
    Handshake {
        sender_mac: String,
    },
    ChatMessage {
        content: String,
    },
    FileTransferInit {
        transfer_id: String,
        file_type: FileType,
        file_name: String,
        file_size: u64,
        total_chunks: u32,
        file_hash: String,
    },
    FileChunk {
        header: ChunkHeader,
        data: Vec<u8>,
    },
    Ack {
        target_msg_id: String,
    },
    ChunkAck {
        transfer_id: String,
        chunk_index: u32,
    },
    FileCompleteAck {
        transfer_id: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageEnvelope {
    pub version: u8,
    pub msg_id: String,
    pub sender_mac: String,
    pub payload: MessagePayload,
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
                fs::create_dir_all(parent)
                    .map_err(|e| format!("Failed to create database directory: {:?}", e))?;
            }
        }

        let db = AsyncConnection::open(db_path)
            .await
            .map_err(|e| format!("Failed to open SQLite database: {e}"))?;

        // 校验 Scheme 兼容性。如果缺少关联文件的关键列，直接返回 Error 并强行彻底重构！
        let check_schema = db
            .call(|conn| {
                let mut stmt = conn.prepare("PRAGMA table_info(messages)")?;
                let mut has_file_type = false;
                let rows = stmt.query_map([], |row| {
                    let col_name: String = row.get(1)?;
                    Ok(col_name)
                })?;

                for col in rows {
                    if let Ok(name) = col {
                        if name == "file_type" {
                            has_file_type = true;
                            break;
                        }
                    }
                }

                if !has_file_type {
                    return Err(rusqlite::Error::ModuleError("Schema mismatch: missing file fields in messages".to_string()).into());
                }
                Ok(())
            })
            .await;

        if check_schema.is_err() {
            warn!("Database schema incompatibility detected. Rebuilding database tables...");
            Self::rebuild_schema(&db).await?;
        } else {
            // 初始化/确保所有表结构均准备完成
            let init_res = Self::init_db_tables(&db).await;
            if let Err(e) = init_res {
                warn!(error = %e, "DB Initialization failed. Dropping and re-creating...");
                Self::rebuild_schema(&db).await?;
            }
        }

        Ok(Self { db })
    }

    async fn init_db_tables(db: &AsyncConnection) -> Result<(), String> {
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
                    last_seen INTEGER NOT NULL,
                    is_online INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS messages (
                    msg_id TEXT PRIMARY KEY,
                    peer_mac TEXT NOT NULL,
                    is_outgoing INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    file_type TEXT NOT NULL DEFAULT 'none',
                    file_name TEXT,
                    file_size INTEGER,
                    file_hash TEXT,
                    file_path TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_messages_peer_mac ON messages(peer_mac);

                CREATE TABLE IF NOT EXISTS file_transfers (
                    transfer_id TEXT PRIMARY KEY,
                    peer_mac TEXT NOT NULL,
                    file_type TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    total_chunks INTEGER NOT NULL,
                    received_chunks INTEGER NOT NULL DEFAULT 0,
                    file_hash TEXT NOT NULL,
                    save_path TEXT NOT NULL,
                    is_completed INTEGER NOT NULL DEFAULT 0,
                    is_outgoing INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS file_chunks_tracker (
                    transfer_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    PRIMARY KEY (transfer_id, chunk_index)
                );

                CREATE TABLE IF NOT EXISTS outbound_chunks_tracker (
                    transfer_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    PRIMARY KEY (transfer_id, chunk_index)
                );
                ",
            )?;
            Ok(())
        })
            .await
            .map_err(|e| format!("Failed to create DB tables: {e}"))
    }

    async fn rebuild_schema(db: &AsyncConnection) -> Result<(), String> {
        db.call(|conn| {
            conn.execute_batch(
                "
                DROP TABLE IF EXISTS outbound_chunks_tracker;
                DROP TABLE IF EXISTS file_chunks_tracker;
                DROP TABLE IF EXISTS file_transfers;
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
                    status TEXT NOT NULL,
                    file_type TEXT NOT NULL DEFAULT 'none',
                    file_name TEXT,
                    file_size INTEGER,
                    file_hash TEXT,
                    file_path TEXT
                );

                CREATE INDEX idx_messages_peer_mac ON messages(peer_mac);

                CREATE TABLE file_transfers (
                    transfer_id TEXT PRIMARY KEY,
                    peer_mac TEXT NOT NULL,
                    file_type TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    total_chunks INTEGER NOT NULL,
                    received_chunks INTEGER NOT NULL DEFAULT 0,
                    file_hash TEXT NOT NULL,
                    save_path TEXT NOT NULL,
                    is_completed INTEGER NOT NULL DEFAULT 0,
                    is_outgoing INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE file_chunks_tracker (
                    transfer_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    PRIMARY KEY (transfer_id, chunk_index)
                );

                CREATE TABLE outbound_chunks_tracker (
                    transfer_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    PRIMARY KEY (transfer_id, chunk_index)
                );
                ",
            )?;
            Ok(())
        })
            .await
            .map_err(|e| format!("Failed to rebuild DB schema: {e}"))
    }

    pub async fn upsert_peer(&self, record: PeerRecord) {
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

        res.unwrap_or_default()
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

    pub async fn has_message(&self, msg_id: &str) -> bool {
        let msg_id_owned = msg_id.to_string();
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare("SELECT 1 FROM messages WHERE msg_id = ?1 LIMIT 1")?;
                let exists = stmt.exists(params![msg_id_owned])?;
                Ok(exists)
            })
            .await
            .unwrap_or(false)
    }

    pub async fn append_message(&self, peer_mac: String, msg: PersistentMessage) {
        info!(msg_id = %msg.msg_id, peer_mac = %peer_mac, "Persisting message to SQLite");

        let res = self
            .db
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO messages (msg_id, peer_mac, is_outgoing, content, timestamp, status, file_type, file_name, file_size, file_hash, file_path)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        msg.msg_id,
                        peer_mac,
                        if msg.is_outgoing { 1 } else { 0 },
                        msg.content,
                        msg.timestamp,
                        msg.status.as_str(),
                        msg.file_type.as_str(),
                        msg.file_name,
                        msg.file_size,
                        msg.file_hash,
                        msg.file_path,
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
            _ => false,
        }
    }

    pub async fn get_messages_for_peer(&self, peer_mac: &str) -> Vec<PersistentMessage> {
        let mac_owned = peer_mac.to_string();
        let res = self
            .db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT msg_id, peer_mac, is_outgoing, content, timestamp, status, file_type, file_name, file_size, file_hash, file_path
                     FROM messages WHERE peer_mac = ?1 ORDER BY timestamp ASC",
                )?;
                let rows = stmt.query_map(params![mac_owned], |row| {
                    let is_outgoing_int: i32 = row.get(2)?;
                    let status_str: String = row.get(5)?;
                    let ft_str: String = row.get(6)?;
                    Ok(PersistentMessage {
                        msg_id: row.get(0)?,
                        peer_mac: row.get(1)?,
                        is_outgoing: is_outgoing_int == 1,
                        content: row.get(3)?,
                        timestamp: row.get(4)?,
                        status: MessageStatus::from_str(&status_str),
                        file_type: FileType::from_str(&ft_str),
                        file_name: row.get(7)?,
                        file_size: row.get(8)?,
                        file_hash: row.get(9)?,
                        file_path: row.get(10)?,
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
                    "SELECT msg_id, peer_mac, is_outgoing, content, timestamp, status, file_type, file_name, file_size, file_hash, file_path
                     FROM messages WHERE peer_mac = ?1 AND is_outgoing = 1 AND status = ?2 ORDER BY timestamp ASC",
                )?;
                let rows = stmt.query_map(params![mac_owned, pending_status], |row| {
                    let status_str: String = row.get(5)?;
                    let ft_str: String = row.get(6)?;
                    Ok(PersistentMessage {
                        msg_id: row.get(0)?,
                        peer_mac: row.get(1)?,
                        is_outgoing: true,
                        content: row.get(3)?,
                        timestamp: row.get(4)?,
                        status: MessageStatus::from_str(&status_str),
                        file_type: FileType::from_str(&ft_str),
                        file_name: row.get(7)?,
                        file_size: row.get(8)?,
                        file_hash: row.get(9)?,
                        file_path: row.get(10)?,
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

    pub async fn insert_file_transfer(&self, record: FileTransferRecord) {
        let res = self
            .db
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO file_transfers
                     (transfer_id, peer_mac, file_type, file_name, file_size, total_chunks, received_chunks, file_hash, save_path, is_completed, is_outgoing)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        record.transfer_id,
                        record.peer_mac,
                        record.file_type.as_str(),
                        record.file_name,
                        record.file_size,
                        record.total_chunks,
                        record.received_chunks,
                        record.file_hash,
                        record.save_path,
                        if record.is_completed { 1 } else { 0 },
                        if record.is_outgoing { 1 } else { 0 },
                    ],
                )?;
                Ok(())
            })
            .await;

        if let Err(e) = res {
            error!(error = %e, "Failed to persist file transfer record");
        }
    }

    pub async fn record_chunk_received(&self, transfer_id: String, chunk_index: u32) -> Result<u32, String> {
        self.db
            .call(move |conn| {
                let tx = conn.transaction()?;

                let inserted = tx.execute(
                    "INSERT OR IGNORE INTO file_chunks_tracker (transfer_id, chunk_index) VALUES (?1, ?2)",
                    params![transfer_id, chunk_index],
                )?;

                if inserted > 0 {
                    tx.execute(
                        "UPDATE file_transfers SET received_chunks = received_chunks + 1 WHERE transfer_id = ?1",
                        params![transfer_id],
                    )?;
                }

                let mut stmt = tx.prepare("SELECT received_chunks, total_chunks FROM file_transfers WHERE transfer_id = ?1")?;
                let mut rows = stmt.query(params![transfer_id])?;

                if let Some(row) = rows.next()? {
                    let recv: u32 = row.get(0)?;
                    let total: u32 = row.get(1)?;
                    if recv >= total {
                        tx.execute(
                            "UPDATE file_transfers SET is_completed = 1 WHERE transfer_id = ?1",
                            params![transfer_id],
                        )?;
                    }
                    drop(rows);
                    drop(stmt);
                    tx.commit()?;
                    Ok(recv)
                } else {
                    Err(rusqlite::Error::QueryReturnedNoRows.into())
                }
            })
            .await
            .map_err(|e| format!("Failed to record chunk: {e}"))
    }

    pub async fn record_outbound_chunk_ack(&self, transfer_id: String, chunk_index: u32) {
        let _ = self
            .db
            .call(move |conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO outbound_chunks_tracker (transfer_id, chunk_index) VALUES (?1, ?2)",
                    params![transfer_id, chunk_index],
                )?;
                Ok(())
            })
            .await;
    }

    pub async fn get_acked_outbound_chunks(&self, transfer_id: &str) -> HashSet<u32> {
        let tid = transfer_id.to_string();
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare("SELECT chunk_index FROM outbound_chunks_tracker WHERE transfer_id = ?1")?;
                let rows = stmt.query_map(params![tid], |r| r.get(0))?;
                let mut set = HashSet::new();
                for r in rows {
                    if let Ok(idx) = r {
                        set.insert(idx);
                    }
                }
                Ok(set)
            })
            .await
            .unwrap_or_default()
    }

    pub async fn clear_file_transfer_trackers(&self, transfer_id: &str) {
        let tid = transfer_id.to_string();
        let _ = self
            .db
            .call(move |conn| {
                conn.execute("DELETE FROM file_chunks_tracker WHERE transfer_id = ?1", params![tid.clone()])?;
                conn.execute("DELETE FROM outbound_chunks_tracker WHERE transfer_id = ?1", params![tid])?;
                Ok(())
            })
            .await;
    }

    pub async fn mark_transfer_completed(&self, transfer_id: &str) {
        let tid = transfer_id.to_string();
        let _ = self
            .db
            .call(move |conn| {
                conn.execute("UPDATE file_transfers SET is_completed = 1 WHERE transfer_id = ?1", params![tid])?;
                Ok(())
            })
            .await;
    }

    pub async fn get_pending_outbound_transfers(&self, peer_mac: &str) -> Vec<FileTransferRecord> {
        let mac_owned = peer_mac.to_string();
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT transfer_id, peer_mac, file_type, file_name, file_size, total_chunks, received_chunks, file_hash, save_path, is_completed, is_outgoing
                     FROM file_transfers WHERE peer_mac = ?1 AND is_outgoing = 1 AND is_completed = 0",
                )?;
                let rows = stmt.query_map(params![mac_owned], |row| {
                    let ft_str: String = row.get(2)?;
                    let completed_int: i32 = row.get(9)?;
                    let outgoing_int: i32 = row.get(10)?;
                    Ok(FileTransferRecord {
                        transfer_id: row.get(0)?,
                        peer_mac: row.get(1)?,
                        file_type: FileType::from_str(&ft_str),
                        file_name: row.get(3)?,
                        file_size: row.get(4)?,
                        total_chunks: row.get(5)?,
                        received_chunks: row.get(6)?,
                        file_hash: row.get(7)?,
                        save_path: row.get(8)?,
                        is_completed: completed_int == 1,
                        is_outgoing: outgoing_int == 1,
                    })
                })?;

                let mut list = Vec::new();
                for r in rows {
                    if let Ok(record) = r {
                        list.push(record);
                    }
                }
                Ok(list)
            })
            .await
            .unwrap_or_default()
    }

    pub async fn get_file_transfer(&self, transfer_id: &str) -> Option<FileTransferRecord> {
        let tid = transfer_id.to_string();
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT transfer_id, peer_mac, file_type, file_name, file_size, total_chunks, received_chunks, file_hash, save_path, is_completed, is_outgoing
                     FROM file_transfers WHERE transfer_id = ?1",
                )?;
                let mut rows = stmt.query_map(params![tid], |row| {
                    let ft_str: String = row.get(2)?;
                    let completed_int: i32 = row.get(9)?;
                    let outgoing_int: i32 = row.get(10)?;
                    Ok(FileTransferRecord {
                        transfer_id: row.get(0)?,
                        peer_mac: row.get(1)?,
                        file_type: FileType::from_str(&ft_str),
                        file_name: row.get(3)?,
                        file_size: row.get(4)?,
                        total_chunks: row.get(5)?,
                        received_chunks: row.get(6)?,
                        file_hash: row.get(7)?,
                        save_path: row.get(8)?,
                        is_completed: completed_int == 1,
                        is_outgoing: outgoing_int == 1,
                    })
                })?;

                if let Some(res) = rows.next() {
                    Ok(res.ok())
                } else {
                    Ok(None)
                }
            })
            .await
            .unwrap_or(None)
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

        let engine = self.clone();
        tokio::spawn(async move {
            engine.flush_outbox(&recipient_mac).await;
        });

        Ok(())
    }

    pub async fn flush_outbox(&self, peer_mac: &str) {
        let pending = self.storage.get_pending_messages(peer_mac).await;

        // 尝试断点续传未能发完的文件
        self.resume_pending_file_transfers(peer_mac).await;

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
                self.flush_outbox(&mac).await;
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
                self.flush_outbox(&mac).await;
            }
        }

        self.emit_event("PEER_LIST_UPDATED".to_string()).await;
    }

    async fn get_or_connect(&self, peer_mac: &str) -> Result<Arc<PeerConnection>, String> {
        if let Some(conn) = self.connections.read().await.get(peer_mac) {
            return Ok(Arc::clone(conn));
        }

        let peer_record = self
            .storage
            .get_peer(peer_mac)
            .await
            .ok_or_else(|| format!("Peer MAC {} not found in local storage", peer_mac))?;

        let addr = format!("{}:{}", peer_record.last_known_ip, peer_record.port);
        info!(peer_mac = %peer_mac, address = %addr, "Connecting to peer");

        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("Connect failed: {e}"))?;
        let (_tx, conn) = self
            .setup_connection(
                stream,
                peer_record.last_known_ip.clone(),
                peer_mac.to_string(),
            )
            .await;

        let handshake = MessageEnvelope {
            version: 1,
            msg_id: Uuid::new_v4().to_string(),
            sender_mac: self.self_mac.clone(),
            payload: MessagePayload::Handshake {
                sender_mac: self.self_mac.clone(),
            },
        };

        conn.send_envelope(&handshake).await?;
        self.connections
            .write()
            .await
            .insert(peer_mac.to_string(), Arc::clone(&conn));
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

        let engine = self.clone();
        let conn_clone = Arc::clone(&conn);
        tokio::spawn(async move {
            engine
                .handle_read_loop(read_half, conn_clone, peer_mac, remote_ip)
                .await;
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
                    self.connections
                        .write()
                        .await
                        .insert(sender_mac.clone(), Arc::clone(&conn));
                    self.storage.set_peer_online_status(&sender_mac, true).await;
                    self.emit_event("PEER_LIST_UPDATED".to_string()).await;
                }
                MessagePayload::ChatMessage { content } => {
                    if current_peer_mac.is_empty() {
                        current_peer_mac = env.sender_mac.clone();
                        *conn.remote_mac.write().await = env.sender_mac.clone();
                        self.connections
                            .write()
                            .await
                            .insert(env.sender_mac.clone(), Arc::clone(&conn));
                        self.storage
                            .set_peer_online_status(&env.sender_mac, true)
                            .await;
                        self.emit_event("PEER_LIST_UPDATED".to_string()).await;
                    }

                    if !self.storage.has_message(&env.msg_id).await {
                        let msg = PersistentMessage {
                            msg_id: env.msg_id.clone(),
                            peer_mac: env.sender_mac.clone(),
                            is_outgoing: false,
                            content,
                            timestamp: chrono_now_timestamp(),
                            status: MessageStatus::Acked,
                            file_type: FileType::None,
                            file_name: None,
                            file_size: None,
                            file_hash: None,
                            file_path: None,
                        };

                        self.storage
                            .append_message(env.sender_mac.clone(), msg)
                            .await;
                        self.emit_event(format!("NEW_MSG:{}", env.sender_mac)).await;
                    }

                    let ack = MessageEnvelope {
                        version: 1,
                        msg_id: Uuid::new_v4().to_string(),
                        sender_mac: self.self_mac.clone(),
                        payload: MessagePayload::Ack {
                            target_msg_id: env.msg_id,
                        },
                    };

                    let _ = conn.send_envelope(&ack).await;
                }
                MessagePayload::FileTransferInit {
                    transfer_id,
                    file_type,
                    file_name,
                    file_size,
                    total_chunks,
                    file_hash,
                } => {
                    let peer_mac = if !current_peer_mac.is_empty() {
                        current_peer_mac.clone()
                    } else {
                        env.sender_mac.clone()
                    };

                    let download_dir = std::env::temp_dir().join("p2p_downloads");
                    let _ = tokio::fs::create_dir_all(&download_dir).await;
                    let target_path = download_dir.join(&file_name).to_string_lossy().to_string();

                    let record = FileTransferRecord {
                        transfer_id: transfer_id.clone(),
                        peer_mac: peer_mac.clone(),
                        file_type,
                        file_name,
                        file_size,
                        total_chunks,
                        received_chunks: 0,
                        file_hash,
                        save_path: target_path,
                        is_completed: false,
                        is_outgoing: false,
                    };

                    self.storage.insert_file_transfer(record).await;

                    let ack = MessageEnvelope {
                        version: 1,
                        msg_id: Uuid::new_v4().to_string(),
                        sender_mac: self.self_mac.clone(),
                        payload: MessagePayload::Ack {
                            target_msg_id: transfer_id,
                        },
                    };
                    let _ = conn.send_envelope(&ack).await;
                }
                MessagePayload::FileChunk { header, data } => {
                    let transfer = self.storage.get_file_transfer(&header.transfer_id).await;

                    if let Some(record) = transfer {
                        let path = record.save_path.clone();
                        let offset = header.offset;
                        let chunk_data = data;

                        let write_res = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                            use std::fs::OpenOptions;
                            use std::io::{Seek, SeekFrom, Write};

                            let mut file = OpenOptions::new()
                                .create(true)
                                .write(true)
                                .open(&path)?;

                            file.seek(SeekFrom::Start(offset))?;
                            file.write_all(&chunk_data)?;
                            Ok(())
                        })
                            .await;

                        if let Ok(Ok(())) = write_res {
                            if let Ok(recv_count) = self
                                .storage
                                .record_chunk_received(header.transfer_id.clone(), header.chunk_index)
                                .await
                            {
                                // 单独向发送端返回 ChunkAck 存储断点记录
                                let chunk_ack = MessageEnvelope {
                                    version: 1,
                                    msg_id: Uuid::new_v4().to_string(),
                                    sender_mac: self.self_mac.clone(),
                                    payload: MessagePayload::ChunkAck {
                                        transfer_id: header.transfer_id.clone(),
                                        chunk_index: header.chunk_index,
                                    },
                                };
                                let _ = conn.send_envelope(&chunk_ack).await;

                                if recv_count >= record.total_chunks {
                                    // 传输全部完成！向接收端的 messages 表写入这条 File 消息
                                    let msg = PersistentMessage {
                                        msg_id: header.transfer_id.clone(),
                                        peer_mac: record.peer_mac.clone(),
                                        is_outgoing: false,
                                        content: format!("[File: {}]", record.file_name),
                                        timestamp: chrono_now_timestamp(),
                                        status: MessageStatus::Acked,
                                        file_type: record.file_type,
                                        file_name: Some(record.file_name),
                                        file_size: Some(record.file_size),
                                        file_hash: Some(record.file_hash),
                                        file_path: Some(record.save_path),
                                    };
                                    self.storage.append_message(record.peer_mac.clone(), msg).await;

                                    // 给 Sender 发送最终完成 Ack
                                    let complete_ack = MessageEnvelope {
                                        version: 1,
                                        msg_id: Uuid::new_v4().to_string(),
                                        sender_mac: self.self_mac.clone(),
                                        payload: MessagePayload::FileCompleteAck {
                                            transfer_id: header.transfer_id.clone(),
                                        },
                                    };
                                    let _ = conn.send_envelope(&complete_ack).await;

                                    self.emit_event(format!("FILE_COMPLETE:{}", header.transfer_id)).await;
                                    self.emit_event(format!("NEW_MSG:{}", record.peer_mac)).await;
                                } else {
                                    self.emit_event(format!(
                                        "FILE_PROGRESS:{}:{}",
                                        header.transfer_id,
                                        (recv_count as f32 / record.total_chunks as f32 * 100.0) as u32
                                    ))
                                        .await;
                                }
                            }
                        }
                    }
                }
                MessagePayload::ChunkAck { transfer_id, chunk_index } => {
                    self.storage.record_outbound_chunk_ack(transfer_id, chunk_index).await;
                }
                MessagePayload::FileCompleteAck { transfer_id } => {
                    let target = if !current_peer_mac.is_empty() {
                        &current_peer_mac
                    } else {
                        &env.sender_mac
                    };

                    self.storage.mark_transfer_completed(&transfer_id).await;
                    self.storage.clear_file_transfer_trackers(&transfer_id).await;

                    if self.storage.mark_acked(target, &transfer_id).await {
                        self.emit_event(format!("ACK:{}", target)).await;
                    }
                    self.emit_event(format!("FILE_SENT:{}", transfer_id)).await;
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
            self.storage
                .set_peer_online_status(&current_peer_mac, false)
                .await;
            self.emit_event("PEER_LIST_UPDATED".to_string()).await;
            info!(peer_mac = %current_peer_mac, ip = %current_peer_ip, "Closed peer connection removed and marked offline");
        }
    }
}

// =============================================================================
// 5. Utility Functions & 断网重传实现
// =============================================================================

pub fn chrono_now_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

impl NetworkEngine {
    pub async fn send_file(&self, recipient_mac: String, file_path_str: String) -> Result<String, String> {
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

        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
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

        // 1. 持久化发送端记录 (file_transfers + messages)
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

        // 2. 异步尝试发射 Chunk（如果网络不可用，静默记录日志并由 Outbox 托管）
        let engine = self.clone();
        let recipient = recipient_mac.clone();
        let tid = transfer_id.clone();
        tokio::spawn(async move {
            if let Err(e) = engine.dispatch_file_chunks(&recipient, &tid).await {
                warn!(transfer_id = %tid, error = %e, "Initial file chunk dispatch failed (queued in outbox)");
            }
        });

        // 立即返回 transfer_id
        Ok(transfer_id)
    }

    /// 断点重传核心逻辑：读取对方 Ack 历史，仅把缺少的 Chunk 重发过去
    async fn dispatch_file_chunks(&self, recipient_mac: &str, transfer_id: &str) -> Result<(), String> {
        let record = self
            .storage
            .get_file_transfer(transfer_id)
            .await
            .ok_or_else(|| format!("Transfer ID {} not found", transfer_id))?;

        let conn = self.get_or_connect(recipient_mac).await?;

        // 1. 发送 Init 报文
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

        // 2. 读取发送方已经被 Ack 成功的 Chunk 集合（用于断点续传）
        let acked_chunks = self.storage.get_acked_outbound_chunks(transfer_id).await;

        let path = PathBuf::from(&record.save_path);
        let file_bytes = fs::read(&path).map_err(|e| format!("Failed to read file contents: {e}"))?;

        const CHUNK_SIZE: usize = 64 * 1024;
        let mut offset: u64 = 0;

        for chunk_index in 0..record.total_chunks {
            let start = (chunk_index as usize) * CHUNK_SIZE;
            let end = std::cmp::min(start + CHUNK_SIZE, file_bytes.len());
            let chunk_data = file_bytes[start..end].to_vec();
            offset = start as u64;

            // 断点续传：若之前此 Chunk 已接收到 Ack，直接跳过！
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

            self.emit_event(format!(
                "FILE_SEND_PROGRESS:{}:{}",
                transfer_id,
                ((chunk_index + 1) as f32 / record.total_chunks as f32 * 100.0) as u32
            )).await;
        }

        Ok(())
    }

    /// 自动重传该 Peer 未完成的文件传输任务
    pub async fn resume_pending_file_transfers(&self, peer_mac: &str) {
        let pending_transfers = self.storage.get_pending_outbound_transfers(peer_mac).await;
        for record in pending_transfers {
            info!(transfer_id = %record.transfer_id, peer_mac = %peer_mac, "Resuming file transfer task...");
            let _ = self.dispatch_file_chunks(peer_mac, &record.transfer_id).await;
        }
    }
}

```

---

## File: rust/src/api/file_transfer/service.rs

```rust
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

```

---

## File: rust/src/api/file_transfer/mod.rs

```rust
pub mod traits;
pub mod service;
```

---

## File: rust/src/api/file_transfer/traits.rs

```rust
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

```

---

## File: rust/src/api/discovery.rs

```rust
use mdns_sd::{IfKind, ServiceDaemon, ServiceEvent, ServiceInfo};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::Duration;
use tracing::{debug, error, info, warn};

pub struct ProtocolConfig;

impl ProtocolConfig {
    pub const SERVICE_PORT: u16 = 14981;
    const SERVICE_TYPE: &str = "_ylchat._tcp.local.";
}

static MDNS_DAEMON: OnceLock<ServiceDaemon> = OnceLock::new();
static REGD_SERVICE: OnceLock<ServiceInfo> = OnceLock::new();

fn get_or_init_daemon() -> Result<&'static ServiceDaemon, String> {
    MDNS_DAEMON.get_or_init(|| {
        let daemon = ServiceDaemon::new().expect("Failed to initialize mDNS daemon");

        if let Some(ip_str) = get_local_ip() {
            if let Ok(ip) = ip_str.parse::<std::net::IpAddr>() {
                if let Err(e) = daemon.enable_interface(IfKind::from(ip)) {
                    warn!("Failed to enable mDNS interface for {}: {}", ip_str, e);
                }
            }
        }

        daemon
    });
    Ok(MDNS_DAEMON.get().unwrap())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerInfo {
    pub mac_address: String,
    pub ip: String,
    pub device_name: String,
}

pub fn register_bonjour_service(mac_address: String) -> Result<(), String> {
    let mdns = get_or_init_daemon()?;

    let device_id = get_device_identifier();
    let service_instance_name = format!("yl_chat_{}", device_id);
    let host_name = format!("{}.local.", device_id);
    let local_ip = get_local_ip().unwrap_or_else(|| "127.0.0.1".into());

    // Send the unique identifier in the mDNS TXT record
    let mut txt_properties = HashMap::new();
    txt_properties.insert("mac".to_string(), mac_address);

    let service_info = ServiceInfo::new(
        ProtocolConfig::SERVICE_TYPE,
        &service_instance_name,
        &host_name,
        &local_ip,
        ProtocolConfig::SERVICE_PORT,
        txt_properties,
    )
        .map_err(|e| format!("Failed to create service info: {e}"))?;

    mdns.register(service_info.clone())
        .map_err(|e| format!("Failed to register mDNS service: {e}"))?;

    let _ = REGD_SERVICE.set(service_info);

    info!(host = %host_name, ip = %local_ip, "mDNS Bonjour service registered successfully");
    Ok(())
}

/// Asynchronous mDNS LAN scanning using tokio::task::spawn_blocking
/// Filter out both self IP and self MAC address.
pub async fn scan_lan_peers(self_mac: String) -> Result<Vec<PeerInfo>, String> {
    tokio::task::spawn_blocking(move || {
        let mdns = get_or_init_daemon()?;

        let receiver = mdns
            .browse(ProtocolConfig::SERVICE_TYPE)
            .map_err(|e| format!("Failed to browse mDNS: {e}"))?;

        let my_ip = get_local_ip();
        let mut peers = HashSet::new();
        let timeout = Duration::from_secs(3);
        let start = std::time::Instant::now();

        debug!(self_mac = %self_mac, "Starting non-blocking LAN Peer scan via mDNS...");

        while start.elapsed() < timeout {
            if let Ok(event) = receiver.recv_timeout(Duration::from_millis(100)) {
                if let ServiceEvent::ServiceResolved(info) = event {
                    let device_name = info
                        .get_hostname()
                        .trim_end_matches('.')
                        .trim_end_matches(".local")
                        .to_string();

                    // 1. 提取 TXT 记录中的 mac 属性字段并存起来
                    let mac_address = info
                        .get_property_val_str("mac")
                        .unwrap_or_default()
                        .to_string();

                    // 2. 过滤无有效 MAC 的节点，或 MAC 属于自己的节点
                    if mac_address.is_empty() || mac_address.eq_ignore_ascii_case(&self_mac) {
                        continue;
                    }

                    for ip in info.get_addresses() {
                        let ip_str = ip.to_string();

                        if ip.is_ipv4() && !ip_str.starts_with("127.") {
                            // 3. 同时根据 IP 过滤本机自身
                            let is_self_ip = my_ip.as_ref().map_or(false, |local| local == &ip_str);

                            if !is_self_ip {
                                peers.insert(PeerInfo {
                                    mac_address: mac_address.clone(), // 存储 MAC 地址
                                    ip: ip_str,
                                    device_name: device_name.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }

        debug!(discovered_count = peers.len(), "LAN Peer scan completed");
        Ok(peers.into_iter().collect())
    })
        .await
        .map_err(|e| format!("JoinError during mDNS scan execution: {e}"))?
}

fn get_local_ip() -> Option<String> {
    if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
        if socket.connect("8.8.8.8:80").is_ok() {
            if let Ok(addr) = socket.local_addr() {
                return Some(addr.ip().to_string());
            }
        }
    }
    None
}

pub fn get_device_identifier() -> String {
    let raw = gethostname::gethostname().to_string_lossy().to_string();
    raw.trim_end_matches(".local")
        .replace(['.', ' ', '\''], "_")
}

```

---

## File: rust/src/api/utils/mod.rs

```rust
use std::time::{SystemTime, UNIX_EPOCH};

/// 获取当前时间的毫秒级 UTC 时间戳
pub fn chrono_now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 计算分块传输的 Block 数量
pub fn calculate_total_chunks(file_size: u64, chunk_size: usize) -> u32 {
    if file_size == 0 {
        return 1;
    }
    ((file_size as usize + chunk_size - 1) / chunk_size) as u32
}

```

---

## File: rust/src/api/models/mod.rs

```rust
use serde::{Deserialize, Serialize};

/// 节点设备记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRecord {
    pub mac_address: String,
    pub last_known_ip: String,
    pub port: u16,
    pub device_name: String,
    pub last_seen: i64,
    pub is_online: bool,
}

/// 消息发送/接收状态
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageStatus {
    Pending,
    Acked,
    Failed,
}

impl MessageStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageStatus::Pending => "pending",
            MessageStatus::Acked => "acked",
            MessageStatus::Failed => "failed",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "acked" => MessageStatus::Acked,
            "failed" => MessageStatus::Failed,
            _ => MessageStatus::Pending,
        }
    }
}

/// 文件传输类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    None,
    Image,
    Video,
    Generic,
}

impl FileType {
    pub fn as_str(&self) -> &'static str {
        match self {
            FileType::None => "none",
            FileType::Image => "image",
            FileType::Video => "video",
            FileType::Generic => "generic",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "image" => FileType::Image,
            "video" => FileType::Video,
            "generic" => FileType::Generic,
            _ => FileType::None,
        }
    }
}

/// 数据库持久化的消息记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentMessage {
    pub msg_id: String,
    pub peer_mac: String,
    pub is_outgoing: bool,
    pub content: String,
    pub timestamp: i64,
    pub status: MessageStatus,
    pub file_type: FileType,
    pub file_name: Option<String>,
    pub file_size: Option<u64>,
    pub file_hash: Option<String>,
    pub file_path: Option<String>,
}

/// 文件分块头信息 (Chunk Header)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkHeader {
    pub transfer_id: String,
    pub chunk_uuid: String,
    pub chunk_index: u32,
    pub total_chunks: u32,
    pub offset: u64,
    pub chunk_hash: String,
    pub file_hash: String,
}

/// 数据库持久化的文件传输记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileTransferRecord {
    pub transfer_id: String,
    pub peer_mac: String,
    pub file_type: FileType,
    pub file_name: String,
    pub file_size: u64,
    pub total_chunks: u32,
    pub received_chunks: u32,
    pub file_hash: String,
    pub save_path: String,
    pub is_completed: bool,
    pub is_outgoing: bool,
}

/// 网络通信信封包 Payload 定义
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum MessagePayload {
    Handshake {
        sender_mac: String,
    },
    ChatMessage {
        content: String,
    },
    FileTransferInit {
        transfer_id: String,
        file_type: FileType,
        file_name: String,
        file_size: u64,
        total_chunks: u32,
        file_hash: String,
    },
    FileChunk {
        header: ChunkHeader,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    ChunkAck {
        transfer_id: String,
        chunk_index: u32,
    },
    FileCompleteAck {
        transfer_id: String,
    },
    Ack {
        target_msg_id: String,
    }
}

/// 网络传输顶级信封对象 (Envelope)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEnvelope {
    pub version: u8,
    pub msg_id: String,
    pub sender_mac: String,
    pub payload: MessagePayload,
}

```

---

## File: rust/src/api/storage/sqlite.rs

```rust
use super::traits::{StorageRepository, StorageRepositoryHandle};
use crate::api::models::{FileTransferRecord, FileType, MessageStatus, PeerRecord, PersistentMessage};
use async_trait::async_trait;
use rusqlite::params;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use flutter_rust_bridge::frb;
use tokio_rusqlite::Connection as AsyncConnection;
use tracing::{error, info, warn};

pub struct LocalStorage {
    db: AsyncConnection,
}

#[frb(ignore)]
impl LocalStorage {
    pub async fn open(db_path: PathBuf) -> Result<Self, String> {
        info!(path = ?db_path, "Opening SQLite database");

        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("Failed to create database directory: {:?}", e))?;
            }
        }

        let db = AsyncConnection::open(db_path)
            .await
            .map_err(|e| format!("Failed to open SQLite database: {e}"))?;

        let check_schema = db
            .call(|conn| {
                let mut stmt = conn.prepare("PRAGMA table_info(messages)")?;
                let mut has_file_type = false;
                let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;

                for col in rows {
                    if let Ok(name) = col {
                        if name == "file_type" {
                            has_file_type = true;
                            break;
                        }
                    }
                }

                if !has_file_type {
                    return Err(rusqlite::Error::ModuleError(
                        "Schema mismatch: missing file fields in messages".to_string(),
                    ).into());
                }
                Ok(())
            })
            .await;

        if check_schema.is_err() {
            warn!("Database schema incompatibility detected. Rebuilding database tables...");
            Self::rebuild_schema(&db).await?;
        } else {
            if let Err(e) = Self::init_db_tables(&db).await {
                warn!(error = %e, "DB Initialization failed. Dropping and re-creating...");
                Self::rebuild_schema(&db).await?;
            }
        }

        Ok(Self { db })
    }

    async fn init_db_tables(db: &AsyncConnection) -> Result<(), String> {
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
                    last_seen INTEGER NOT NULL,
                    is_online INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS messages (
                    msg_id TEXT PRIMARY KEY,
                    peer_mac TEXT NOT NULL,
                    is_outgoing INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    file_type TEXT NOT NULL DEFAULT 'none',
                    file_name TEXT,
                    file_size INTEGER,
                    file_hash TEXT,
                    file_path TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_messages_peer_mac ON messages(peer_mac);

                CREATE TABLE IF NOT EXISTS file_transfers (
                    transfer_id TEXT PRIMARY KEY,
                    peer_mac TEXT NOT NULL,
                    file_type TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    total_chunks INTEGER NOT NULL,
                    received_chunks INTEGER NOT NULL DEFAULT 0,
                    file_hash TEXT NOT NULL,
                    save_path TEXT NOT NULL,
                    is_completed INTEGER NOT NULL DEFAULT 0,
                    is_outgoing INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS file_chunks_tracker (
                    transfer_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    PRIMARY KEY (transfer_id, chunk_index)
                );

                CREATE TABLE IF NOT EXISTS outbound_chunks_tracker (
                    transfer_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    PRIMARY KEY (transfer_id, chunk_index)
                );
                ",
            )?;
            Ok(())
        })
            .await
            .map_err(|e| format!("Failed to create DB tables: {e}"))
    }

    async fn rebuild_schema(db: &AsyncConnection) -> Result<(), String> {
        db.call(|conn| {
            conn.execute_batch(
                "
                DROP TABLE IF EXISTS outbound_chunks_tracker;
                DROP TABLE IF EXISTS file_chunks_tracker;
                DROP TABLE IF EXISTS file_transfers;
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
                    status TEXT NOT NULL,
                    file_type TEXT NOT NULL DEFAULT 'none',
                    file_name TEXT,
                    file_size INTEGER,
                    file_hash TEXT,
                    file_path TEXT
                );

                CREATE INDEX idx_messages_peer_mac ON messages(peer_mac);

                CREATE TABLE file_transfers (
                    transfer_id TEXT PRIMARY KEY,
                    peer_mac TEXT NOT NULL,
                    file_type TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    total_chunks INTEGER NOT NULL,
                    received_chunks INTEGER NOT NULL DEFAULT 0,
                    file_hash TEXT NOT NULL,
                    save_path TEXT NOT NULL,
                    is_completed INTEGER NOT NULL DEFAULT 0,
                    is_outgoing INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE file_chunks_tracker (
                    transfer_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    PRIMARY KEY (transfer_id, chunk_index)
                );

                CREATE TABLE outbound_chunks_tracker (
                    transfer_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    PRIMARY KEY (transfer_id, chunk_index)
                );
                ",
            )?;
            Ok(())
        })
            .await
            .map_err(|e| format!("Failed to rebuild DB schema: {e}"))
    }
}

#[frb(ignore)]
#[async_trait]
impl StorageRepository for LocalStorage {
    async fn upsert_peer(&self, record: PeerRecord) {
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

    async fn set_peer_online_status(&self, mac: &str, is_online: bool) {
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

    async fn get_all_peers(&self) -> Vec<PeerRecord> {
        self.db
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
            .await
            .unwrap_or_default()
    }

    async fn get_peer(&self, mac: &str) -> Option<PeerRecord> {
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

    async fn has_message(&self, msg_id: &str) -> bool {
        let msg_id_owned = msg_id.to_string();
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare("SELECT 1 FROM messages WHERE msg_id = ?1 LIMIT 1")?;
                let exists = stmt.exists(params![msg_id_owned])?;
                Ok(exists)
            })
            .await
            .unwrap_or(false)
    }

    async fn append_message(&self, peer_mac: String, msg: PersistentMessage) {
        info!(msg_id = %msg.msg_id, peer_mac = %peer_mac, "Persisting message to SQLite");

        let res = self
            .db
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO messages (msg_id, peer_mac, is_outgoing, content, timestamp, status, file_type, file_name, file_size, file_hash, file_path)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        msg.msg_id,
                        peer_mac,
                        if msg.is_outgoing { 1 } else { 0 },
                        msg.content,
                        msg.timestamp,
                        msg.status.as_str(),
                        msg.file_type.as_str(),
                        msg.file_name,
                        msg.file_size,
                        msg.file_hash,
                        msg.file_path,
                    ],
                )?;
                Ok(())
            })
            .await;

        if let Err(e) = res {
            error!(error = %e, "Failed to append message to SQLite");
        }
    }

    async fn mark_acked(&self, peer_mac: &str, target_msg_id: &str) -> bool {
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
            _ => false,
        }
    }

    async fn get_messages_for_peer(&self, peer_mac: &str) -> Vec<PersistentMessage> {
        let mac_owned = peer_mac.to_string();
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT msg_id, peer_mac, is_outgoing, content, timestamp, status, file_type, file_name, file_size, file_hash, file_path
                     FROM messages WHERE peer_mac = ?1 ORDER BY timestamp ASC",
                )?;
                let rows = stmt.query_map(params![mac_owned], |row| {
                    let is_outgoing_int: i32 = row.get(2)?;
                    let status_str: String = row.get(5)?;
                    let ft_str: String = row.get(6)?;
                    Ok(PersistentMessage {
                        msg_id: row.get(0)?,
                        peer_mac: row.get(1)?,
                        is_outgoing: is_outgoing_int == 1,
                        content: row.get(3)?,
                        timestamp: row.get(4)?,
                        status: MessageStatus::from_str(&status_str),
                        file_type: FileType::from_str(&ft_str),
                        file_name: row.get(7)?,
                        file_size: row.get(8)?,
                        file_hash: row.get(9)?,
                        file_path: row.get(10)?,
                    })
                })?;

                let mut list = Vec::new();
                for msg in rows {
                    list.push(msg?);
                }
                Ok(list)
            })
            .await
            .unwrap_or_default()
    }

    async fn get_pending_messages(&self, peer_mac: &str) -> Vec<PersistentMessage> {
        let mac_owned = peer_mac.to_string();
        let pending_status = MessageStatus::Pending.as_str();

        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT msg_id, peer_mac, is_outgoing, content, timestamp, status, file_type, file_name, file_size, file_hash, file_path
                     FROM messages WHERE peer_mac = ?1 AND is_outgoing = 1 AND status = ?2 ORDER BY timestamp ASC",
                )?;
                let rows = stmt.query_map(params![mac_owned, pending_status], |row| {
                    let status_str: String = row.get(5)?;
                    let ft_str: String = row.get(6)?;
                    Ok(PersistentMessage {
                        msg_id: row.get(0)?,
                        peer_mac: row.get(1)?,
                        is_outgoing: true,
                        content: row.get(3)?,
                        timestamp: row.get(4)?,
                        status: MessageStatus::from_str(&status_str),
                        file_type: FileType::from_str(&ft_str),
                        file_name: row.get(7)?,
                        file_size: row.get(8)?,
                        file_hash: row.get(9)?,
                        file_path: row.get(10)?,
                    })
                })?;

                let mut list = Vec::new();
                for msg in rows {
                    list.push(msg?);
                }
                Ok(list)
            })
            .await
            .unwrap_or_default()
    }

    async fn insert_file_transfer(&self, record: FileTransferRecord) {
        let res = self
            .db
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO file_transfers
                     (transfer_id, peer_mac, file_type, file_name, file_size, total_chunks, received_chunks, file_hash, save_path, is_completed, is_outgoing)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        record.transfer_id,
                        record.peer_mac,
                        record.file_type.as_str(),
                        record.file_name,
                        record.file_size,
                        record.total_chunks,
                        record.received_chunks,
                        record.file_hash,
                        record.save_path,
                        if record.is_completed { 1 } else { 0 },
                        if record.is_outgoing { 1 } else { 0 },
                    ],
                )?;
                Ok(())
            })
            .await;

        if let Err(e) = res {
            error!(error = %e, "Failed to persist file transfer record");
        }
    }

    async fn record_chunk_received(&self, transfer_id: String, chunk_index: u32) -> Result<u32, String> {
        self.db
            .call(move |conn| {
                let tx = conn.transaction()?;

                let inserted = tx.execute(
                    "INSERT OR IGNORE INTO file_chunks_tracker (transfer_id, chunk_index) VALUES (?1, ?2)",
                    params![transfer_id, chunk_index],
                )?;

                if inserted > 0 {
                    tx.execute(
                        "UPDATE file_transfers SET received_chunks = received_chunks + 1 WHERE transfer_id = ?1",
                        params![transfer_id],
                    )?;
                }

                let mut stmt = tx.prepare("SELECT received_chunks, total_chunks FROM file_transfers WHERE transfer_id = ?1")?;
                let mut rows = stmt.query(params![transfer_id])?;

                if let Some(row) = rows.next()? {
                    let recv: u32 = row.get(0)?;
                    let total: u32 = row.get(1)?;
                    if recv >= total {
                        tx.execute(
                            "UPDATE file_transfers SET is_completed = 1 WHERE transfer_id = ?1",
                            params![transfer_id],
                        )?;
                    }
                    drop(rows);
                    drop(stmt);
                    tx.commit()?;
                    Ok(recv)
                } else {
                    Err(rusqlite::Error::QueryReturnedNoRows.into())
                }
            })
            .await
            .map_err(|e| format!("Failed to record chunk: {e}"))
    }

    async fn record_outbound_chunk_ack(&self, transfer_id: String, chunk_index: u32) {
        let _ = self
            .db
            .call(move |conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO outbound_chunks_tracker (transfer_id, chunk_index) VALUES (?1, ?2)",
                    params![transfer_id, chunk_index],
                )?;
                Ok(())
            })
            .await;
    }

    async fn get_acked_outbound_chunks(&self, transfer_id: &str) -> HashSet<u32> {
        let tid = transfer_id.to_string();
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare("SELECT chunk_index FROM outbound_chunks_tracker WHERE transfer_id = ?1")?;
                let rows = stmt.query_map(params![tid], |r| r.get(0))?;
                let mut set = HashSet::new();
                for r in rows {
                    if let Ok(idx) = r {
                        set.insert(idx);
                    }
                }
                Ok(set)
            })
            .await
            .unwrap_or_default()
    }

    async fn clear_file_transfer_trackers(&self, transfer_id: &str) {
        let tid = transfer_id.to_string();
        let _ = self
            .db
            .call(move |conn| {
                conn.execute("DELETE FROM file_chunks_tracker WHERE transfer_id = ?1", params![tid.clone()])?;
                conn.execute("DELETE FROM outbound_chunks_tracker WHERE transfer_id = ?1", params![tid])?;
                Ok(())
            })
            .await;
    }

    async fn mark_transfer_completed(&self, transfer_id: &str) {
        let tid = transfer_id.to_string();
        let _ = self
            .db
            .call(move |conn| {
                conn.execute("UPDATE file_transfers SET is_completed = 1 WHERE transfer_id = ?1", params![tid])?;
                Ok(())
            })
            .await;
    }

    async fn get_pending_outbound_transfers(&self, peer_mac: &str) -> Vec<FileTransferRecord> {
        let mac_owned = peer_mac.to_string();
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT transfer_id, peer_mac, file_type, file_name, file_size, total_chunks, received_chunks, file_hash, save_path, is_completed, is_outgoing
                     FROM file_transfers WHERE peer_mac = ?1 AND is_outgoing = 1 AND is_completed = 0",
                )?;
                let rows = stmt.query_map(params![mac_owned], |row| {
                    let ft_str: String = row.get(2)?;
                    let completed_int: i32 = row.get(9)?;
                    let outgoing_int: i32 = row.get(10)?;
                    Ok(FileTransferRecord {
                        transfer_id: row.get(0)?,
                        peer_mac: row.get(1)?,
                        file_type: FileType::from_str(&ft_str),
                        file_name: row.get(3)?,
                        file_size: row.get(4)?,
                        total_chunks: row.get(5)?,
                        received_chunks: row.get(6)?,
                        file_hash: row.get(7)?,
                        save_path: row.get(8)?,
                        is_completed: completed_int == 1,
                        is_outgoing: outgoing_int == 1,
                    })
                })?;

                let mut list = Vec::new();
                for r in rows {
                    if let Ok(record) = r {
                        list.push(record);
                    }
                }
                Ok(list)
            })
            .await
            .unwrap_or_default()
    }

    async fn get_file_transfer(&self, transfer_id: &str) -> Option<FileTransferRecord> {
        let tid = transfer_id.to_string();
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT transfer_id, peer_mac, file_type, file_name, file_size, total_chunks, received_chunks, file_hash, save_path, is_completed, is_outgoing
                     FROM file_transfers WHERE transfer_id = ?1",
                )?;
                let mut rows = stmt.query_map(params![tid], |row| {
                    let ft_str: String = row.get(2)?;
                    let completed_int: i32 = row.get(9)?;
                    let outgoing_int: i32 = row.get(10)?;
                    Ok(FileTransferRecord {
                        transfer_id: row.get(0)?,
                        peer_mac: row.get(1)?,
                        file_type: FileType::from_str(&ft_str),
                        file_name: row.get(3)?,
                        file_size: row.get(4)?,
                        total_chunks: row.get(5)?,
                        received_chunks: row.get(6)?,
                        file_hash: row.get(7)?,
                        save_path: row.get(8)?,
                        is_completed: completed_int == 1,
                        is_outgoing: outgoing_int == 1,
                    })
                })?;

                if let Some(res) = rows.next() {
                    Ok(res.ok())
                } else {
                    Ok(None)
                }
            })
            .await
            .unwrap_or(None)
    }
}

// =========================================================================
// FRB Opaque 包装层（供 Dart 端安全的生成和调用）
// =========================================================================

pub struct LocalStorageHandle {
    pub(crate) inner: Arc<LocalStorage>,
}

impl LocalStorageHandle {
    pub async fn open(db_path: String) -> Result<Self, String> {
        let path = PathBuf::from(db_path);
        let storage = LocalStorage::open(path).await?;
        Ok(Self {
            inner: Arc::new(storage),
        })
    }

    pub fn as_storage_repository(&self) -> StorageRepositoryHandle {
        StorageRepositoryHandle {
            inner: Arc::clone(&self.inner) as Arc<dyn StorageRepository>,
        }
    }

    // Peer 操作
    pub async fn upsert_peer(&self, record: PeerRecord) {
        self.inner.upsert_peer(record).await;
    }

    pub async fn set_peer_online_status(&self, mac: String, is_online: bool) {
        self.inner.set_peer_online_status(&mac, is_online).await;
    }

    pub async fn get_all_peers(&self) -> Vec<PeerRecord> {
        self.inner.get_all_peers().await
    }

    pub async fn get_peer(&self, mac: String) -> Option<PeerRecord> {
        self.inner.get_peer(&mac).await
    }

    // Message 操作
    pub async fn has_message(&self, msg_id: String) -> bool {
        self.inner.has_message(&msg_id).await
    }

    pub async fn append_message(&self, peer_mac: String, msg: PersistentMessage) {
        self.inner.append_message(peer_mac, msg).await;
    }

    pub async fn mark_acked(&self, peer_mac: String, target_msg_id: String) -> bool {
        self.inner.mark_acked(&peer_mac, &target_msg_id).await
    }

    pub async fn get_messages_for_peer(&self, peer_mac: String) -> Vec<PersistentMessage> {
        self.inner.get_messages_for_peer(&peer_mac).await
    }

    pub async fn get_pending_messages(&self, peer_mac: String) -> Vec<PersistentMessage> {
        self.inner.get_pending_messages(&peer_mac).await
    }

    // FileTransfer 操作
    pub async fn insert_file_transfer(&self, record: FileTransferRecord) {
        self.inner.insert_file_transfer(record).await;
    }

    pub async fn record_chunk_received(&self, transfer_id: String, chunk_index: u32) -> Result<u32, String> {
        self.inner.record_chunk_received(transfer_id, chunk_index).await
    }

    pub async fn record_outbound_chunk_ack(&self, transfer_id: String, chunk_index: u32) {
        self.inner.record_outbound_chunk_ack(transfer_id, chunk_index).await;
    }

    pub async fn get_acked_outbound_chunks(&self, transfer_id: String) -> Vec<u32> {
        self.inner.get_acked_outbound_chunks(&transfer_id).await.into_iter().collect()
    }

    pub async fn clear_file_transfer_trackers(&self, transfer_id: String) {
        self.inner.clear_file_transfer_trackers(&transfer_id).await;
    }

    pub async fn mark_transfer_completed(&self, transfer_id: String) {
        self.inner.mark_transfer_completed(&transfer_id).await;
    }

    pub async fn get_pending_outbound_transfers(&self, peer_mac: String) -> Vec<FileTransferRecord> {
        self.inner.get_pending_outbound_transfers(&peer_mac).await
    }

    pub async fn get_file_transfer(&self, transfer_id: String) -> Option<FileTransferRecord> {
        self.inner.get_file_transfer(&transfer_id).await
    }
}

```

---

## File: rust/src/api/storage/mod.rs

```rust
pub mod traits;
pub mod sqlite;
```

---

## File: rust/src/api/storage/traits.rs

```rust
use crate::api::models::{FileTransferRecord, PeerRecord, PersistentMessage};
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use flutter_rust_bridge::frb;

#[frb(ignore)]
#[async_trait]
pub trait StorageRepository: Send + Sync {
    // Peer 操作
    async fn upsert_peer(&self, record: PeerRecord);
    async fn set_peer_online_status(&self, mac: &str, is_online: bool);
    async fn get_all_peers(&self) -> Vec<PeerRecord>;
    async fn get_peer(&self, mac: &str) -> Option<PeerRecord>;

    // Message 操作
    async fn has_message(&self, msg_id: &str) -> bool;
    async fn append_message(&self, peer_mac: String, msg: PersistentMessage);
    async fn mark_acked(&self, peer_mac: &str, target_msg_id: &str) -> bool;
    async fn get_messages_for_peer(&self, peer_mac: &str) -> Vec<PersistentMessage>;
    async fn get_pending_messages(&self, peer_mac: &str) -> Vec<PersistentMessage>;

    // FileTransfer 操作
    async fn insert_file_transfer(&self, record: FileTransferRecord);
    async fn record_chunk_received(&self, transfer_id: String, chunk_index: u32) -> Result<u32, String>;
    async fn record_outbound_chunk_ack(&self, transfer_id: String, chunk_index: u32);
    async fn get_acked_outbound_chunks(&self, transfer_id: &str) -> HashSet<u32>;
    async fn clear_file_transfer_trackers(&self, transfer_id: &str);
    async fn mark_transfer_completed(&self, transfer_id: &str);
    async fn get_pending_outbound_transfers(&self, peer_mac: &str) -> Vec<FileTransferRecord>;
    async fn get_file_transfer(&self, transfer_id: &str) -> Option<FileTransferRecord>;
}

// =========================================================================
// FRB Opaque 包装层（供 Dart 端安全的生成和调用）
// =========================================================================

pub struct StorageRepositoryHandle {
    pub(crate) inner: Arc<dyn StorageRepository>,
}

impl StorageRepositoryHandle {
    pub fn new(inner: Arc<dyn StorageRepository>) -> Self {
        Self { inner }
    }

    // Peer 操作
    pub async fn upsert_peer(&self, record: PeerRecord) {
        self.inner.upsert_peer(record).await;
    }

    pub async fn set_peer_online_status(&self, mac: String, is_online: bool) {
        self.inner.set_peer_online_status(&mac, is_online).await;
    }

    pub async fn get_all_peers(&self) -> Vec<PeerRecord> {
        self.inner.get_all_peers().await
    }

    pub async fn get_peer(&self, mac: String) -> Option<PeerRecord> {
        self.inner.get_peer(&mac).await
    }

    // Message 操作
    pub async fn has_message(&self, msg_id: String) -> bool {
        self.inner.has_message(&msg_id).await
    }

    pub async fn append_message(&self, peer_mac: String, msg: PersistentMessage) {
        self.inner.append_message(peer_mac, msg).await;
    }

    pub async fn mark_acked(&self, peer_mac: String, target_msg_id: String) -> bool {
        self.inner.mark_acked(&peer_mac, &target_msg_id).await
    }

    pub async fn get_messages_for_peer(&self, peer_mac: String) -> Vec<PersistentMessage> {
        self.inner.get_messages_for_peer(&peer_mac).await
    }

    pub async fn get_pending_messages(&self, peer_mac: String) -> Vec<PersistentMessage> {
        self.inner.get_pending_messages(&peer_mac).await
    }

    // FileTransfer 操作
    pub async fn insert_file_transfer(&self, record: FileTransferRecord) {
        self.inner.insert_file_transfer(record).await;
    }

    pub async fn record_chunk_received(&self, transfer_id: String, chunk_index: u32) -> Result<u32, String> {
        self.inner.record_chunk_received(transfer_id, chunk_index).await
    }

    pub async fn record_outbound_chunk_ack(&self, transfer_id: String, chunk_index: u32) {
        self.inner.record_outbound_chunk_ack(transfer_id, chunk_index).await;
    }

    pub async fn get_acked_outbound_chunks(&self, transfer_id: String) -> Vec<u32> {
        self.inner.get_acked_outbound_chunks(&transfer_id).await.into_iter().collect()
    }

    pub async fn clear_file_transfer_trackers(&self, transfer_id: String) {
        self.inner.clear_file_transfer_trackers(&transfer_id).await;
    }

    pub async fn mark_transfer_completed(&self, transfer_id: String) {
        self.inner.mark_transfer_completed(&transfer_id).await;
    }

    pub async fn get_pending_outbound_transfers(&self, peer_mac: String) -> Vec<FileTransferRecord> {
        self.inner.get_pending_outbound_transfers(&peer_mac).await
    }

    pub async fn get_file_transfer(&self, transfer_id: String) -> Option<FileTransferRecord> {
        self.inner.get_file_transfer(&transfer_id).await
    }
}

```

---

## File: rust/src/api/mod.rs

```rust
pub mod simple;
pub mod discovery;
pub mod bridge;
pub mod storage;
pub mod connection;
pub mod messaging;
pub mod file_transfer;
pub mod engine;
pub mod models;
pub mod utils;


```

---

## File: rust/src/api/simple.rs

```rust
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[flutter_rust_bridge::frb(sync)] // Synchronous mode for simplicity of the demo
pub fn greet(name: String) -> String {
    format!("Hello, {name}!")
}

#[flutter_rust_bridge::frb(init)]
pub fn init_app() {
    // Default utilities - feel free to customize
    flutter_rust_bridge::setup_default_user_utils();

    // 显式创建一个允许 DEBUG 级别输出的订阅器
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG) // 👈 关键：必须设为 DEBUG 或 TRACE
        .finish();

    // 设置为全局默认订阅器
    let _ = tracing::subscriber::set_global_default(subscriber);
}

```

---

## File: rust/src/api/messaging/service.rs

```rust
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

```

---

## File: rust/src/api/messaging/mod.rs

```rust
pub mod traits;
pub mod service;
```

---

## File: rust/src/api/messaging/traits.rs

```rust
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

```

---

## File: rust/src/api/engine.rs

```rust
use crate::api::connection::manager::{PeerConnection, TcpConnectionManager};
use crate::api::connection::traits::ConnectionManager;
use crate::api::file_transfer::service::{DefaultFileTransferService, DefaultFileTransferServiceHandle};
use crate::api::file_transfer::traits::{FileTransferService, FileTransferServiceHandle};
use crate::api::messaging::service::{DefaultMessagingService, DefaultMessagingServiceHandle};
use crate::api::messaging::traits::{MessagingService, MessagingServiceHandle};
use crate::api::models::{
    FileType, FileTransferRecord, MessageEnvelope, MessagePayload, MessageStatus, PeerRecord,
    PersistentMessage,
};
use crate::api::storage::sqlite::{LocalStorage, LocalStorageHandle};
use crate::api::storage::traits::{StorageRepository, StorageRepositoryHandle};
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
    pub async fn new(self_mac: String, db_path_str: String) -> Result<Self, String> {
        let db_path = PathBuf::from(db_path_str);
        info!(self_mac = %self_mac, db_path = ?db_path, "Initializing Modular NetworkEngine");

        // 1. 初始化 Storage
        let storage = Arc::new(LocalStorage::open(db_path).await?);

        // 2. 初始化 Notification Event Sink
        let notify_sink = Arc::new(RwLock::new(None::<StreamSink<String>>));

        let notify_sink_clone = Arc::clone(&notify_sink);
        let event_emitter = Arc::new(move |event: String| {
            let sink_guard = notify_sink_clone.clone();
            tokio::spawn(async move {
                if let Some(ref sink) = *sink_guard.read().await {
                    let _ = sink.add(event);
                }
            });
        });

        // 3. 初始化 Connection Manager
        let connection_manager = Arc::new(TcpConnectionManager::new(
            self_mac.clone(),
            Arc::clone(&storage) as Arc<dyn StorageRepository>,
        ));

        // 4. 初始化 Messaging Service
        let messaging_service = Arc::new(DefaultMessagingService::new(
            self_mac.clone(),
            Arc::clone(&storage) as Arc<dyn StorageRepository>,
            Arc::clone(&connection_manager) as Arc<dyn ConnectionManager>,
            event_emitter.clone(),
        ));

        // 5. 初始化 File Transfer Service
        let file_transfer_service = Arc::new(DefaultFileTransferService::new(
            self_mac.clone(),
            Arc::clone(&storage) as Arc<dyn StorageRepository>,
            Arc::clone(&connection_manager) as Arc<dyn ConnectionManager>,
            event_emitter.clone(),
        ));

        // 6. 构造 Arc<NetworkEngine> 结构体
        let engine_arc = Arc::new(Self {
            self_mac,
            storage,
            connection_manager: connection_manager.clone(),
            messaging_service,
            file_transfer_service,
            notify_sink,
        });

        // 7. 同步设置 ConnectionManager 中的 engine 弱引用（避免异步延迟导致的竞态）
        connection_manager.set_engine(Arc::downgrade(&engine_arc)).await;

        Ok((*engine_arc).clone())
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

    pub async fn send_file(&self, recipient_mac: String, file_path_str: String) -> Result<String, String> {
        self.file_transfer_service.send_file(recipient_mac, file_path_str).await
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

            match env.payload {
                MessagePayload::Handshake { sender_mac } => {
                    current_peer_mac = sender_mac.clone();
                    *conn.remote_mac.write().await = sender_mac.clone();
                    self.connection_manager.insert_connection(sender_mac.clone(), Arc::clone(&conn)).await;
                    self.storage.set_peer_online_status(&sender_mac, true).await;
                    self.emit_event("PEER_LIST_UPDATED".to_string()).await;
                }
                MessagePayload::ChatMessage { content } => {
                    if current_peer_mac.is_empty() {
                        current_peer_mac = env.sender_mac.clone();
                        *conn.remote_mac.write().await = env.sender_mac.clone();
                        self.connection_manager.insert_connection(env.sender_mac.clone(), Arc::clone(&conn)).await;
                        self.storage.set_peer_online_status(&env.sender_mac, true).await;
                        self.emit_event("PEER_LIST_UPDATED".to_string()).await;
                    }

                    if !self.storage.has_message(&env.msg_id).await {
                        let msg = PersistentMessage {
                            msg_id: env.msg_id.clone(),
                            peer_mac: env.sender_mac.clone(),
                            is_outgoing: false,
                            content,
                            timestamp: chrono_now_timestamp(),
                            status: MessageStatus::Acked,
                            file_type: FileType::None,
                            file_name: None,
                            file_size: None,
                            file_hash: None,
                            file_path: None,
                        };

                        self.storage.append_message(env.sender_mac.clone(), msg).await;
                        self.emit_event(format!("NEW_MSG:{}", env.sender_mac)).await;
                    }

                    let ack = MessageEnvelope {
                        version: 1,
                        msg_id: Uuid::new_v4().to_string(),
                        sender_mac: self.self_mac.clone(),
                        payload: MessagePayload::Ack {
                            target_msg_id: env.msg_id,
                        },
                    };

                    let _ = conn.send_envelope(&ack).await;
                }
                MessagePayload::FileTransferInit {
                    transfer_id,
                    file_type,
                    file_name,
                    file_size,
                    total_chunks,
                    file_hash,
                } => {
                    let peer_mac = if !current_peer_mac.is_empty() {
                        current_peer_mac.clone()
                    } else {
                        env.sender_mac.clone()
                    };

                    let download_dir = std::env::temp_dir().join("p2p_downloads");
                    let _ = tokio::fs::create_dir_all(&download_dir).await;
                    let target_path = download_dir.join(&file_name).to_string_lossy().to_string();

                    let record = FileTransferRecord {
                        transfer_id: transfer_id.clone(),
                        peer_mac: peer_mac.clone(),
                        file_type,
                        file_name,
                        file_size,
                        total_chunks,
                        received_chunks: 0,
                        file_hash,
                        save_path: target_path,
                        is_completed: false,
                        is_outgoing: false,
                    };

                    self.storage.insert_file_transfer(record).await;

                    let ack = MessageEnvelope {
                        version: 1,
                        msg_id: Uuid::new_v4().to_string(),
                        sender_mac: self.self_mac.clone(),
                        payload: MessagePayload::Ack {
                            target_msg_id: transfer_id,
                        },
                    };
                    let _ = conn.send_envelope(&ack).await;
                }
                MessagePayload::FileChunk { header, data } => {
                    let transfer = self.storage.get_file_transfer(&header.transfer_id).await;

                    if let Some(record) = transfer {
                        let path = record.save_path.clone();
                        let offset = header.offset;
                        let chunk_data = data;

                        let write_res = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                            use std::fs::OpenOptions;
                            use std::io::{Seek, SeekFrom, Write};

                            let mut file = OpenOptions::new()
                                .create(true)
                                .write(true)
                                .open(&path)?;

                            file.seek(SeekFrom::Start(offset))?;
                            file.write_all(&chunk_data)?;
                            Ok(())
                        })
                            .await;

                        if let Ok(Ok(())) = write_res {
                            if let Ok(recv_count) = self
                                .storage
                                .record_chunk_received(header.transfer_id.clone(), header.chunk_index)
                                .await
                            {
                                let chunk_ack = MessageEnvelope {
                                    version: 1,
                                    msg_id: Uuid::new_v4().to_string(),
                                    sender_mac: self.self_mac.clone(),
                                    payload: MessagePayload::ChunkAck {
                                        transfer_id: header.transfer_id.clone(),
                                        chunk_index: header.chunk_index,
                                    },
                                };
                                let _ = conn.send_envelope(&chunk_ack).await;

                                if recv_count >= record.total_chunks {
                                    let msg = PersistentMessage {
                                        msg_id: header.transfer_id.clone(),
                                        peer_mac: record.peer_mac.clone(),
                                        is_outgoing: false,
                                        content: format!("[File: {}]", record.file_name),
                                        timestamp: chrono_now_timestamp(),
                                        status: MessageStatus::Acked,
                                        file_type: record.file_type,
                                        file_name: Some(record.file_name),
                                        file_size: Some(record.file_size),
                                        file_hash: Some(record.file_hash),
                                        file_path: Some(record.save_path),
                                    };
                                    self.storage.append_message(record.peer_mac.clone(), msg).await;

                                    let complete_ack = MessageEnvelope {
                                        version: 1,
                                        msg_id: Uuid::new_v4().to_string(),
                                        sender_mac: self.self_mac.clone(),
                                        payload: MessagePayload::FileCompleteAck {
                                            transfer_id: header.transfer_id.clone(),
                                        },
                                    };
                                    let _ = conn.send_envelope(&complete_ack).await;

                                    self.emit_event(format!("FILE_COMPLETE:{}", header.transfer_id)).await;
                                    self.emit_event(format!("NEW_MSG:{}", record.peer_mac)).await;
                                } else {
                                    self.emit_event(format!(
                                        "FILE_PROGRESS:{}:{}",
                                        header.transfer_id,
                                        (recv_count as f32 / record.total_chunks as f32 * 100.0) as u32
                                    ))
                                        .await;
                                }
                            }
                        }
                    }
                }
                MessagePayload::ChunkAck { transfer_id, chunk_index } => {
                    self.storage.record_outbound_chunk_ack(transfer_id, chunk_index).await;
                }
                MessagePayload::FileCompleteAck { transfer_id } => {
                    let target = if !current_peer_mac.is_empty() {
                        &current_peer_mac
                    } else {
                        &env.sender_mac
                    };

                    self.storage.mark_transfer_completed(&transfer_id).await;
                    self.storage.clear_file_transfer_trackers(&transfer_id).await;

                    if self.storage.mark_acked(target, &transfer_id).await {
                        self.emit_event(format!("ACK:{}", target)).await;
                    }
                    self.emit_event(format!("FILE_SENT:{}", transfer_id)).await;
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
    pub async fn new(self_mac: String, db_path_str: String) -> Result<Self, String> {
        let engine = NetworkEngine::new(self_mac, db_path_str).await?;
        Ok(Self {
            inner: Arc::new(engine),
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

    pub async fn send_file(&self, recipient_mac: String, file_path_str: String) -> Result<String, String> {
        self.inner.send_file(recipient_mac, file_path_str).await
    }

    pub async fn run_scan_and_flush_cycle(&self) {
        self.inner.run_scan_and_flush_cycle().await;
    }

    pub async fn start_listener(&self, port: u16, notify_sink: StreamSink<String>) -> Result<(), String> {
        self.inner.start_listener(port, notify_sink).await
    }

    pub fn get_storage(&self) -> StorageRepositoryHandle {
        StorageRepositoryHandle {
            inner: Arc::clone(&self.inner.storage),
        }
    }

    pub fn get_messaging_service(&self) -> MessagingServiceHandle {
        MessagingServiceHandle {
            inner: Arc::clone(&self.inner.messaging_service),
        }
    }

    pub fn get_file_transfer_service(&self) -> FileTransferServiceHandle {
        FileTransferServiceHandle {
            inner: Arc::clone(&self.inner.file_transfer_service),
        }
    }

    pub async fn flush_outbox(&self, peer_mac: String) {
        self.inner.flush_outbox(peer_mac).await;
    }
}

```

---

