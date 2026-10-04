use super::traits::{StorageRepository, StorageRepositoryHandle};
use crate::api::models::{FileTransferRecord, FileType, MessageStatus, PeerRecord, PersistentMessage};
use async_trait::async_trait;
use rusqlite::params;
use std::collections::HashSet;
use std::path::PathBuf;
use flutter_rust_bridge::frb;
use tokio_rusqlite::Connection as AsyncConnection;
use tracing::{error, info, warn};

#[frb(ignore)]
pub struct LocalStorage {
    db: AsyncConnection,
}

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

        // 检查是否存在 sent_chunks 字段，若不存在则直接触发 rebuild_schema 删除并重建表结构
        let check_schema = db
            .call(|conn| {
                let mut stmt = conn.prepare("PRAGMA table_info(messages)")?;
                let mut has_sent_chunks = false;
                let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;

                for col in rows {
                    if let Ok(name) = col {
                        if name == "sent_chunks" {
                            has_sent_chunks = true;
                            break;
                        }
                    }
                }

                if !has_sent_chunks {
                    return Err(rusqlite::Error::ModuleError(
                        "Schema mismatch: missing chunk fields in messages".to_string(),
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
                    file_path TEXT,
                    sent_chunks INTEGER NOT NULL DEFAULT 0,
                    total_chunks INTEGER NOT NULL DEFAULT 0
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

    /// DROP 掉旧表并创建包含 chunk 进度的新表结构
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
                    file_path TEXT,
                    sent_chunks INTEGER NOT NULL DEFAULT 0,
                    total_chunks INTEGER NOT NULL DEFAULT 0
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
                    "INSERT OR REPLACE INTO messages (
                        msg_id, peer_mac, is_outgoing, content, timestamp, status,
                        file_type, file_name, file_size, file_hash, file_path,
                        sent_chunks, total_chunks
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
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
                        msg.sent_chunks,
                        msg.total_chunks,
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
                    "SELECT msg_id, peer_mac, is_outgoing, content, timestamp, status, file_type, file_name, file_size, file_hash, file_path, sent_chunks, total_chunks
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
                        sent_chunks: row.get(11)?,
                        total_chunks: row.get(12)?,
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
                    "SELECT msg_id, peer_mac, is_outgoing, content, timestamp, status, file_type, file_name, file_size, file_hash, file_path, sent_chunks, total_chunks
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
                        sent_chunks: row.get(11)?,
                        total_chunks: row.get(12)?,
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


    /// 更新数据库中指定消息的发送进度 sent_chunks
    async fn update_message_progress(&self, msg_id: &str, sent_chunks: u32) {
        let msg_id_owned = msg_id.to_string();
        let res = self
            .db
            .call(move |conn| {
                conn.execute(
                    "UPDATE messages SET sent_chunks = ?1 WHERE msg_id = ?2",
                    params![sent_chunks, msg_id_owned],
                )?;
                Ok(())
            })
            .await;

        if let Err(e) = res {
            error!(error = %e, msg_id = %msg_id, "Failed to update message progress");
        }
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

    async fn update_transfer_progress(&self, transfer_id: &str, received_chunks: u32) -> Result<(), String> {
        let tid = transfer_id.to_string();
        self.db
            .call(move |conn| {
                conn.execute(
                    "UPDATE file_transfers SET received_chunks = ?1 WHERE transfer_id = ?2",
                    params![received_chunks, tid],
                )?;
                Ok(())
            })
            .await
            .map_err(|e| format!("Failed to update transfer progress: {e}"))
    }

    async fn get_transfer_progress(&self, transfer_id: &str) -> Option<(u32, u32)> {
        let tid = transfer_id.to_string();
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT received_chunks, total_chunks FROM file_transfers WHERE transfer_id = ?1",
                )?;
                let mut rows = stmt.query(params![tid])?;
                if let Some(row) = rows.next()? {
                    let received: u32 = row.get(0)?;
                    let total: u32 = row.get(1)?;
                    Ok(Some((received, total)))
                } else {
                    Ok(None)
                }
            })
            .await
            .unwrap_or(None)
    }
}
