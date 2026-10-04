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

impl FileTransferRecord {
    /// 获取当前传输进度的浮点值 (范围: 0.0 - 1.0)
    pub fn progress_ratio(&self) -> f64 {
        if self.total_chunks == 0 {
            return 1.0;
        }
        (self.received_chunks as f64 / self.total_chunks as f64).min(1.0)
    }

    /// 获取当前传输百分比 (范围: 0 - 100)
    pub fn progress_percentage(&self) -> u8 {
        (self.progress_ratio() * 100.0) as u8
    }
}
