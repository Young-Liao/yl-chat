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
