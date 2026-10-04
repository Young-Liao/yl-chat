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
