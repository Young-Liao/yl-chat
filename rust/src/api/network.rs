use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo, IfKind}; // 🔑 引入 IfKind
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::Duration;

const SERVICE_PORT: u16 = 14981;
const SERVICE_TYPE: &str = "_ylchat._tcp.local.";

static MDNS_DAEMON: OnceLock<ServiceDaemon> = OnceLock::new();
static REGD_SERVICE: OnceLock<ServiceInfo> = OnceLock::new();

/// 修改：初始化守护进程，并在需要时通过指定的 IP 显式启用对应网络接口
fn get_or_init_daemon() -> Result<&'static ServiceDaemon, String> {
    MDNS_DAEMON.get_or_init(|| {
        let daemon = ServiceDaemon::new().expect("Failed to initialize mDNS daemon");

        // 🔑 动态获取当前路由 IP 并显式绑定该接口
        if let Some(ip_str) = get_local_ip() {
            if let Ok(ip) = ip_str.parse::<std::net::IpAddr>() {
                // mdns_sd 的 enable_interface 支持传入 IfKind::Ip(IpAddr)
                if let Err(e) = daemon.enable_interface(IfKind::from(ip)) {
                    eprintln!("Warning: Failed to enable mDNS interface for {}: {}", ip_str, e);
                } else {
                    println!("mDNS daemon successfully explicit bound to interface IP: {}", ip_str);
                }
            }
        }

        daemon
    });
    Ok(MDNS_DAEMON.get().unwrap())
}

pub fn register_bonjour_service() -> Result<(), String> {
    let mdns = get_or_init_daemon()?;

    let device_id = get_device_identifier();
    let service_instance_name = format!("yl_chat_{}", device_id);
    let host_name = format!("{}.local.", device_id);
    let local_ip = get_local_ip().unwrap_or_else(|| "127.0.0.1".into());

    let service_info = ServiceInfo::new(
        SERVICE_TYPE,
        &service_instance_name,
        &host_name,
        &local_ip,
        SERVICE_PORT,
        HashMap::new(),
    )
        .map_err(|e| format!("Failed to create service info: {e}"))?;

    // 真正注册服务
    mdns.register(service_info.clone())
        .map_err(|e| format!("Failed to register mDNS service: {e}"))?;

    // 存入静态变量，防止生命周期结束导致服务注销
    let _ = REGD_SERVICE.set(service_info);

    println!("Succeeded in registering the service: {}, IP: {}", host_name, local_ip);
    Ok(())
}

/// Helper function to find the primary non-loopback local LAN IPv4 address
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

/// Scans the local network for peers and returns their resolved IP addresses
pub async fn scan_lan_peers() -> Result<Vec<String>, String> {
    let mdns = get_or_init_daemon()?;

    let receiver = mdns
        .browse(SERVICE_TYPE)
        .map_err(|e| format!("Failed to browse mDNS: {e}"))?;

    let my_ip = get_local_ip();
    let mut peer_ips = HashSet::new();
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        // 使用 recv_timeout 配合短延时，防止阻塞外部异步环境的循环太久
        if let Ok(event) = receiver.recv_timeout(Duration::from_millis(100)) {
            if let ServiceEvent::ServiceResolved(info) = event {
                for ip in info.get_addresses() {
                    let ip_str = ip.to_string();

                    // Filter out loopback and self-discovery
                    if !ip_str.starts_with("127.") {
                        if let Some(ref local_ip) = my_ip {
                            if &ip_str != local_ip {
                                peer_ips.insert(ip_str);
                            }
                        } else {
                            peer_ips.insert(ip_str);
                        }
                    }
                }
            }
        }
    }

    Ok(peer_ips.into_iter().collect())
}

fn get_device_identifier() -> String {
    let raw = gethostname::gethostname().to_string_lossy().to_string();
    raw.trim_end_matches(".local")
        .replace(['.', ' ', '\''], "_")
}
