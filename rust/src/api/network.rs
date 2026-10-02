use mdns_sd::{ServiceDaemon, ServiceInfo, ServiceEvent};
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::Duration;

const SERVICE_PORT: u16 = 14981;
const SERVICE_TYPE: &str = "_ylchat._tcp.local.";

static MDNS_DAEMON: OnceLock<ServiceDaemon> = OnceLock::new();

fn get_or_init_daemon() -> Result<&'static ServiceDaemon, String> {
    MDNS_DAEMON
        .get_or_init(|| ServiceDaemon::new().expect("Failed to initialize mDNS daemon"));
    Ok(MDNS_DAEMON.get().unwrap())
}

/// Starts advertising this device's node on the local network via Bonjour/mDNS
pub fn register_bonjour_service() -> Result<(), String> {
    let mdns = get_or_init_daemon()?;

    let dynamic_name = format!("yl_chat_{}", get_device_identifier());
    let host_name = format!("{}.local.", dynamic_name);

    let service_info = ServiceInfo::new(
        SERVICE_TYPE,
        &dynamic_name,
        &host_name,
        "", // Auto-resolves local IP
        SERVICE_PORT,
        HashMap::new(),
    )
        .map_err(|e| format!("Failed to create service info: {e}"))?;

    mdns.register(service_info)
        .map_err(|e| format!("Failed to register mDNS service: {e}"))?;

    Ok(())
}

/// Scans the local network for peers and returns their resolved IP addresses
pub async fn scan_lan_peers() -> Result<Vec<String>, String> {
    let mdns = get_or_init_daemon()?;
    let receiver = mdns
        .browse(SERVICE_TYPE)
        .map_err(|e| format!("Failed to browse mDNS: {e}"))?;

    let mut peer_ips = HashSet::new();
    let timeout = Duration::from_secs(4);
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        if let Ok(event) = receiver.recv_timeout(Duration::from_millis(100)) {
            if let ServiceEvent::ServiceResolved(info) = event {
                for ip in info.get_addresses() {
                    let ip_str = ip.to_string();
                    // Ignore local loopback IP
                    if !ip_str.starts_with("127.") {
                        peer_ips.insert(ip_str);
                    }
                }
            }
        }
    }

    Ok(peer_ips.into_iter().collect())
}

fn get_device_identifier() -> String {
    gethostname::gethostname()
        .to_string_lossy()
        .replace('.', "_")
}
