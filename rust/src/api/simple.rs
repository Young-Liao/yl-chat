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
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");
}
