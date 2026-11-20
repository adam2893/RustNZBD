use anyhow::Result;
use parking_lot::RwLock;
use sabnzbd_api::{create_router, AppState};
use sabnzbd_core::Config;
use sabnzbd_nzb::DownloadQueue;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rustnzbd=debug,sabnzbd_api=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Arc::new(RwLock::new(Config::default()));
    let queue = Arc::new(DownloadQueue::new());

    let cfg_read = config.read();
    let port = cfg_read.http_port;
    let api_key = cfg_read.api_key.clone();
    drop(cfg_read);

    let state = AppState {
        config: config.clone(),
        queue: queue.clone(),
    };

    let app = create_router(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    info!("------------------------------------------------------------");
    info!(" RustNZBD (SABnzbd in Rust)");
    info!(" Web & API running at: http://127.0.0.1:{}", port);
    info!(" API Key: {}", api_key);
    info!(" SABnzbd API URL: http://127.0.0.1:{}/api?apikey={}", port, api_key);
    info!("------------------------------------------------------------");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
