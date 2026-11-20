use anyhow::Result;
use parking_lot::RwLock;
use sabnzbd_api::{create_router, AppState};
use sabnzbd_core::{Config, JobStatus, QueueItem, ServerConfig};
use sabnzbd_decode::{FileAssembler, YencDecoder};
use sabnzbd_nntp::{NntpError, NntpPool};
use sabnzbd_nzb::{DownloadQueue, NzbDocument};
use sabnzbd_postproc::PostProcessor;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rustnzbd=debug,sabnzbd_api=debug,sabnzbd_nntp=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config_path = std::env::var("RUSTNZBD_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("rustnzbd.toml"));

    let state_path = std::env::var("RUSTNZBD_STATE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("rustnzbd_state.json"));

    let loaded_cfg = Config::load_or_create(&config_path)?;
    let port = loaded_cfg.http_port;
    let api_key = loaded_cfg.api_key.clone();
    let host_ip: std::net::IpAddr = loaded_cfg.http_host.parse().unwrap_or([0, 0, 0, 0].into());

    let config = Arc::new(RwLock::new(loaded_cfg));
    let queue = Arc::new(DownloadQueue::new_persistent(state_path));

    // Spawn background download & post-processing worker
    tokio::spawn(download_worker_loop(config.clone(), queue.clone()));

    let state = AppState {
        config: config.clone(),
        queue: queue.clone(),
        config_path: Some(config_path.clone()),
    };

    let app = create_router(state);
    let addr = SocketAddr::from((host_ip, port));

    info!("------------------------------------------------------------");
    info!(" RustNZBD (SABnzbd in Rust)");
    info!(" Config: {:?}", config_path);
    info!(" Web & API running at: http://127.0.0.1:{}", port);
    info!(" API Key: {}", api_key);
    info!(" SABnzbd API URL: http://127.0.0.1:{}/api?apikey={}", port, api_key);
    info!("------------------------------------------------------------");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn download_worker_loop(config: Arc<RwLock<Config>>, queue: Arc<DownloadQueue>) {
    loop {
        if let Some((job, doc)) = queue.claim_next_job() {
            let cfg_snapshot = config.read().clone();
            if let Err(err) = process_job(&config, &cfg_snapshot, &queue, &job, doc).await {
                error!("Job {} failed: {}", job.id, err);
                let job_dir = cfg_snapshot.download_dir.join(&job.filename);
                queue.finish_job(&job.id, job_dir, Some(err));
            }
        } else {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

async fn process_job(
    live_config: &Arc<RwLock<Config>>,
    cfg: &Config,
    queue: &DownloadQueue,
    job: &QueueItem,
    doc: NzbDocument,
) -> std::result::Result<(), String> {
    let mut servers: Vec<ServerConfig> = cfg
        .servers
        .iter()
        .filter(|s| s.enable)
        .cloned()
        .collect();
    servers.sort_by_key(|s| s.priority);

    if servers.is_empty() {
        return Err("No enabled NNTP servers configured".to_string());
    }

    let pools: Arc<Vec<Arc<NntpPool>>> = Arc::new(
        servers
            .into_iter()
            .map(|s| Arc::new(NntpPool::new(s)))
            .collect(),
    );

    let safe_job_name = FileAssembler::safe_filename(&job.filename, &job.id);
    let work_dir = cfg.download_dir.join(&safe_job_name);
    tokio::fs::create_dir_all(&work_dir)
        .await
        .map_err(|e| e.to_string())?;

    let mut tasks = JoinSet::new();
    let started = Instant::now();

    for (file_idx, file) in doc.files.into_iter().enumerate() {
        let fallback_name = format!("file_{file_idx}.bin");
        for seg in file.segments {
            let pools = pools.clone();
            let dir = work_dir.clone();
            let fallback = fallback_name.clone();

            tasks.spawn(async move {
                let raw = fetch_from_pools(&pools, &seg.message_id).await?;
                let decoded = YencDecoder::decode(&raw).map_err(|e| e.to_string())?;
                let len = decoded.data.len() as u64;
                FileAssembler::write_article(&dir, &fallback, &decoded)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok::<u64, String>(len)
            });
        }
    }

    let mut downloaded_total = 0u64;
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(bytes)) => {
                downloaded_total += bytes;
                let elapsed = started.elapsed().as_secs_f64().max(0.001);
                let speed_limit = live_config.read().speed_limit;
                if let Some(limit_bps) = speed_limit {
                    if limit_bps > 0 {
                        let target_secs = downloaded_total as f64 / limit_bps as f64;
                        if target_secs > elapsed {
                            tokio::time::sleep(Duration::from_secs_f64(target_secs - elapsed)).await;
                        }
                    }
                }
                let actual_elapsed = started.elapsed().as_secs_f64().max(0.001);
                let speed_bps = (downloaded_total as f64 / actual_elapsed) as u64;
                queue.add_downloaded_bytes(&job.id, bytes, speed_bps);
            }
            Ok(Err(e)) => warn!("Segment error in job {}: {}", job.id, e),
            Err(e) => warn!("Worker panic in job {}: {}", job.id, e),
        }
    }

    queue.set_status(&job.id, JobStatus::Checking);
    if let Err(e) = PostProcessor::verify_and_repair(&work_dir).await {
        warn!("PAR2 check/repair issue for {}: {}", job.id, e);
    }

    queue.set_status(&job.id, JobStatus::Extracting);
    if let Ok(mut entries) = tokio::fs::read_dir(&work_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
                if matches!(ext.to_ascii_lowercase().as_str(), "rar" | "zip" | "7z") {
                    if let Err(e) = PostProcessor::extract_archive(&path, &work_dir).await {
                        warn!("Archive extraction issue for {:?}: {}", path, e);
                    }
                }
            }
        }
    }

    let final_dir = cfg.complete_dir.join(&safe_job_name);
    move_to_complete(&work_dir, &final_dir)
        .await
        .map_err(|e| e.to_string())?;

    info!("Job {} completed at {:?}", job.id, final_dir);
    queue.finish_job(&job.id, final_dir, None);
    Ok(())
}

async fn fetch_from_pools(pools: &[Arc<NntpPool>], message_id: &str) -> std::result::Result<bytes::Bytes, String> {
    let mut last_err = String::from("No NNTP pools available");
    for pool in pools {
        match pool.fetch_body(message_id).await {
            Ok(bytes) => return Ok(bytes),
            Err(NntpError::ArticleNotFound(_)) => {
                last_err = format!("Article {} not found", message_id);
                continue;
            }
            Err(e) => {
                last_err = e.to_string();
                continue;
            }
        }
    }
    Err(last_err)
}

async fn move_to_complete(src: &Path, dst: &Path) -> std::io::Result<()> {
    if let Some(parent) = dst.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if tokio::fs::rename(src, dst).await.is_err() {
        // Fallback across filesystems: copy files then remove src
        tokio::fs::create_dir_all(dst).await?;
        let mut entries = tokio::fs::read_dir(src).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                tokio::fs::copy(entry.path(), dst.join(entry.file_name())).await?;
            }
        }
        tokio::fs::remove_dir_all(src).await?;
    }
    Ok(())
}
