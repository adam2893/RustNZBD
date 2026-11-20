use anyhow::Result;
use parking_lot::RwLock;
use sabnzbd_api::{create_router, extract_nzb_xml, AppState};
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

    // Spawn background download & watched-folder workers
    tokio::spawn(download_worker_loop(config.clone(), queue.clone()));
    tokio::spawn(watch_dir_loop(config.clone(), queue.clone()));

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
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("RustNZBD shut down cleanly");
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutdown signal received");
}

async fn watch_dir_loop(config: Arc<RwLock<Config>>, queue: Arc<DownloadQueue>) {
    loop {
        let watch_dir = config.read().watch_dir.clone();
        if let Some(dir) = watch_dir {
            if dir.exists() {
                let _ = scan_watch_dir(&dir, &queue).await;
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn scan_watch_dir(dir: &Path, queue: &DownloadQueue) -> std::io::Result<()> {
    let mut entries = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let lower = name.to_ascii_lowercase();
        if !lower.ends_with(".nzb") && !lower.ends_with(".nzb.gz") {
            continue;
        }

        if let Ok(raw) = tokio::fs::read(&path).await {
            let xml = extract_nzb_xml(&raw);
            match NzbDocument::parse(&xml) {
                Ok(doc) => {
                    let total_bytes = doc.total_bytes();
                    info!("Watch dir imported: {} ({} bytes)", name, total_bytes);
                    queue.add_nzb_with_doc(name.to_string(), total_bytes, "default".to_string(), Some(doc));
                    let imported = path.with_file_name(format!("{name}.imported"));
                    let _ = tokio::fs::rename(&path, imported).await;
                }
                Err(e) => {
                    warn!("Invalid NZB in watch dir {:?}: {}", path, e);
                }
            }
        }
    }
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

    let cat_root = cfg.category_dir(&job.category);
    let final_dir = cat_root.join(&safe_job_name);
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    fn yenc_part(name: &str, part: usize, total: usize, begin: usize, total_size: usize, data: &[u8]) -> Vec<u8> {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(data);
        let crc = hasher.finalize();

        let end = begin + data.len() - 1;
        let mut out = format!(
            "=ybegin part={part} total={total} line=128 size={total_size} name={name}\r\n=ypart begin={begin} end={end}\r\n"
        )
        .into_bytes();

        for &b in data {
            let shifted = b.wrapping_add(42);
            if matches!(shifted, 0 | b'\n' | b'\r' | b'=') {
                out.push(b'=');
                out.push(shifted.wrapping_add(64));
            } else {
                out.push(shifted);
            }
        }
        out.extend_from_slice(format!("\r\n=yend size={} part={part} pcrc32={crc:08x}\r\n", data.len()).as_bytes());
        out
    }

    #[tokio::test]
    async fn test_e2e_api_to_nntp_to_complete_dir() {
        let payload = b"RustNZBD end-to-end verification payload!";
        let part1 = yenc_part("payload.bin", 1, 2, 1, payload.len(), &payload[..20]);
        let part2 = yenc_part("payload.bin", 2, 2, 21, payload.len(), &payload[20..]);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nntp_port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let p1 = part1.clone();
                let p2 = part2.clone();
                tokio::spawn(async move {
                    let (r, mut w) = socket.into_split();
                    let mut reader = BufReader::new(r);
                    let _ = w.write_all(b"200 MockNNTP\r\n").await;
                    let mut line = String::new();
                    while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                        let cmd = line.trim();
                        if cmd == "BODY <p1@test>" {
                            let _ = w.write_all(b"222 0 <p1@test> body\r\n").await;
                            let _ = w.write_all(&p1).await;
                            let _ = w.write_all(b".\r\n").await;
                        } else if cmd == "BODY <p2@test>" {
                            let _ = w.write_all(b"222 0 <p2@test> body\r\n").await;
                            let _ = w.write_all(&p2).await;
                            let _ = w.write_all(b".\r\n").await;
                        } else if cmd == "QUIT" {
                            let _ = w.write_all(b"205 Bye\r\n").await;
                            break;
                        }
                        line.clear();
                    }
                });
            }
        });

        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.download_dir = tmp.path().join("incomplete");
        cfg.complete_dir = tmp.path().join("complete");
        cfg.categories.clear();
        cfg.servers.push(ServerConfig {
            name: "LocalMock".to_string(),
            host: "127.0.0.1".to_string(),
            port: nntp_port,
            ssl: false,
            connections: 2,
            enable: true,
            ..Default::default()
        });

        let api_key = cfg.api_key.clone();
        let shared_cfg = Arc::new(RwLock::new(cfg.clone()));
        let queue = Arc::new(DownloadQueue::new());

        let app = create_router(AppState {
            config: shared_cfg.clone(),
            queue: queue.clone(),
            config_path: None,
        });

        let nzb = r#"<nzb>
            <file poster="p" date="1" subject="payload.bin">
                <groups><group>alt.test</group></groups>
                <segments>
                    <segment bytes="20" number="1">p1@test</segment>
                    <segment bytes="21" number="2">p2@test</segment>
                </segments>
            </file>
        </nzb>"#;

        let resp = app
            .oneshot(
                Request::post(format!("/api?mode=addfile&nzbname=e2e_job&cat=tv&apikey={api_key}"))
                    .body(Body::from(nzb))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let (job, doc) = queue.claim_next_job().expect("Job should be queued");
        process_job(&shared_cfg, &cfg, &queue, &job, doc).await.unwrap();

        assert!(queue.list().is_empty());
        let hist = queue.history();
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].status, "Completed");

        let assembled_path = cfg.complete_dir.join("tv").join("e2e_job").join("payload.bin");
        let assembled = tokio::fs::read(&assembled_path).await.unwrap();
        assert_eq!(assembled, payload);
    }
}
