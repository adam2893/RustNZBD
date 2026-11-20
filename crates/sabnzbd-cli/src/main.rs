use anyhow::Result;
use parking_lot::RwLock;
use sabnzbd_api::{create_router, extract_nzb_xml, AppState};
use sabnzbd_core::{Config, JobStatus, QueueItem, ScheduleAction, ServerConfig, StatsTracker};
use sabnzbd_decode::{ArticleDecoder, FileAssembler};
use sabnzbd_nntp::{NntpError, NntpPool};
use sabnzbd_nzb::{DownloadQueue, NzbDocument};
use sabnzbd_postproc::{PostProcessor, Sorter};
use sabnzbd_services::{Notifier, RssPoller, Scheduler};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn main() -> Result<()> {
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

    let args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|a| a == "--import-ini") {
        let ini_path = args.get(pos + 1).expect("Usage: rustnzbd --import-ini <sabnzbd.ini>");
        let ini_text = std::fs::read_to_string(ini_path)?;
        let imported = Config::import_sabnzbd_ini(&ini_text);
        imported.save(&config_path)?;
        info!("Imported {} into {:?}", ini_path, config_path);
        return Ok(());
    }

    let state_path = std::env::var("RUSTNZBD_STATE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("rustnzbd_state.json"));

    let loaded_cfg = Config::load_or_import(&config_path)?;
    let queue = Arc::new(DownloadQueue::new_persistent(state_path));
    let shutdown_notify = Arc::new(Notify::new());

    #[cfg(feature = "tray")]
    if loaded_cfg.enable_tray || args.iter().any(|a| a == "--tray") {
        let port = loaded_cfg.http_port;
        let q_bg = queue.clone();
        let sd_bg = shutdown_notify.clone();
        let cfg_path_bg = config_path.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
            if let Err(e) = rt.block_on(run_server(loaded_cfg, cfg_path_bg, q_bg, sd_bg)) {
                error!("Server error: {}", e);
            }
        });
        run_tray_loop(port, queue, shutdown_notify)?;
        return Ok(());
    }

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_server(loaded_cfg, config_path, queue, shutdown_notify))?;
    Ok(())
}

#[cfg(feature = "tray")]
fn run_tray_loop(
    port: u16,
    queue: Arc<DownloadQueue>,
    shutdown_notify: Arc<Notify>,
) -> Result<()> {
    use tao::event_loop::{ControlFlow, EventLoopBuilder};
    use tray_icon::menu::{Menu, MenuEvent, MenuItem};
    use tray_icon::{Icon, TrayIconBuilder};

    let event_loop = EventLoopBuilder::new().build();
    let menu = Menu::new();
    let open_item = MenuItem::new("Open Web UI", true, None);
    let pause_item = MenuItem::new("Pause Queue", true, None);
    let resume_item = MenuItem::new("Resume Queue", true, None);
    let quit_item = MenuItem::new("Quit RustNZBD", true, None);
    menu.append(&open_item)?;
    menu.append(&pause_item)?;
    menu.append(&resume_item)?;
    menu.append(&quit_item)?;

    // 16x16 RGBA cyan/slate icon
    let mut rgba = Vec::with_capacity(16 * 16 * 4);
    for y in 0..16 {
        for x in 0..16 {
            let border = x == 0 || x == 15 || y == 0 || y == 15;
            if border {
                rgba.extend_from_slice(&[15, 23, 42, 255]);
            } else {
                rgba.extend_from_slice(&[56, 189, 248, 255]);
            }
        }
    }
    let icon = Icon::from_rgba(rgba, 16, 16)?;
    let _tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip("RustNZBD")
        .with_icon(icon)
        .build()?;

    let open_id = open_item.id().clone();
    let pause_id = pause_item.id().clone();
    let resume_id = resume_item.id().clone();
    let quit_id = quit_item.id().clone();
    let url = format!("http://127.0.0.1:{port}/");

    event_loop.run(move |_event, _, control_flow| {
        *control_flow = ControlFlow::WaitUntil(
            std::time::Instant::now() + Duration::from_millis(200),
        );
        while let Ok(ev) = MenuEvent::receiver().try_recv() {
            if ev.id == open_id {
                open_browser(&url);
            } else if ev.id == pause_id {
                queue.pause();
            } else if ev.id == resume_id {
                queue.resume();
            } else if ev.id == quit_id {
                shutdown_notify.notify_one();
                *control_flow = ControlFlow::Exit;
            }
        }
    });
}

#[cfg(feature = "tray")]
fn open_browser(url: &str) {
    #[cfg(target_os = "macos")]
    let _ = std::process::Command::new("open").arg(url).spawn();
    #[cfg(target_os = "linux")]
    let _ = std::process::Command::new("xdg-open").arg(url).spawn();
    #[cfg(target_os = "windows")]
    let _ = std::process::Command::new("cmd").args(["/c", "start", url]).spawn();
}

async fn run_server(
    loaded_cfg: Config,
    config_path: PathBuf,
    queue: Arc<DownloadQueue>,
    shutdown_notify: Arc<Notify>,
) -> Result<()> {
    let port = loaded_cfg.http_port;
    let api_key = loaded_cfg.api_key.clone();
    let host_ip: std::net::IpAddr = loaded_cfg.http_host.parse().unwrap_or([0, 0, 0, 0].into());
    let https_cfg = match (loaded_cfg.https_port, &loaded_cfg.https_cert, &loaded_cfg.https_key) {
        (Some(p), Some(c), Some(k)) => Some((p, c.clone(), k.clone())),
        _ => None,
    };

    let config = Arc::new(RwLock::new(loaded_cfg));
    let stats = StatsTracker::new();

    // Spawn background download, watched-folder, and scheduler/RSS workers
    tokio::spawn(download_worker_loop(config.clone(), queue.clone(), stats.clone()));
    tokio::spawn(watch_dir_loop(config.clone(), queue.clone()));
    tokio::spawn(scheduler_and_rss_loop(config.clone(), queue.clone()));

    let state = AppState {
        config: config.clone(),
        queue: queue.clone(),
        stats,
        config_path: Some(config_path.clone()),
        shutdown: Some(shutdown_notify.clone()),
    };

    let app = create_router(state);
    let addr = SocketAddr::from((host_ip, port));

    if let Some((tls_port, cert_path, key_path)) = https_cfg {
        let tls_app = app.clone();
        let tls_addr = SocketAddr::from((host_ip, tls_port));
        tokio::spawn(async move {
            if let Err(e) = serve_https(tls_addr, &cert_path, &key_path, tls_app).await {
                error!("HTTPS listener error: {}", e);
            }
        });
        info!(" HTTPS Web & API running at: https://127.0.0.1:{}", tls_port);
    }

    info!("------------------------------------------------------------");
    info!(" RustNZBD (SABnzbd in Rust)");
    info!(" Config: {:?}", config_path);
    info!(" Web & API running at: http://127.0.0.1:{}", port);
    info!(" API Key: {}", api_key);
    info!(" SABnzbd API URL: http://127.0.0.1:{}/api?apikey={}", port, api_key);
    info!("------------------------------------------------------------");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_notify))
        .await?;

    info!("RustNZBD shut down cleanly");
    Ok(())
}

async fn shutdown_signal(notify: Arc<Notify>) {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = notify.notified() => {}
    }
    info!("Shutdown signal received");
}

async fn serve_https(
    addr: SocketAddr,
    cert_path: &Path,
    key_path: &Path,
    app: axum::Router,
) -> Result<()> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(key_path)?;
    let tls_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls_cfg));
    let listener = tokio::net::TcpListener::bind(addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let service = hyper_util::service::TowerToHyperService::new(app.clone());
        tokio::spawn(async move {
            if let Ok(tls_stream) = acceptor.accept(stream).await {
                let io = hyper_util::rt::TokioIo::new(tls_stream);
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await;
            }
        });
    }
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

async fn scheduler_and_rss_loop(config: Arc<RwLock<Config>>, queue: Arc<DownloadQueue>) {
    let mut scheduler = Scheduler::new();
    let mut poller = RssPoller::new();
    let mut last_poll: HashMap<String, Instant> = HashMap::new();

    loop {
        let (schedules, feeds) = {
            let cfg = config.read();
            (cfg.schedules.clone(), cfg.rss_feeds.clone())
        };

        let mut force_rss = false;
        for action in scheduler.due_actions(&schedules, chrono::Local::now()) {
            match action {
                ScheduleAction::Pause => queue.pause(),
                ScheduleAction::Resume => queue.resume(),
                ScheduleAction::SpeedLimit(limit) => config.write().speed_limit = limit,
                ScheduleAction::ScanRss => force_rss = true,
            }
        }

        let detect_dups = config.read().detect_duplicates;
        for feed in &feeds {
            if !feed.enabled {
                continue;
            }
            let due = force_rss
                || last_poll
                    .get(&feed.name)
                    .map(|t| t.elapsed() >= Duration::from_secs(feed.interval_minutes.max(1) * 60))
                    .unwrap_or(true);
            if !due {
                continue;
            }
            last_poll.insert(feed.name.clone(), Instant::now());
            for item in poller.poll_feed(feed).await {
                if let Ok(client) = reqwest::Client::builder().timeout(Duration::from_secs(30)).build() {
                    if let Ok(resp) = client.get(&item.link).send().await {
                        if let Ok(bytes) = resp.bytes().await {
                            let xml = extract_nzb_xml(&bytes);
                            if let Ok(doc) = NzbDocument::parse(&xml) {
                                let total = doc.total_bytes();
                                let _ = queue.add_nzb_checked(
                                    item.title,
                                    total,
                                    feed.category.clone(),
                                    Some(doc),
                                    detect_dups,
                                );
                            }
                        }
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(15)).await;
    }
}

async fn download_worker_loop(
    config: Arc<RwLock<Config>>,
    queue: Arc<DownloadQueue>,
    stats: Arc<StatsTracker>,
) {
    loop {
        if let Some((job, doc)) = queue.claim_next_job() {
            let cfg_snapshot = config.read().clone();
            if let Err(err) = process_job(&config, &cfg_snapshot, &queue, &stats, &job, doc).await {
                error!("Job {} failed: {}", job.id, err);
                let job_dir = cfg_snapshot.download_dir.join(&job.filename);
                queue.finish_job(&job.id, job_dir, Some(err.clone()));
                Notifier::notify(
                    &cfg_snapshot.notifications,
                    false,
                    &job.filename,
                    &job.category,
                    Some(&err),
                )
                .await;
            }
        } else {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

fn is_archive_subject(subject: &str) -> bool {
    let lower = subject.to_ascii_lowercase();
    lower.contains(".rar") || lower.contains(".zip") || lower.contains(".7z")
}

async fn process_job(
    live_config: &Arc<RwLock<Config>>,
    cfg: &Config,
    queue: &DownloadQueue,
    stats: &StatsTracker,
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

    let mut passwords = Vec::new();
    if let Some(p) = &job.password {
        passwords.push(p.clone());
    }
    passwords.extend(cfg.passwords.iter().cloned());

    let archive_seg_count: usize = doc
        .files
        .iter()
        .filter(|f| is_archive_subject(&f.subject))
        .map(|f| f.segments.len())
        .sum();
    let archive_remaining = Arc::new(AtomicUsize::new(archive_seg_count));
    let mut direct_unpack_handle: Option<tokio::task::JoinHandle<bool>> = None;

    let mut tasks = JoinSet::new();
    let started = Instant::now();
    let stats_arc = Arc::new(stats);
    let _ = stats_arc;

    for (file_idx, file) in doc.files.into_iter().enumerate() {
        let fallback_name = format!("file_{file_idx}.bin");
        let is_archive = is_archive_subject(&file.subject);
        for seg in file.segments {
            let pools = pools.clone();
            let dir = work_dir.clone();
            let fallback = fallback_name.clone();

            tasks.spawn(async move {
                let (server_name, raw) = fetch_from_pools(&pools, &seg.message_id).await?;
                let decoded = ArticleDecoder::decode(&raw).map_err(|e| e.to_string())?;
                let len = decoded.data.len() as u64;
                FileAssembler::write_article(&dir, &fallback, &decoded)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok::<(String, u64, bool), String>((server_name, len, is_archive))
            });
        }
    }

    let mut downloaded_total = 0u64;
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok((server_name, bytes, is_archive))) => {
                stats.record_success(&server_name, bytes);
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

                // Trigger Direct Unpack as soon as all archive segments finish downloading
                if is_archive && archive_remaining.fetch_sub(1, Ordering::SeqCst) == 1 && direct_unpack_handle.is_none() {
                    let unpack_dir = work_dir.clone();
                    let unpack_pw = passwords.clone();
                    info!("Direct Unpack triggered for {}", job.id);
                    direct_unpack_handle = Some(tokio::spawn(async move {
                        unpack_dir_archives(&unpack_dir, &unpack_pw).await
                    }));
                }
            }
            Ok(Err(e)) => {
                if let Some(first_pool) = pools.first() {
                    stats.record_failure(first_pool.server_name());
                }
                warn!("Segment error in job {}: {}", job.id, e);
            }
            Err(e) => warn!("Worker panic in job {}: {}", job.id, e),
        }
    }

    queue.set_status(&job.id, JobStatus::Checking);
    if let Err(e) = PostProcessor::verify_and_repair(&work_dir).await {
        warn!("PAR2 check/repair issue for {}: {}", job.id, e);
    }

    queue.set_status(&job.id, JobStatus::Extracting);
    let extracted_any = if let Some(handle) = direct_unpack_handle {
        handle.await.unwrap_or(false)
    } else {
        unpack_dir_archives(&work_dir, &passwords).await
    };

    if cfg.cleanup_archives && extracted_any {
        if let Err(e) = PostProcessor::cleanup_processed_files(&work_dir).await {
            warn!("Cleanup issue for {}: {}", job.id, e);
        }
    }

    if cfg.deobfuscate_filenames {
        if let Err(e) = PostProcessor::deobfuscate_dir(&work_dir, &job.filename).await {
            warn!("Deobfuscation issue for {}: {}", job.id, e);
        }
    }

    let cat_root = cfg.category_dir(&job.category);
    let sorted_dest = match Sorter::sort_media_file(&work_dir, &cat_root, &job.filename, &cfg.sorting).await {
        Ok(Some(path)) => {
            info!("Sorted media to {:?}", path);
            Some(path)
        }
        Ok(None) => None,
        Err(e) => {
            warn!("Sorting issue for {}: {}", job.id, e);
            None
        }
    };

    let final_dir = if let Some(sorted_file) = sorted_dest {
        let _ = tokio::fs::remove_dir_all(&work_dir).await;
        sorted_file.parent().unwrap_or(&cat_root).to_path_buf()
    } else {
        let dest = cat_root.join(&safe_job_name);
        move_to_complete(&work_dir, &dest)
            .await
            .map_err(|e| e.to_string())?;
        dest
    };

    if let (Some(script_dir), Some(script_name)) = (&cfg.script_dir, job.script.as_ref().or(cfg.default_script.as_ref())) {
        let script_path = script_dir.join(script_name);
        if script_path.exists() {
            if let Err(e) = PostProcessor::run_postproc_script(&script_path, &final_dir, &job.filename, &job.category, true).await {
                warn!("Post-proc script failed for {}: {}", job.id, e);
            }
        }
    }

    info!("Job {} completed at {:?}", job.id, final_dir);
    queue.finish_job(&job.id, final_dir.clone(), None);
    Notifier::notify(
        &cfg.notifications,
        true,
        &job.filename,
        &job.category,
        final_dir.to_str(),
    )
    .await;
    Ok(())
}

async fn unpack_dir_archives(work_dir: &Path, passwords: &[String]) -> bool {
    let mut extracted_any = false;
    if let Ok(mut entries) = tokio::fs::read_dir(work_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let ext = path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            let should_extract = match ext.as_str() {
                "rar" => PostProcessor::is_primary_rar_volume(&path),
                "zip" | "7z" => true,
                _ => false,
            };
            if should_extract {
                match PostProcessor::extract_archive_with_passwords(&path, work_dir, passwords).await {
                    Ok(()) => extracted_any = true,
                    Err(e) => warn!("Archive extraction issue for {:?}: {}", path, e),
                }
            }
        }
    }
    extracted_any
}

async fn fetch_from_pools(pools: &[Arc<NntpPool>], message_id: &str) -> std::result::Result<(String, bytes::Bytes), String> {
    let mut last_err = String::from("No NNTP pools available");
    for pool in pools {
        match pool.fetch_body(message_id).await {
            Ok(bytes) => return Ok((pool.server_name().to_string(), bytes)),
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
        let stats = StatsTracker::new();

        let app = create_router(AppState {
            config: shared_cfg.clone(),
            queue: queue.clone(),
            stats: stats.clone(),
            config_path: None,
            shutdown: None,
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
        process_job(&shared_cfg, &cfg, &queue, &stats, &job, doc).await.unwrap();
        assert_eq!(stats.total_bytes(), payload.len() as u64);

        assert!(queue.list().is_empty());
        let hist = queue.history();
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].status, "Completed");

        let assembled_path = cfg.complete_dir.join("tv").join("e2e_job").join("payload.bin");
        let assembled = tokio::fs::read(&assembled_path).await.unwrap();
        assert_eq!(assembled, payload);
    }
}
