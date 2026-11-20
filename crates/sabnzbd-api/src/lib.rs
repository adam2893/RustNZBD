use axum::{
    body::Bytes,
    extract::{Query, State},
    http::header,
    response::{Html, IntoResponse, Json, Response},
    routing::get,
    Router,
};
use flate2::read::GzDecoder;
use parking_lot::RwLock;
use sabnzbd_core::{Config, I18nCatalog, JobStatus, ServerConfig, StatsTracker};
use sabnzbd_nzb::{DownloadQueue, NzbDocument};
use serde::Deserialize;
use serde_json::{json, Value};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

pub const SABNZBD_VERSION: &str = "4.4.1";

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<Config>>,
    pub queue: Arc<DownloadQueue>,
    pub stats: Arc<StatsTracker>,
    pub config_path: Option<PathBuf>,
    pub shutdown: Option<Arc<Notify>>,
}

#[derive(Debug, Deserialize)]
pub struct ApiQuery {
    pub mode: Option<String>,
    pub output: Option<String>,
    pub apikey: Option<String>,
    pub name: Option<String>, // url, sub-action ("delete"), or nzo_id
    pub value: Option<String>,
    pub cat: Option<String>,
    pub nzbname: Option<String>,
    pub priority: Option<i32>,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index_handler))
        .route("/api", get(api_handler).post(api_post_handler))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn index_handler() -> Html<&'static str> {
    Html(include_str!("ui.html"))
}

fn check_auth(state: &AppState, mode: &str, apikey: &Option<String>) -> Option<Response> {
    if mode == "version" {
        return None;
    }
    let current_api_key = state.config.read().api_key.clone();
    match apikey {
        Some(key) if key == &current_api_key => None,
        Some(_) => Some(Json(json!({"status": false, "error": "API Key Incorrect"})).into_response()),
        None => Some(Json(json!({"status": false, "error": "API Key Required"})).into_response()),
    }
}

async fn api_handler(
    State(state): State<AppState>,
    Query(query): Query<ApiQuery>,
) -> Response {
    handle_api(state, query, None).await
}

async fn api_post_handler(
    State(state): State<AppState>,
    Query(query): Query<ApiQuery>,
    body: Bytes,
) -> Response {
    handle_api(state, query, Some(body)).await
}

async fn handle_api(
    state: AppState,
    query: ApiQuery,
    body: Option<Bytes>,
) -> Response {
    let want_xml = query.output.as_deref() == Some("xml");
    let val = handle_api_json(state, query, body).await;
    if want_xml {
        let xml = format!("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<sabnzbd>{}</sabnzbd>", json_to_xml(&val));
        ([(header::CONTENT_TYPE, "application/xml; charset=utf-8")], xml).into_response()
    } else {
        Json(val).into_response()
    }
}

async fn handle_api_json(
    state: AppState,
    query: ApiQuery,
    body: Option<Bytes>,
) -> Value {
    let mode = query.mode.as_deref().unwrap_or("queue");

    if check_auth(&state, mode, &query.apikey).is_some() {
        return match query.apikey {
            Some(_) => json!({"status": false, "error": "API Key Incorrect"}),
            None => json!({"status": false, "error": "API Key Required"}),
        };
    }

    let current_api_key = state.config.read().api_key.clone();

    match mode {
        "version" => json!({ "version": SABNZBD_VERSION }),

        "queue" => {
            if let Some(id) = query.value.as_deref() {
                match query.name.as_deref() {
                    Some("delete") => {
                        let deleted = state.queue.delete_queue_item(id);
                        return json!({ "status": deleted, "nzo_ids": [id] });
                    }
                    Some("pause") => {
                        let ok = state.queue.pause_item(id);
                        return json!({ "status": ok, "nzo_ids": [id] });
                    }
                    Some("resume") => {
                        let ok = state.queue.resume_item(id);
                        return json!({ "status": ok, "nzo_ids": [id] });
                    }
                    Some("priority") => {
                        let prio = query.priority.unwrap_or(0);
                        let ok = state.queue.set_priority(id, prio);
                        return json!({ "status": ok, "position": prio });
                    }
                    _ => {}
                }
            }

            let paused = state.queue.is_paused();
            let items = state.queue.list();
            let total_bytes: u64 = items.iter().map(|i| i.total_bytes).sum();
            let downloaded_bytes: u64 = items.iter().map(|i| i.downloaded_bytes).sum();
            let mb_left = (total_bytes.saturating_sub(downloaded_bytes)) as f64 / 1_048_576.0;
            let total_speed: u64 = items.iter().map(|i| i.speed_bps).sum();

            let queue_status = if paused {
                "Paused"
            } else if items.iter().any(|i| i.status == JobStatus::Downloading) {
                "Downloading"
            } else {
                "Idle"
            };

            let slots: Vec<_> = items
                .into_iter()
                .map(|item| {
                    json!({
                        "nzo_id": item.id,
                        "filename": item.filename,
                        "size": format!("{:.1} MB", item.total_bytes as f64 / 1_048_576.0),
                        "sizeleft": format!("{:.1} MB", item.total_bytes.saturating_sub(item.downloaded_bytes) as f64 / 1_048_576.0),
                        "status": format!("{:?}", item.status),
                        "priority": item.priority,
                        "cat": item.category,
                        "timeleft": "0:00:00",
                        "percentage": if item.total_bytes > 0 { (item.downloaded_bytes * 100) / item.total_bytes } else { 0 },
                    })
                })
                .collect();

            let speedlimit_kbps = state
                .config
                .read()
                .speed_limit
                .map(|bps| (bps / 1024).to_string())
                .unwrap_or_default();

            json!({
                "queue": {
                    "version": SABNZBD_VERSION,
                    "paused": paused,
                    "pause_int": if paused { "1" } else { "0" },
                    "speedlimit": speedlimit_kbps,
                    "speedlimit_abs": speedlimit_kbps,
                    "speed": format!("{:.1} K", total_speed as f64 / 1024.0),
                    "size": format!("{:.1} MB", total_bytes as f64 / 1_048_576.0),
                    "sizeleft": format!("{:.1} MB", mb_left),
                    "mb": format!("{:.2}", total_bytes as f64 / 1_048_576.0),
                    "mbleft": format!("{:.2}", mb_left),
                    "slots": slots,
                    "status": queue_status
                }
            })
        }

        "history" => {
            if query.name.as_deref() == Some("delete") {
                if let Some(id) = query.value.as_deref() {
                    let deleted = state.queue.delete_history_item(id);
                    return json!({ "status": deleted, "nzo_ids": [id] });
                }
            }

            let hist = state.queue.history();
            let total_size: u64 = hist.iter().map(|h| h.size).sum();
            let slots: Vec<_> = hist
                .into_iter()
                .map(|h| {
                    json!({
                        "nzo_id": h.id,
                        "name": h.name,
                        "bytes": h.size,
                        "size": format!("{:.1} MB", h.size as f64 / 1_048_576.0),
                        "category": h.category,
                        "status": h.status,
                        "completed": h.completed_time.timestamp(),
                        "storage": h.storage_path,
                        "fail_message": h.failure_reason.unwrap_or_default(),
                    })
                })
                .collect();

            json!({
                "history": {
                    "total_size": format!("{:.1} MB", total_size as f64 / 1_048_576.0),
                    "month_size": "0 B",
                    "week_size": "0 B",
                    "day_size": "0 B",
                    "slots": slots
                }
            })
        }

        "get_files" => {
            let Some(id) = query.value.as_deref() else {
                return json!({ "status": false, "error": "Missing value (nzo_id)" });
            };
            let Some(files) = state.queue.get_files(id) else {
                return json!({ "files": [] });
            };
            let list: Vec<_> = files
                .into_iter()
                .enumerate()
                .map(|(idx, f)| {
                    let bytes: u64 = f.segments.iter().map(|s| s.bytes).sum();
                    json!({
                        "nzf_id": format!("{id}_{idx}"),
                        "filename": f.subject,
                        "bytes": bytes,
                        "segments": f.segments.len(),
                        "status": "active",
                    })
                })
                .collect();
            json!({ "files": list })
        }

        "pause" => {
            state.queue.pause();
            info!("Queue paused via API");
            json!({ "status": true })
        }

        "resume" => {
            state.queue.resume();
            info!("Queue resumed via API");
            json!({ "status": true })
        }

        "shutdown" => {
            info!("Shutdown requested via API");
            if let Some(notify) = &state.shutdown {
                notify.notify_one();
            }
            json!({ "status": true })
        }

        "server_stats" => {
            let servers = state.stats.snapshot();
            let total = state.stats.total_bytes();
            json!({
                "servers": servers,
                "total": total,
                "month": total,
                "week": total,
                "day": total
            })
        }

        "get_cats" => {
            let mut cats = vec!["*".to_string()];
            for k in state.config.read().categories.keys() {
                if !cats.contains(k) {
                    cats.push(k.clone());
                }
            }
            cats.sort();
            json!({ "categories": cats })
        }

        "warnings" => json!({ "warnings": [] }),

        "change_cat" => {
            if let (Some(id), Some(cat)) = (query.value.as_deref(), query.name.clone()) {
                let ok = state.queue.set_category(id, cat);
                return json!({ "status": ok });
            }
            json!({ "status": false, "error": "Missing value or name" })
        }

        "auth" => json!({ "auth": "apikey" }),

        "get_lang" => {
            let cfg = state.config.read();
            let lang = cfg.language.clone();
            let (locales, strings) = if let Some(po_dir) = &cfg.po_dir {
                let avail = I18nCatalog::available_locales(po_dir);
                let cat = I18nCatalog::load_locale(po_dir, &lang);
                (avail, cat.strings)
            } else {
                (vec!["en".to_string()], std::collections::HashMap::new())
            };
            json!({
                "language": lang,
                "locales": locales,
                "strings": strings
            })
        }

        "fullstatus" => {
            json!({
                "status": {
                    "version": SABNZBD_VERSION,
                    "config": {
                        "api_key": current_api_key,
                    }
                }
            })
        }

        "config" => {
            if query.name.as_deref() == Some("speedlimit") {
                let raw_val = query.value.as_deref().unwrap_or("").trim();
                let limit_bps = if raw_val.is_empty() || raw_val == "0" || raw_val == "100%" {
                    None
                } else if let Ok(kbps) = raw_val.trim_end_matches(['K', 'k']).parse::<usize>() {
                    Some(kbps.saturating_mul(1024))
                } else {
                    return json!({ "status": false, "error": "Invalid speedlimit" });
                };
                {
                    let mut cfg = state.config.write();
                    cfg.speed_limit = limit_bps;
                    if let Some(path) = &state.config_path {
                        if let Err(e) = cfg.save(path) {
                            warn!("Failed saving config to {:?}: {}", path, e);
                        }
                    }
                }
                return json!({ "status": true });
            }
            json!({ "status": false, "error": "Unsupported config operation" })
        }

        "get_config" => {
            let mut cfg = state.config.read().clone();
            for s in &mut cfg.servers {
                if s.password.is_some() {
                    s.password = Some("********".to_string());
                }
            }
            if cfg.notifications.smtp_password.is_some() {
                cfg.notifications.smtp_password = Some("********".to_string());
            }
            json!({ "config": cfg })
        }

        "save_settings" => {
            let Some(raw_body) = body else {
                return json!({ "status": false, "error": "Missing settings JSON" });
            };
            let patch: Value = match serde_json::from_slice(&raw_body) {
                Ok(v) => v,
                Err(e) => return json!({ "status": false, "error": format!("Invalid JSON: {e}") }),
            };
            {
                let mut cfg = state.config.write();
                if let Some(v) = patch.get("cleanup_archives").and_then(|x| x.as_bool()) {
                    cfg.cleanup_archives = v;
                }
                if let Some(v) = patch.get("deobfuscate_filenames").and_then(|x| x.as_bool()) {
                    cfg.deobfuscate_filenames = v;
                }
                if let Some(v) = patch.get("detect_duplicates").and_then(|x| x.as_bool()) {
                    cfg.detect_duplicates = v;
                }
                if let Some(v) = patch.get("speed_limit") {
                    cfg.speed_limit = v.as_u64().map(|n| n as usize);
                }
                if let Some(v) = patch.get("language").and_then(|x| x.as_str()) {
                    cfg.language = v.to_string();
                }
                if let Some(v) = patch.get("passwords") {
                    if let Ok(pw) = serde_json::from_value(v.clone()) {
                        cfg.passwords = pw;
                    }
                }
                if let Some(v) = patch.get("categories") {
                    if let Ok(cats) = serde_json::from_value(v.clone()) {
                        cfg.categories = cats;
                    }
                }
                if let Some(v) = patch.get("sorting") {
                    if let Ok(sorting) = serde_json::from_value(v.clone()) {
                        cfg.sorting = sorting;
                    }
                }
                if let Some(v) = patch.get("notifications") {
                    let prev_pass = cfg.notifications.smtp_password.clone();
                    if let Ok(mut notif) = serde_json::from_value::<sabnzbd_core::NotificationConfig>(v.clone()) {
                        if notif.smtp_password.as_deref() == Some("********") {
                            notif.smtp_password = prev_pass;
                        }
                        cfg.notifications = notif;
                    }
                }
                if let Some(v) = patch.get("rss_feeds") {
                    if let Ok(feeds) = serde_json::from_value(v.clone()) {
                        cfg.rss_feeds = feeds;
                    }
                }
                if let Some(v) = patch.get("schedules") {
                    if let Ok(scheds) = serde_json::from_value(v.clone()) {
                        cfg.schedules = scheds;
                    }
                }
                if let Some(path) = &state.config_path {
                    if let Err(e) = cfg.save(path) {
                        warn!("Failed saving config to {:?}: {}", path, e);
                    }
                }
            }
            json!({ "status": true })
        }

        "delete_server" => {
            let idx = query.value.as_deref().and_then(|v| v.parse::<usize>().ok());
            let mut removed = false;
            if let Some(i) = idx {
                let mut cfg = state.config.write();
                if i < cfg.servers.len() {
                    cfg.servers.remove(i);
                    removed = true;
                    if let Some(path) = &state.config_path {
                        let _ = cfg.save(path);
                    }
                }
            }
            json!({ "status": removed })
        }

        "add_server" => {
            let Some(raw_body) = body else {
                return json!({ "status": false, "error": "Missing server JSON" });
            };
            let server: ServerConfig = match serde_json::from_slice(&raw_body) {
                Ok(s) => s,
                Err(e) => {
                    return json!({ "status": false, "error": format!("Invalid server JSON: {}", e) });
                }
            };
            {
                let mut cfg = state.config.write();
                cfg.servers.push(server);
                if let Some(path) = &state.config_path {
                    if let Err(e) = cfg.save(path) {
                        warn!("Failed saving config to {:?}: {}", path, e);
                    }
                }
            }
            json!({ "status": true })
        }

        "addlocalfile" => {
            let Some(path_str) = query.name.as_deref() else {
                return json!({ "status": false, "error": "Missing file path" });
            };
            let path = PathBuf::from(path_str);
            let raw = match tokio::fs::read(&path).await {
                Ok(b) => b,
                Err(e) => {
                    return json!({ "status": false, "error": format!("Failed reading file: {}", e) });
                }
            };
            let xml_str = extract_nzb_xml(&raw);
            let fallback_name = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("local.nzb")
                .to_string();
            let cat = query.cat.unwrap_or_else(|| "default".to_string());
            match NzbDocument::parse(&xml_str) {
                Ok(doc) => {
                    let total_bytes = doc.total_bytes();
                    let reject_dups = state.config.read().detect_duplicates;
                    match state.queue.add_nzb_checked(fallback_name, total_bytes, cat, Some(doc), reject_dups) {
                        Ok(item) => json!({ "status": true, "nzo_ids": [item.id] }),
                        Err(e) => json!({ "status": false, "error": e.to_string() }),
                    }
                }
                Err(e) => json!({ "status": false, "error": format!("Invalid NZB XML: {}", e) }),
            }
        }

        "addurl" => {
            let url = query.name.unwrap_or_default();
            if url.is_empty() {
                return json!({ "status": false, "error": "Missing URL" });
            }
            let cat = query.cat.unwrap_or_else(|| "default".to_string());
            let fallback_name = query
                .nzbname
                .unwrap_or_else(|| {
                    url.rsplit('/')
                        .next()
                        .and_then(|s| s.split('?').next())
                        .filter(|s| !s.is_empty())
                        .unwrap_or("download.nzb")
                        .to_string()
                });

            info!("Fetching NZB from URL: {}", url);
            let client = match reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    return json!({ "status": false, "error": format!("HTTP client error: {}", e) });
                }
            };

            let resp = match client.get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    return json!({ "status": false, "error": format!("Fetch failed: {}", e) });
                }
            };

            if !resp.status().is_success() {
                return json!({
                    "status": false,
                    "error": format!("Upstream returned HTTP {}", resp.status())
                });
            }

            let raw_bytes = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    return json!({ "status": false, "error": format!("Read body failed: {}", e) });
                }
            };

            let xml_str = extract_nzb_xml(&raw_bytes);
            match NzbDocument::parse(&xml_str) {
                Ok(doc) => {
                    let total_bytes = doc.total_bytes();
                    let reject_dups = state.config.read().detect_duplicates;
                    match state.queue.add_nzb_checked(fallback_name, total_bytes, cat, Some(doc), reject_dups) {
                        Ok(item) => json!({
                            "status": true,
                            "nzo_ids": [item.id]
                        }),
                        Err(e) => json!({
                            "status": false,
                            "error": e.to_string()
                        }),
                    }
                }
                Err(e) => json!({
                    "status": false,
                    "error": format!("Invalid NZB XML from URL: {}", e)
                }),
            }
        }

        "addfile" => {
            let Some(raw_body) = body else {
                return json!({ "status": false, "error": "Missing NZB payload" });
            };
            let xml_str = extract_nzb_xml(&raw_body);
            match NzbDocument::parse(&xml_str) {
                Ok(doc) => {
                    let total_bytes = doc.total_bytes();
                    let name = query
                        .nzbname
                        .or(query.name)
                        .unwrap_or_else(|| "upload.nzb".to_string());
                    let cat = query.cat.unwrap_or_else(|| "default".to_string());
                    info!("Add NZB file: {} ({} bytes)", name, total_bytes);
                    let reject_dups = state.config.read().detect_duplicates;
                    match state.queue.add_nzb_checked(name, total_bytes, cat, Some(doc), reject_dups) {
                        Ok(item) => json!({
                            "status": true,
                            "nzo_ids": [item.id]
                        }),
                        Err(e) => json!({
                            "status": false,
                            "error": e.to_string()
                        }),
                    }
                }
                Err(e) => json!({
                    "status": false,
                    "error": format!("Invalid NZB XML: {}", e)
                }),
            }
        }

        _ => json!({
            "status": false,
            "error": format!("Unknown mode: {}", mode)
        }),
    }
}

fn json_to_xml(val: &Value) -> String {
    match val {
        Value::Object(map) => map
            .iter()
            .map(|(k, v)| match v {
                Value::Array(arr) => arr
                    .iter()
                    .map(|item| format!("<{k}>{}</{k}>", json_to_xml(item)))
                    .collect::<String>(),
                _ => format!("<{k}>{}</{k}>", json_to_xml(v)),
            })
            .collect(),
        Value::Array(arr) => arr
            .iter()
            .map(|item| format!("<item>{}</item>", json_to_xml(item)))
            .collect(),
        Value::String(s) => s
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;"),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
    }
}

/// Extracts `<nzb ...>...</nzb>` from raw XML, gzipped `.nzb.gz`, or multipart/form-data upload.
pub fn extract_nzb_xml(raw: &[u8]) -> String {
    let decompressed;
    let slice = if raw.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = GzDecoder::new(raw);
        let mut buf = Vec::new();
        if decoder.read_to_end(&mut buf).is_ok() {
            decompressed = buf;
            &decompressed[..]
        } else {
            raw
        }
    } else {
        raw
    };

    let text = String::from_utf8_lossy(slice);
    if let Some(start) = text.find("<nzb") {
        if let Some(end) = text.rfind("</nzb>") {
            return text[start..end + 6].to_string();
        }
    }
    text.into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::Value;
    use tower::ServiceExt;

    fn test_state(cfg_path: Option<PathBuf>) -> (AppState, String) {
        let cfg = Config::default();
        let key = cfg.api_key.clone();
        let state = AppState {
            config: Arc::new(RwLock::new(cfg)),
            queue: Arc::new(DownloadQueue::new()),
            stats: StatsTracker::new(),
            config_path: cfg_path,
            shutdown: None,
        };
        (state, key)
    }

    async fn json_body(resp: Response) -> Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn test_api_version_and_auth() {
        let (state, _) = test_state(None);
        let app = create_router(state);

        let resp = app
            .clone()
            .oneshot(Request::get("/api?mode=version").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v = json_body(resp).await;
        assert_eq!(v["version"], SABNZBD_VERSION);

        let resp = app
            .oneshot(Request::get("/api?mode=queue&apikey=wrong").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let v = json_body(resp).await;
        assert_eq!(v["status"], false);
    }

    #[tokio::test]
    async fn test_api_addfile_queue_pause_delete() {
        let (state, key) = test_state(None);
        let app = create_router(state);

        let nzb = r#"<nzb><file poster="a" date="1" subject="s"><groups><group>alt.test</group></groups><segments><segment bytes="1234" number="1">id1@test</segment></segments></file></nzb>"#;

        let resp = app
            .clone()
            .oneshot(
                Request::post(format!("/api?mode=addfile&nzbname=demo.nzb&cat=tv&apikey={key}"))
                    .body(Body::from(nzb))
                    .unwrap(),
            )
            .await
            .unwrap();
        let add_json = json_body(resp).await;
        assert_eq!(add_json["status"], true);
        let nzo_id = add_json["nzo_ids"][0].as_str().unwrap().to_string();

        // Pause
        let resp = app
            .clone()
            .oneshot(Request::get(format!("/api?mode=pause&apikey={key}")).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(json_body(resp).await["status"], true);

        // Queue shows paused + 1 slot
        let resp = app
            .clone()
            .oneshot(Request::get(format!("/api?mode=queue&apikey={key}")).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let q = json_body(resp).await;
        assert_eq!(q["queue"]["paused"], true);
        assert_eq!(q["queue"]["slots"].as_array().unwrap().len(), 1);
        assert_eq!(q["queue"]["slots"][0]["filename"], "demo.nzb");

        // Delete
        let resp = app
            .clone()
            .oneshot(
                Request::get(format!("/api?mode=queue&name=delete&value={nzo_id}&apikey={key}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(json_body(resp).await["status"], true);
    }

    #[tokio::test]
    async fn test_api_add_server_redacts_password() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("rustnzbd.toml");
        let (state, key) = test_state(Some(cfg_path.clone()));
        let app = create_router(state);

        let srv = json!({
            "name": "Usenet",
            "host": "news.example.com",
            "port": 563,
            "ssl": true,
            "username": "u",
            "password": "supersecretpassword",
            "connections": 8,
            "retention": 0,
            "priority": 0,
            "enable": true,
            "optional": false
        });

        let resp = app
            .clone()
            .oneshot(
                Request::post(format!("/api?mode=add_server&apikey={key}"))
                    .body(Body::from(srv.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(json_body(resp).await["status"], true);
        assert!(cfg_path.exists());

        let resp = app
            .oneshot(Request::get(format!("/api?mode=get_config&apikey={key}")).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let cfg = json_body(resp).await;
        assert_eq!(cfg["config"]["servers"][0]["password"], "********");
    }

    #[tokio::test]
    async fn test_api_gzipped_nzb_and_speedlimit() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let (state, key) = test_state(None);
        let app = create_router(state);

        let nzb = r#"<nzb><file poster="a" date="1" subject="s"><groups><group>alt.test</group></groups><segments><segment bytes="2048" number="1">id1@test</segment></segments></file></nzb>"#;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(nzb.as_bytes()).unwrap();
        let gz_bytes = encoder.finish().unwrap();

        let resp = app
            .clone()
            .oneshot(
                Request::post(format!("/api?mode=addfile&nzbname=gz.nzb&apikey={key}"))
                    .body(Body::from(gz_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(json_body(resp).await["status"], true);

        let resp = app
            .clone()
            .oneshot(
                Request::get(format!("/api?mode=config&name=speedlimit&value=500K&apikey={key}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(json_body(resp).await["status"], true);

        let resp = app
            .oneshot(Request::get(format!("/api?mode=queue&apikey={key}")).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let q = json_body(resp).await;
        assert_eq!(q["queue"]["speedlimit"], "500");
        assert_eq!(q["queue"]["slots"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_api_get_files_stats_and_xml_output() {
        let (state, key) = test_state(None);
        state.stats.record_success("Primary", 4096);
        let app = create_router(state);

        let nzb = r#"<nzb><file poster="a" date="1" subject="movie.mkv"><groups><group>alt.test</group></groups><segments><segment bytes="1024" number="1">id1@test</segment></segments></file></nzb>"#;
        let resp = app
            .clone()
            .oneshot(
                Request::post(format!("/api?mode=addfile&nzbname=xml.nzb&apikey={key}"))
                    .body(Body::from(nzb))
                    .unwrap(),
            )
            .await
            .unwrap();
        let nzo_id = json_body(resp).await["nzo_ids"][0].as_str().unwrap().to_string();

        let resp = app
            .clone()
            .oneshot(
                Request::get(format!("/api?mode=get_files&value={nzo_id}&apikey={key}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let files_json = json_body(resp).await;
        assert_eq!(files_json["files"][0]["filename"], "movie.mkv");

        let resp = app
            .clone()
            .oneshot(
                Request::get(format!("/api?mode=server_stats&apikey={key}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let stats_json = json_body(resp).await;
        assert_eq!(stats_json["total"], 4096);

        let resp = app
            .oneshot(Request::get("/api?mode=version&output=xml").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let xml = String::from_utf8(body.to_vec()).unwrap();
        assert!(xml.contains("<version>4.4.1</version>"));
    }
}
