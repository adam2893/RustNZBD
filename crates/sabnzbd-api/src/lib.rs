use axum::{
    body::Bytes,
    extract::{Query, State},
    response::{Html, IntoResponse, Json, Response},
    routing::get,
    Router,
};
use flate2::read::GzDecoder;
use parking_lot::RwLock;
use sabnzbd_core::{Config, JobStatus, ServerConfig};
use sabnzbd_nzb::{DownloadQueue, NzbDocument};
use serde::Deserialize;
use serde_json::json;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

pub const SABNZBD_VERSION: &str = "4.4.1";

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<Config>>,
    pub queue: Arc<DownloadQueue>,
    pub config_path: Option<PathBuf>,
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
    let mode = query.mode.as_deref().unwrap_or("queue");

    if let Some(err_resp) = check_auth(&state, mode, &query.apikey) {
        return err_resp;
    }

    let current_api_key = state.config.read().api_key.clone();

    match mode {
        "version" => Json(json!({ "version": SABNZBD_VERSION })).into_response(),

        "queue" => {
            if let Some(id) = query.value.as_deref() {
                match query.name.as_deref() {
                    Some("delete") => {
                        let deleted = state.queue.delete_queue_item(id);
                        return Json(json!({ "status": deleted, "nzo_ids": [id] })).into_response();
                    }
                    Some("pause") => {
                        let ok = state.queue.pause_item(id);
                        return Json(json!({ "status": ok, "nzo_ids": [id] })).into_response();
                    }
                    Some("resume") => {
                        let ok = state.queue.resume_item(id);
                        return Json(json!({ "status": ok, "nzo_ids": [id] })).into_response();
                    }
                    Some("priority") => {
                        let prio = query.priority.unwrap_or(0);
                        let ok = state.queue.set_priority(id, prio);
                        return Json(json!({ "status": ok, "position": prio })).into_response();
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

            Json(json!({
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
            })).into_response()
        }

        "history" => {
            if query.name.as_deref() == Some("delete") {
                if let Some(id) = query.value.as_deref() {
                    let deleted = state.queue.delete_history_item(id);
                    return Json(json!({ "status": deleted, "nzo_ids": [id] })).into_response();
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

            Json(json!({
                "history": {
                    "total_size": format!("{:.1} MB", total_size as f64 / 1_048_576.0),
                    "month_size": "0 B",
                    "week_size": "0 B",
                    "day_size": "0 B",
                    "slots": slots
                }
            })).into_response()
        }

        "pause" => {
            state.queue.pause();
            info!("Queue paused via API");
            Json(json!({ "status": true })).into_response()
        }

        "resume" => {
            state.queue.resume();
            info!("Queue resumed via API");
            Json(json!({ "status": true })).into_response()
        }

        "server_stats" => {
            Json(json!({
                "servers": {},
                "total": 0,
                "month": 0,
                "week": 0,
                "day": 0
            })).into_response()
        }

        "get_cats" => {
            let mut cats = vec!["*".to_string()];
            for k in state.config.read().categories.keys() {
                if !cats.contains(k) {
                    cats.push(k.clone());
                }
            }
            cats.sort();
            Json(json!({ "categories": cats })).into_response()
        }

        "warnings" => Json(json!({ "warnings": [] })).into_response(),

        "change_cat" => {
            if let (Some(id), Some(cat)) = (query.value.as_deref(), query.name.clone()) {
                let ok = state.queue.set_category(id, cat);
                return Json(json!({ "status": ok })).into_response();
            }
            Json(json!({ "status": false, "error": "Missing value or name" })).into_response()
        }

        "auth" => Json(json!({ "auth": "apikey" })).into_response(),

        "fullstatus" => {
            Json(json!({
                "status": {
                    "version": SABNZBD_VERSION,
                    "config": {
                        "api_key": current_api_key,
                    }
                }
            })).into_response()
        }

        "config" => {
            if query.name.as_deref() == Some("speedlimit") {
                let raw_val = query.value.as_deref().unwrap_or("").trim();
                let limit_bps = if raw_val.is_empty() || raw_val == "0" || raw_val == "100%" {
                    None
                } else if let Ok(kbps) = raw_val.trim_end_matches(['K', 'k']).parse::<usize>() {
                    Some(kbps.saturating_mul(1024))
                } else {
                    return Json(json!({ "status": false, "error": "Invalid speedlimit" })).into_response();
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
                return Json(json!({ "status": true })).into_response();
            }
            Json(json!({ "status": false, "error": "Unsupported config operation" })).into_response()
        }

        "get_config" => {
            let mut cfg = state.config.read().clone();
            for s in &mut cfg.servers {
                if s.password.is_some() {
                    s.password = Some("********".to_string());
                }
            }
            Json(json!({ "config": cfg })).into_response()
        }

        "add_server" => {
            let Some(raw_body) = body else {
                return Json(json!({ "status": false, "error": "Missing server JSON" })).into_response();
            };
            let server: ServerConfig = match serde_json::from_slice(&raw_body) {
                Ok(s) => s,
                Err(e) => {
                    return Json(json!({ "status": false, "error": format!("Invalid server JSON: {}", e) })).into_response();
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
            Json(json!({ "status": true })).into_response()
        }

        "addurl" => {
            let url = query.name.unwrap_or_default();
            if url.is_empty() {
                return Json(json!({ "status": false, "error": "Missing URL" })).into_response();
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
                    return Json(json!({ "status": false, "error": format!("HTTP client error: {}", e) })).into_response();
                }
            };

            let resp = match client.get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    return Json(json!({ "status": false, "error": format!("Fetch failed: {}", e) })).into_response();
                }
            };

            if !resp.status().is_success() {
                return Json(json!({
                    "status": false,
                    "error": format!("Upstream returned HTTP {}", resp.status())
                })).into_response();
            }

            let raw_bytes = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    return Json(json!({ "status": false, "error": format!("Read body failed: {}", e) })).into_response();
                }
            };

            let xml_str = extract_nzb_xml(&raw_bytes);
            match NzbDocument::parse(&xml_str) {
                Ok(doc) => {
                    let total_bytes = doc.total_bytes();
                    let reject_dups = state.config.read().detect_duplicates;
                    match state.queue.add_nzb_checked(fallback_name, total_bytes, cat, Some(doc), reject_dups) {
                        Ok(item) => Json(json!({
                            "status": true,
                            "nzo_ids": [item.id]
                        })).into_response(),
                        Err(e) => Json(json!({
                            "status": false,
                            "error": e.to_string()
                        })).into_response(),
                    }
                }
                Err(e) => Json(json!({
                    "status": false,
                    "error": format!("Invalid NZB XML from URL: {}", e)
                })).into_response(),
            }
        }

        "addfile" => {
            let Some(raw_body) = body else {
                return Json(json!({ "status": false, "error": "Missing NZB payload" })).into_response();
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
                        Ok(item) => Json(json!({
                            "status": true,
                            "nzo_ids": [item.id]
                        })).into_response(),
                        Err(e) => Json(json!({
                            "status": false,
                            "error": e.to_string()
                        })).into_response(),
                    }
                }
                Err(e) => Json(json!({
                    "status": false,
                    "error": format!("Invalid NZB XML: {}", e)
                })).into_response(),
            }
        }

        _ => Json(json!({
            "status": false,
            "error": format!("Unknown mode: {}", mode)
        })).into_response(),
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
            config_path: cfg_path,
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
}
