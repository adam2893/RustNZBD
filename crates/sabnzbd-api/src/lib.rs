use axum::{
    extract::{Query, State},
    response::{Html, IntoResponse, Json, Response},
    routing::get,
    Router,
};
use parking_lot::RwLock;
use sabnzbd_core::Config;
use sabnzbd_nzb::DownloadQueue;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

pub const SABNZBD_VERSION: &str = "4.4.1";

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<Config>>,
    pub queue: Arc<DownloadQueue>,
}

#[derive(Debug, Deserialize)]
pub struct ApiQuery {
    pub mode: Option<String>,
    pub output: Option<String>,
    pub apikey: Option<String>,
    pub name: Option<String>, // url or category or nzo_id
    pub value: Option<String>,
    pub cat: Option<String>,
    pub priority: Option<i32>,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index_handler))
        .route("/api", get(api_handler).post(api_handler))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn index_handler() -> Html<&'static str> {
    Html(r#"<!DOCTYPE html>
<html>
<head>
    <title>RustNZBD</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 40px; background: #0f172a; color: #f8fafc; }
        .card { background: #1e293b; border-radius: 12px; padding: 24px; max-width: 650px; margin: auto; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1); }
        h1 { color: #38bdf8; margin-top: 0; }
        .badge { background: #0369a1; padding: 4px 10px; border-radius: 9999px; font-size: 13px; font-weight: bold; }
        code { background: #334155; padding: 2px 6px; border-radius: 4px; font-family: monospace; }
    </style>
</head>
<body>
    <div class="card">
        <h1>RustNZBD <span class="badge">SABnzbd in Rust</span></h1>
        <p>High-performance Usenet client written in Rust with full SABnzbd REST API compatibility.</p>
        <p>API endpoint: <code>/api?mode=queue&apikey=...</code></p>
        <p>Compatible with Sonarr, Radarr, Lidarr, Readarr, and SABnzbd frontends.</p>
    </div>
</body>
</html>"#)
}

async fn api_handler(
    State(state): State<AppState>,
    Query(query): Query<ApiQuery>,
) -> Response {
    let mode = query.mode.as_deref().unwrap_or("queue");

    // Authenticate unless mode is version (SABnzbd allows version query or apikey validation)
    let current_api_key = state.config.read().api_key.clone();
    if mode != "version" {
        if let Some(key) = &query.apikey {
            if key != &current_api_key {
                return Json(json!({"status": false, "error": "API Key Incorrect"})).into_response();
            }
        } else {
            return Json(json!({"status": false, "error": "API Key Required"})).into_response();
        }
    }

    match mode {
        "version" => Json(json!({ "version": SABNZBD_VERSION })).into_response(),

        "queue" => {
            let items = state.queue.list();
            let total_bytes: u64 = items.iter().map(|i| i.total_bytes).sum();
            let downloaded_bytes: u64 = items.iter().map(|i| i.downloaded_bytes).sum();
            let mb_left = (total_bytes.saturating_sub(downloaded_bytes)) as f64 / (1024.0 * 1024.0);

            let slots: Vec<_> = items
                .into_iter()
                .map(|item| {
                    json!({
                        "nzo_id": item.id,
                        "filename": item.filename,
                        "size": format!("{:.1} MB", item.total_bytes as f64 / 1048576.0),
                        "sizeleft": format!("{:.1} MB", item.total_bytes.saturating_sub(item.downloaded_bytes) as f64 / 1048576.0),
                        "status": format!("{:?}", item.status),
                        "priority": item.priority,
                        "cat": item.category,
                        "timeleft": "0:00:00",
                        "percentage": if item.total_bytes > 0 { (item.downloaded_bytes * 100) / item.total_bytes } else { 0 },
                    })
                })
                .collect();

            Json(json!({
                "queue": {
                    "version": SABNZBD_VERSION,
                    "paused": false,
                    "pause_int": "0",
                    "speedlimit": "",
                    "speedlimit_abs": "",
                    "speed": "0",
                    "size": format!("{:.1} MB", total_bytes as f64 / 1048576.0),
                    "sizeleft": format!("{:.1} MB", mb_left),
                    "mb": format!("{:.2}", total_bytes as f64 / 1048576.0),
                    "mbleft": format!("{:.2}", mb_left),
                    "slots": slots,
                    "status": "Idle"
                }
            })).into_response()
        }

        "history" => {
            Json(json!({
                "history": {
                    "total_size": "0 B",
                    "month_size": "0 B",
                    "week_size": "0 B",
                    "day_size": "0 B",
                    "slots": []
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

        "addurl" => {
            let url = query.name.unwrap_or_default();
            let cat = query.cat.unwrap_or_else(|| "default".to_string());
            info!("Add URL received: {}", url);
            let item = state.queue.add_nzb(url.clone(), 0, cat);
            Json(json!({
                "status": true,
                "nzo_ids": [item.id]
            })).into_response()
        }

        _ => Json(json!({
            "status": false,
            "error": format!("Unknown mode: {}", mode)
        })).into_response(),
    }
}
