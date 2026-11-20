use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub ssl: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connections: usize,
    pub retention: u32,
    pub priority: i32,
    pub enable: bool,
    pub optional: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            name: "Default Server".to_string(),
            host: "news.example.com".to_string(),
            port: 563,
            ssl: true,
            username: None,
            password: None,
            connections: 8,
            retention: 0,
            priority: 0,
            enable: false,
            optional: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub api_key: String,
    pub nzb_key: String,
    pub http_port: u16,
    pub http_host: String,
    pub complete_dir: PathBuf,
    pub download_dir: PathBuf,
    pub servers: Vec<ServerConfig>,
    pub speed_limit: Option<usize>, // bytes/s, None = unlimited
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_key: Uuid::new_v4().to_string().replace("-", ""),
            nzb_key: Uuid::new_v4().to_string().replace("-", ""),
            http_port: 8080,
            http_host: "0.0.0.0".to_string(),
            complete_dir: PathBuf::from("./downloads/complete"),
            download_dir: PathBuf::from("./downloads/incomplete"),
            servers: Vec::new(),
            speed_limit: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Queued,
    Downloading,
    Paused,
    Checking,
    Repairing,
    Extracting,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueItem {
    pub id: String,
    pub filename: String,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub status: JobStatus,
    pub priority: i32,
    pub category: String,
    pub eta_seconds: Option<u64>,
    pub speed_bps: u64,
    pub time_added: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryItem {
    pub id: String,
    pub name: String,
    pub size: u64,
    pub category: String,
    pub status: String,
    pub completed_time: DateTime<Utc>,
    pub storage_path: PathBuf,
    pub failure_reason: Option<String>,
}
