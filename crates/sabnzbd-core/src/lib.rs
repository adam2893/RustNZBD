use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TOML parse error: {0}")]
    TomlDe(#[from] toml::de::Error),
    #[error("TOML serialize error: {0}")]
    TomlSer(#[from] toml::ser::Error),
}

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
            api_key: Uuid::new_v4().to_string().replace('-', ""),
            nzb_key: Uuid::new_v4().to_string().replace('-', ""),
            http_port: 8080,
            http_host: "0.0.0.0".to_string(),
            complete_dir: PathBuf::from("./downloads/complete"),
            download_dir: PathBuf::from("./downloads/incomplete"),
            servers: Vec::new(),
            speed_limit: None,
        }
    }
}

impl Config {
    /// Loads TOML config from `path`, or writes and returns a default config if missing.
    pub fn load_or_create(path: &Path) -> Result<Self, ConfigError> {
        if path.exists() {
            let content = std::fs::read_to_string(path)?;
            let cfg: Self = toml::from_str(&content)?;
            Ok(cfg)
        } else {
            let cfg = Self::default();
            cfg.save(path)?;
            Ok(cfg)
        }
    }

    /// Persists config as pretty TOML, creating parent directories if needed.
    pub fn save(&self, path: &Path) -> Result<(), ConfigError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let encoded = toml::to_string_pretty(self)?;
        std::fs::write(path, encoded)?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_load_or_create_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("nested").join("rustnzbd.toml");

        let created = Config::load_or_create(&cfg_path).unwrap();
        assert!(cfg_path.exists());

        let loaded = Config::load_or_create(&cfg_path).unwrap();
        assert_eq!(created.api_key, loaded.api_key);
        assert_eq!(created.nzb_key, loaded.nzb_key);
        assert_eq!(created.http_port, loaded.http_port);
    }
}
