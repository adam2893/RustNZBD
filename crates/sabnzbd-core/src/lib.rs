use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
    #[serde(default)]
    pub watch_dir: Option<PathBuf>,
    #[serde(default)]
    pub script_dir: Option<PathBuf>,
    #[serde(default)]
    pub default_script: Option<String>,
    #[serde(default)]
    pub categories: HashMap<String, PathBuf>,
    pub servers: Vec<ServerConfig>,
    pub speed_limit: Option<usize>, // bytes/s, None = unlimited
    #[serde(default = "default_true")]
    pub cleanup_archives: bool,
    #[serde(default = "default_true")]
    pub deobfuscate_filenames: bool,
    #[serde(default = "default_true")]
    pub detect_duplicates: bool,
    #[serde(default)]
    pub passwords: Vec<String>,
    #[serde(default)]
    pub rss_feeds: Vec<RssFeedConfig>,
    #[serde(default)]
    pub schedules: Vec<ScheduleRule>,
    #[serde(default)]
    pub notifications: NotificationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RssFeedConfig {
    pub name: String,
    pub uri: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_rss_interval")]
    pub interval_minutes: u64,
    #[serde(default = "default_cat")]
    pub category: String,
    #[serde(default)]
    pub accept_filters: Vec<String>,
    #[serde(default)]
    pub reject_filters: Vec<String>,
}

fn default_rss_interval() -> u64 {
    15
}

fn default_cat() -> String {
    "default".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleAction {
    Pause,
    Resume,
    SpeedLimit(Option<usize>),
    ScanRss,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRule {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub hour: u8,
    pub minute: u8,
    #[serde(default)]
    pub days: Vec<u8>, // 1=Mon..7=Sun, empty = every day
    pub action: ScheduleAction,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotificationConfig {
    #[serde(default)]
    pub webhook_url: Option<String>,
    #[serde(default = "default_true")]
    pub notify_on_complete: bool,
    #[serde(default = "default_true")]
    pub notify_on_fail: bool,
}

fn default_true() -> bool {
    true
}

impl Default for Config {
    fn default() -> Self {
        let mut categories = HashMap::new();
        categories.insert("tv".to_string(), PathBuf::from("./downloads/complete/tv"));
        categories.insert("movies".to_string(), PathBuf::from("./downloads/complete/movies"));
        Self {
            api_key: Uuid::new_v4().to_string().replace('-', ""),
            nzb_key: Uuid::new_v4().to_string().replace('-', ""),
            http_port: 8080,
            http_host: "0.0.0.0".to_string(),
            complete_dir: PathBuf::from("./downloads/complete"),
            download_dir: PathBuf::from("./downloads/incomplete"),
            watch_dir: Some(PathBuf::from("./downloads/watch")),
            script_dir: None,
            default_script: None,
            categories,
            servers: Vec::new(),
            speed_limit: None,
            cleanup_archives: true,
            deobfuscate_filenames: true,
            detect_duplicates: true,
            passwords: Vec::new(),
            rss_feeds: Vec::new(),
            schedules: Vec::new(),
            notifications: NotificationConfig::default(),
        }
    }
}

/// Extracts SABnzbd inline password syntax `Release.Name{{secret}}.nzb` -> `("Release.Name.nzb", Some("secret"))`.
pub fn extract_nzb_password(name: &str) -> (String, Option<String>) {
    if let Some(start) = name.find("{{") {
        if let Some(rel_end) = name[start + 2..].find("}}") {
            let end = start + 2 + rel_end;
            let pass = name[start + 2..end].to_string();
            let cleaned = format!("{}{}", &name[..start], &name[end + 2..]);
            return (cleaned, if pass.is_empty() { None } else { Some(pass) });
        }
    }
    (name.to_string(), None)
}

impl Config {
    /// Resolves the final destination directory for a completed job based on its category.
    pub fn category_dir(&self, category: &str) -> PathBuf {
        let cat = category.trim();
        if cat.is_empty() || cat == "default" || cat == "*" {
            return self.complete_dir.clone();
        }
        if let Some(mapped) = self.categories.get(cat) {
            return mapped.clone();
        }
        let safe_cat = Path::new(cat)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("default");
        self.complete_dir.join(safe_cat)
    }
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

    /// Imports core settings and `[[server]]` sections from a legacy `sabnzbd.ini` string.
    pub fn import_sabnzbd_ini(ini: &str) -> Self {
        let mut cfg = Self::default();
        let mut section = String::new();
        let mut current_server: Option<ServerConfig> = None;

        for raw_line in ini.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }
            if line.starts_with("[[") && line.ends_with("]]") {
                if let Some(srv) = current_server.take() {
                    cfg.servers.push(srv);
                }
                let name = line[2..line.len() - 2].trim().to_string();
                current_server = Some(ServerConfig {
                    name,
                    enable: true,
                    ..Default::default()
                });
                continue;
            }
            if line.starts_with('[') && line.ends_with(']') {
                if let Some(srv) = current_server.take() {
                    cfg.servers.push(srv);
                }
                section = line[1..line.len() - 1].trim().to_string();
                continue;
            }

            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            let key = k.trim();
            let val = v.trim();

            if let Some(srv) = current_server.as_mut() {
                match key {
                    "host" => srv.host = val.to_string(),
                    "port" => srv.port = val.parse().unwrap_or(srv.port),
                    "ssl" => srv.ssl = val == "1" || val.eq_ignore_ascii_case("true"),
                    "username" => srv.username = (!val.is_empty()).then(|| val.to_string()),
                    "password" => srv.password = (!val.is_empty()).then(|| val.to_string()),
                    "connections" => srv.connections = val.parse().unwrap_or(srv.connections),
                    "priority" => srv.priority = val.parse().unwrap_or(0),
                    "retention" => srv.retention = val.parse().unwrap_or(0),
                    "enable" => srv.enable = val == "1" || val.eq_ignore_ascii_case("true"),
                    "optional" => srv.optional = val == "1" || val.eq_ignore_ascii_case("true"),
                    _ => {}
                }
            } else if section == "misc" {
                match key {
                    "api_key" if !val.is_empty() => cfg.api_key = val.to_string(),
                    "nzb_key" if !val.is_empty() => cfg.nzb_key = val.to_string(),
                    "port" => cfg.http_port = val.parse().unwrap_or(cfg.http_port),
                    "host" if !val.is_empty() => cfg.http_host = val.to_string(),
                    "download_dir" if !val.is_empty() => cfg.download_dir = PathBuf::from(val),
                    "complete_dir" if !val.is_empty() => cfg.complete_dir = PathBuf::from(val),
                    "dirscan_dir" if !val.is_empty() => cfg.watch_dir = Some(PathBuf::from(val)),
                    "script_dir" if !val.is_empty() => cfg.script_dir = Some(PathBuf::from(val)),
                    _ => {}
                }
            }
        }

        if let Some(srv) = current_server.take() {
            cfg.servers.push(srv);
        }
        cfg
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
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub script: Option<String>,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServerStats {
    pub bytes_downloaded: u64,
    pub articles_ok: u64,
    pub articles_failed: u64,
}

#[derive(Debug, Default)]
pub struct StatsTracker {
    servers: DashMap<String, ServerStats>,
}

impl StatsTracker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_success(&self, server_name: &str, bytes: u64) {
        let mut entry = self.servers.entry(server_name.to_string()).or_default();
        entry.bytes_downloaded = entry.bytes_downloaded.saturating_add(bytes);
        entry.articles_ok = entry.articles_ok.saturating_add(1);
    }

    pub fn record_failure(&self, server_name: &str) {
        let mut entry = self.servers.entry(server_name.to_string()).or_default();
        entry.articles_failed = entry.articles_failed.saturating_add(1);
    }

    pub fn snapshot(&self) -> HashMap<String, ServerStats> {
        self.servers
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    pub fn total_bytes(&self) -> u64 {
        self.servers.iter().map(|r| r.value().bytes_downloaded).sum()
    }
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

    #[test]
    fn test_extract_nzb_password() {
        let (clean, pass) = extract_nzb_password("My.Show.S01E01{{s3cr3t}}.nzb");
        assert_eq!(clean, "My.Show.S01E01.nzb");
        assert_eq!(pass.as_deref(), Some("s3cr3t"));

        let (clean2, pass2) = extract_nzb_password("Plain.Release.nzb");
        assert_eq!(clean2, "Plain.Release.nzb");
        assert_eq!(pass2, None);
    }

    #[test]
    fn test_import_sabnzbd_ini() {
        let ini = r#"
[misc]
api_key = deadbeef1234
port = 9090
download_dir = /data/incomplete
complete_dir = /data/complete
[servers]
[[news.provider.com]]
host = news.provider.com
port = 563
ssl = 1
username = alice
password = secret
connections = 16
enable = 1
"#;
        let cfg = Config::import_sabnzbd_ini(ini);
        assert_eq!(cfg.api_key, "deadbeef1234");
        assert_eq!(cfg.http_port, 9090);
        assert_eq!(cfg.complete_dir, PathBuf::from("/data/complete"));
        assert_eq!(cfg.servers.len(), 1);
        assert_eq!(cfg.servers[0].host, "news.provider.com");
        assert_eq!(cfg.servers[0].connections, 16);
        assert!(cfg.servers[0].ssl);
    }
}
