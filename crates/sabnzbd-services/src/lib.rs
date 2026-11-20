use chrono::{DateTime, Datelike, Local, Timelike};
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use sabnzbd_core::{NotificationConfig, RssFeedConfig, ScheduleAction, ScheduleRule};
use serde_json::json;
use std::collections::HashSet;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{info, warn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RssItem {
    pub title: String,
    pub link: String,
}

/// Parses RSS 2.0 `<item>` entries (`<title>`, `<link>`, or `<enclosure url="...">`).
pub fn parse_rss_feed(xml: &str) -> Vec<RssItem> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut items = Vec::new();
    let mut in_item = false;
    let mut current_tag = String::new();
    let mut title = String::new();
    let mut link = String::new();
    let mut enclosure_url = String::new();
    let mut buf = Vec::new();

    while let Ok(ev) = reader.read_event_into(&mut buf) {
        match ev {
            Event::Start(ref e) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if tag == "item" {
                    in_item = true;
                    title.clear();
                    link.clear();
                    enclosure_url.clear();
                } else if in_item {
                    if tag == "enclosure" {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"url" {
                                enclosure_url = String::from_utf8_lossy(&attr.value).to_string();
                            }
                        }
                    }
                    current_tag = tag;
                }
            }
            Event::Empty(ref e) if in_item && e.name().as_ref() == b"enclosure" => {
                for attr in e.attributes().flatten() {
                    if attr.key.as_ref() == b"url" {
                        enclosure_url = String::from_utf8_lossy(&attr.value).to_string();
                    }
                }
            }
            Event::Text(e) if in_item => {
                if let Ok(text) = e.unescape() {
                    match current_tag.as_str() {
                        "title" => title.push_str(&text),
                        "link" => link.push_str(&text),
                        _ => {}
                    }
                }
            }
            Event::End(ref e) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if tag == "item" && in_item {
                    let final_link = if !enclosure_url.is_empty() {
                        enclosure_url.clone()
                    } else {
                        link.clone()
                    };
                    if !title.is_empty() && !final_link.is_empty() {
                        items.push(RssItem {
                            title: title.trim().to_string(),
                            link: final_link.trim().to_string(),
                        });
                    }
                    in_item = false;
                }
                current_tag.clear();
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    items
}

/// Evaluates SABnzbd-style accept/reject case-insensitive substring filters.
pub fn matches_filters(title: &str, accept: &[String], reject: &[String]) -> bool {
    let lower = title.to_ascii_lowercase();
    for r in reject {
        let pat = r.trim().to_ascii_lowercase();
        if !pat.is_empty() && lower.contains(&pat) {
            return false;
        }
    }
    if accept.is_empty() {
        return true;
    }
    accept.iter().any(|a| {
        let pat = a.trim().to_ascii_lowercase();
        !pat.is_empty() && lower.contains(&pat)
    })
}

pub struct RssPoller {
    seen: HashSet<String>,
}

impl Default for RssPoller {
    fn default() -> Self {
        Self::new()
    }
}

impl RssPoller {
    pub fn new() -> Self {
        Self {
            seen: HashSet::new(),
        }
    }

    /// Fetches a feed and returns newly matched `(title, nzb_url)` pairs.
    pub async fn poll_feed(&mut self, feed: &RssFeedConfig) -> Vec<RssItem> {
        if !feed.enabled || feed.uri.is_empty() {
            return Vec::new();
        }
        let Ok(client) = reqwest::Client::builder().timeout(Duration::from_secs(20)).build() else {
            return Vec::new();
        };
        let Ok(resp) = client.get(&feed.uri).send().await else {
            warn!("RSS fetch failed for {}", feed.name);
            return Vec::new();
        };
        let Ok(xml) = resp.text().await else {
            return Vec::new();
        };

        let mut matched = Vec::new();
        for item in parse_rss_feed(&xml) {
            if self.seen.contains(&item.link) {
                continue;
            }
            self.seen.insert(item.link.clone());
            if matches_filters(&item.title, &feed.accept_filters, &feed.reject_filters) {
                info!("RSS '{}' matched: {}", feed.name, item.title);
                matched.push(item);
            }
        }
        matched
    }
}

#[derive(Default)]
pub struct Scheduler {
    last_minute_key: Option<(i32, u32, u32, u32)>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns all actions due at `now`, firing at most once per minute.
    pub fn due_actions(&mut self, rules: &[ScheduleRule], now: DateTime<Local>) -> Vec<ScheduleAction> {
        let minute_key = (now.year(), now.ordinal(), now.hour(), now.minute());
        if self.last_minute_key == Some(minute_key) {
            return Vec::new();
        }
        self.last_minute_key = Some(minute_key);

        let weekday = now.weekday().number_from_monday() as u8;
        let hour = now.hour() as u8;
        let minute = now.minute() as u8;

        rules
            .iter()
            .filter(|r| {
                r.enabled
                    && r.hour == hour
                    && r.minute == minute
                    && (r.days.is_empty() || r.days.contains(&weekday))
            })
            .map(|r| r.action.clone())
            .collect()
    }
}

pub struct Notifier;

impl Notifier {
    /// Dispatches both webhook and SMTP email notifications if configured.
    pub async fn notify(
        cfg: &NotificationConfig,
        completed: bool,
        job_name: &str,
        category: &str,
        detail: Option<&str>,
    ) {
        let should_send = if completed {
            cfg.notify_on_complete
        } else {
            cfg.notify_on_fail
        };
        if !should_send {
            return;
        }

        let status = if completed { "Completed" } else { "Failed" };

        if let Some(url) = cfg.webhook_url.as_deref().filter(|u| !u.trim().is_empty()) {
            let payload = json!({
                "content": format!("RustNZBD: {job_name} [{category}] -> {status}"),
                "event": status,
                "name": job_name,
                "category": category,
                "detail": detail.unwrap_or("")
            });

            if let Ok(client) = reqwest::Client::builder().timeout(Duration::from_secs(10)).build() {
                if let Err(e) = client
                    .post(url)
                    .header("Content-Type", "application/json")
                    .body(payload.to_string())
                    .send()
                    .await
                {
                    warn!("Webhook notification failed: {}", e);
                }
            }
        }

        if cfg.smtp_host.as_deref().map(|s| !s.trim().is_empty()).unwrap_or(false) {
            if let Err(e) = Self::send_smtp(cfg, status, job_name, category, detail).await {
                warn!("SMTP notification failed: {}", e);
            }
        }
    }

    pub async fn send_smtp(
        cfg: &NotificationConfig,
        status: &str,
        job_name: &str,
        category: &str,
        detail: Option<&str>,
    ) -> Result<(), String> {
        let host = cfg.smtp_host.as_deref().ok_or("Missing smtp_host")?;
        let from = cfg.smtp_from.as_deref().ok_or("Missing smtp_from")?;
        let to = cfg.smtp_to.as_deref().ok_or("Missing smtp_to")?;

        let stream = TcpStream::connect(format!("{host}:{}", cfg.smtp_port))
            .await
            .map_err(|e| e.to_string())?;
        let (r, mut w) = stream.into_split();
        let mut reader = BufReader::new(r);

        Self::expect_smtp(&mut reader, "220").await?;
        w.write_all(b"EHLO rustnzbd\r\n").await.map_err(|e| e.to_string())?;
        Self::expect_smtp(&mut reader, "250").await?;

        if let (Some(user), Some(pass)) = (&cfg.smtp_user, &cfg.smtp_password) {
            if !user.is_empty() {
                w.write_all(b"AUTH LOGIN\r\n").await.map_err(|e| e.to_string())?;
                Self::expect_smtp(&mut reader, "334").await?;
                let u_b64 = Self::b64(user.as_bytes());
                w.write_all(format!("{u_b64}\r\n").as_bytes()).await.map_err(|e| e.to_string())?;
                Self::expect_smtp(&mut reader, "334").await?;
                let p_b64 = Self::b64(pass.as_bytes());
                w.write_all(format!("{p_b64}\r\n").as_bytes()).await.map_err(|e| e.to_string())?;
                Self::expect_smtp(&mut reader, "235").await?;
            }
        }

        w.write_all(format!("MAIL FROM:<{from}>\r\n").as_bytes()).await.map_err(|e| e.to_string())?;
        Self::expect_smtp(&mut reader, "250").await?;
        w.write_all(format!("RCPT TO:<{to}>\r\n").as_bytes()).await.map_err(|e| e.to_string())?;
        Self::expect_smtp(&mut reader, "250").await?;
        w.write_all(b"DATA\r\n").await.map_err(|e| e.to_string())?;
        Self::expect_smtp(&mut reader, "354").await?;

        let body = format!(
            "From: <{from}>\r\nTo: <{to}>\r\nSubject: RustNZBD: {job_name} [{status}]\r\n\r\nJob: {job_name}\r\nCategory: {category}\r\nStatus: {status}\r\nDetail: {}\r\n.\r\n",
            detail.unwrap_or("")
        );
        w.write_all(body.as_bytes()).await.map_err(|e| e.to_string())?;
        Self::expect_smtp(&mut reader, "250").await?;
        let _ = w.write_all(b"QUIT\r\n").await;
        Ok(())
    }

    async fn expect_smtp(
        reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
        code: &str,
    ) -> Result<(), String> {
        let mut line = String::new();
        loop {
            line.clear();
            let n = reader.read_line(&mut line).await.map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("Unexpected EOF from SMTP server".to_string());
            }
            if !line.starts_with(code) {
                return Err(format!("SMTP expected {code}, got: {}", line.trim()));
            }
            // Multi-line SMTP replies have '-' at index 3 (e.g. "250-...")
            if line.as_bytes().get(3) != Some(&b'-') {
                return Ok(());
            }
        }
    }

    fn b64(input: &[u8]) -> String {
        const TBL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in input.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
            let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
            let n = (b0 << 16) | (b1 << 8) | b2;
            out.push(TBL[((n >> 18) & 63) as usize] as char);
            out.push(TBL[((n >> 12) & 63) as usize] as char);
            out.push(if chunk.len() > 1 { TBL[((n >> 6) & 63) as usize] as char } else { '=' });
            out.push(if chunk.len() > 2 { TBL[(n & 63) as usize] as char } else { '=' });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_rss_parse_and_filter() {
        let rss = r#"<?xml version="1.0"?>
        <rss version="2.0"><channel>
            <item>
                <title>Ubuntu.24.04.LTS.1080p</title>
                <link>https://idx.example/get/1</link>
            </item>
            <item>
                <title>Ubuntu.24.04.CAM</title>
                <enclosure url="https://idx.example/get/2" type="application/x-nzb" />
            </item>
        </channel></rss>"#;

        let items = parse_rss_feed(rss);
        assert_eq!(items.len(), 2);
        assert_eq!(items[1].link, "https://idx.example/get/2");

        let accept = vec!["ubuntu".to_string()];
        let reject = vec!["cam".to_string()];
        assert!(matches_filters(&items[0].title, &accept, &reject));
        assert!(!matches_filters(&items[1].title, &accept, &reject));
    }

    #[test]
    fn test_scheduler_fires_once_per_minute() {
        let mut sched = Scheduler::new();
        let rules = vec![ScheduleRule {
            enabled: true,
            hour: 23,
            minute: 30,
            days: vec![],
            action: ScheduleAction::SpeedLimit(Some(500 * 1024)),
        }];

        let dt = Local.with_ymd_and_hms(2026, 11, 20, 23, 30, 5).unwrap();
        let first = sched.due_actions(&rules, dt);
        assert_eq!(first, vec![ScheduleAction::SpeedLimit(Some(500 * 1024))]);

        let same_min = Local.with_ymd_and_hms(2026, 11, 20, 23, 30, 45).unwrap();
        assert!(sched.due_actions(&rules, same_min).is_empty());
    }

    #[tokio::test]
    async fn test_smtp_email_notification() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let (r, mut w) = sock.into_split();
            let mut reader = BufReader::new(r);
            w.write_all(b"220 mock.smtp Ready\r\n").await.unwrap();
            let mut line = String::new();
            let mut in_data = false;
            while reader.read_line(&mut line).await.unwrap() > 0 {
                let cmd = line.trim();
                if in_data {
                    if cmd == "." {
                        in_data = false;
                        w.write_all(b"250 Queued\r\n").await.unwrap();
                    }
                } else if cmd.starts_with("EHLO") || cmd.starts_with("MAIL FROM") || cmd.starts_with("RCPT TO") {
                    w.write_all(b"250 Ok\r\n").await.unwrap();
                } else if cmd == "DATA" {
                    in_data = true;
                    w.write_all(b"354 End data with .\r\n").await.unwrap();
                } else if cmd == "QUIT" {
                    w.write_all(b"221 Bye\r\n").await.unwrap();
                    break;
                }
                line.clear();
            }
        });

        let cfg = NotificationConfig {
            smtp_host: Some("127.0.0.1".to_string()),
            smtp_port: port,
            smtp_from: Some("rustnzbd@example.com".to_string()),
            smtp_to: Some("adam@example.com".to_string()),
            ..Default::default()
        };

        Notifier::send_smtp(&cfg, "Completed", "ubuntu.nzb", "linux", None)
            .await
            .unwrap();
    }
}
