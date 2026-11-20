use chrono::{DateTime, Datelike, Local, Timelike};
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use sabnzbd_core::{NotificationConfig, RssFeedConfig, ScheduleAction, ScheduleRule};
use serde_json::json;
use std::collections::HashSet;
use std::time::Duration;
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
    /// Sends a JSON webhook notification (compatible with Discord/Slack/custom endpoints).
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
        let Some(url) = &cfg.webhook_url else {
            return;
        };
        if !should_send || url.trim().is_empty() {
            return;
        }

        let status = if completed { "Completed" } else { "Failed" };
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
}
