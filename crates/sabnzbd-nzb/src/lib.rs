use chrono::Utc;
use parking_lot::RwLock;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use sabnzbd_core::{HistoryItem, JobStatus, QueueItem};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum NzbError {
    #[error("XML parsing error: {0}")]
    Xml(#[from] quick_xml::Error),
    #[error("Invalid NZB: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NzbSegment {
    pub bytes: u64,
    pub number: u32,
    pub message_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NzbFile {
    pub poster: String,
    pub date: u64,
    pub subject: String,
    pub groups: Vec<String>,
    pub segments: Vec<NzbSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NzbDocument {
    pub files: Vec<NzbFile>,
}

impl NzbDocument {
    pub fn parse(xml: &str) -> Result<Self, NzbError> {
        let mut reader = Reader::from_str(xml);
        reader.config_mut().trim_text(true);

        let mut files = Vec::new();
        let mut current_file: Option<NzbFile> = None;
        let mut current_segment_bytes: u64 = 0;
        let mut current_segment_number: u32 = 0;
        let mut current_text = String::new();

        let mut buf = Vec::new();
        loop {
            match reader.read_event_into(&mut buf)? {
                Event::Start(ref e) => match e.name().as_ref() {
                    b"file" => {
                        let mut poster = String::new();
                        let mut date = 0;
                        let mut subject = String::new();
                        for attr in e.attributes().flatten() {
                            match attr.key.as_ref() {
                                b"poster" => poster = String::from_utf8_lossy(&attr.value).to_string(),
                                b"date" => date = String::from_utf8_lossy(&attr.value).parse().unwrap_or(0),
                                b"subject" => subject = String::from_utf8_lossy(&attr.value).to_string(),
                                _ => {}
                            }
                        }
                        current_file = Some(NzbFile {
                            poster,
                            date,
                            subject,
                            groups: Vec::new(),
                            segments: Vec::new(),
                        });
                    }
                    b"segment" => {
                        for attr in e.attributes().flatten() {
                            match attr.key.as_ref() {
                                b"bytes" => current_segment_bytes = String::from_utf8_lossy(&attr.value).parse().unwrap_or(0),
                                b"number" => current_segment_number = String::from_utf8_lossy(&attr.value).parse().unwrap_or(0),
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                },
                Event::Text(e) => {
                    current_text = e.unescape()?.into_owned();
                }
                Event::End(ref e) => match e.name().as_ref() {
                    b"group" => {
                        if let Some(file) = &mut current_file {
                            file.groups.push(current_text.clone());
                        }
                    }
                    b"segment" => {
                        if let Some(file) = &mut current_file {
                            file.segments.push(NzbSegment {
                                bytes: current_segment_bytes,
                                number: current_segment_number,
                                message_id: current_text.clone(),
                            });
                        }
                    }
                    b"file" => {
                        if let Some(file) = current_file.take() {
                            files.push(file);
                        }
                    }
                    _ => {}
                },
                Event::Eof => break,
                _ => {}
            }
            buf.clear();
        }

        Ok(NzbDocument { files })
    }

    pub fn total_bytes(&self) -> u64 {
        self.files.iter().flat_map(|f| &f.segments).map(|s| s.bytes).sum()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueuedEntry {
    item: QueueItem,
    document: Option<NzbDocument>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedState {
    items: Vec<QueuedEntry>,
    history: Vec<HistoryItem>,
    paused: bool,
}

pub struct DownloadQueue {
    items: Arc<RwLock<VecDeque<QueuedEntry>>>,
    history: Arc<RwLock<Vec<HistoryItem>>>,
    paused: Arc<RwLock<bool>>,
    state_path: Option<PathBuf>,
}

impl Default for DownloadQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl DownloadQueue {
    pub fn new() -> Self {
        Self {
            items: Arc::new(RwLock::new(VecDeque::new())),
            history: Arc::new(RwLock::new(Vec::new())),
            paused: Arc::new(RwLock::new(false)),
            state_path: None,
        }
    }

    /// Creates a persistent queue backed by a JSON state file, recovering any in-flight jobs to Queued.
    pub fn new_persistent(path: PathBuf) -> Self {
        if path.exists() {
            match std::fs::read_to_string(&path)
                .ok()
                .and_then(|s| serde_json::from_str::<PersistedState>(&s).ok())
            {
                Some(mut state) => {
                    for entry in &mut state.items {
                        if matches!(
                            entry.item.status,
                            JobStatus::Downloading
                                | JobStatus::Checking
                                | JobStatus::Repairing
                                | JobStatus::Extracting
                        ) {
                            entry.item.status = if state.paused {
                                JobStatus::Paused
                            } else {
                                JobStatus::Queued
                            };
                            entry.item.downloaded_bytes = 0;
                            entry.item.speed_bps = 0;
                        }
                    }
                    return Self {
                        items: Arc::new(RwLock::new(VecDeque::from(state.items))),
                        history: Arc::new(RwLock::new(state.history)),
                        paused: Arc::new(RwLock::new(state.paused)),
                        state_path: Some(path),
                    };
                }
                None => {
                    warn!("Failed to load queue state from {:?}, starting fresh", path);
                }
            }
        }
        Self {
            items: Arc::new(RwLock::new(VecDeque::new())),
            history: Arc::new(RwLock::new(Vec::new())),
            paused: Arc::new(RwLock::new(false)),
            state_path: Some(path),
        }
    }

    fn persist(&self) {
        let Some(path) = &self.state_path else {
            return;
        };
        let state = PersistedState {
            items: self.items.read().iter().cloned().collect(),
            history: self.history.read().clone(),
            paused: *self.paused.read(),
        };
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        if let Ok(json) = serde_json::to_string(&state) {
            let tmp = path.with_extension("tmp");
            if std::fs::write(&tmp, json).is_ok() {
                let _ = std::fs::rename(&tmp, path);
            }
        }
    }

    pub fn add_nzb(&self, filename: String, total_bytes: u64, category: String) -> QueueItem {
        self.add_nzb_with_doc(filename, total_bytes, category, None)
    }

    pub fn add_nzb_with_doc(
        &self,
        filename: String,
        total_bytes: u64,
        category: String,
        document: Option<NzbDocument>,
    ) -> QueueItem {
        let status = if *self.paused.read() {
            JobStatus::Paused
        } else {
            JobStatus::Queued
        };
        let item = QueueItem {
            id: format!("SABnzbd_nzo_{}", &Uuid::new_v4().to_string().replace('-', "")[..10]),
            filename,
            total_bytes,
            downloaded_bytes: 0,
            status,
            priority: 0,
            category,
            eta_seconds: None,
            speed_bps: 0,
            time_added: Utc::now(),
        };

        self.items.write().push_back(QueuedEntry {
            item: item.clone(),
            document,
        });
        self.persist();
        item
    }

    pub fn list(&self) -> Vec<QueueItem> {
        self.items.read().iter().map(|e| e.item.clone()).collect()
    }

    pub fn history(&self) -> Vec<HistoryItem> {
        self.history.read().clone()
    }

    pub fn is_paused(&self) -> bool {
        *self.paused.read()
    }

    pub fn pause(&self) {
        *self.paused.write() = true;
        for entry in self.items.write().iter_mut() {
            if entry.item.status == JobStatus::Downloading || entry.item.status == JobStatus::Queued {
                entry.item.status = JobStatus::Paused;
            }
        }
        self.persist();
    }

    pub fn resume(&self) {
        *self.paused.write() = false;
        for entry in self.items.write().iter_mut() {
            if entry.item.status == JobStatus::Paused {
                entry.item.status = JobStatus::Queued;
            }
        }
        self.persist();
    }

    pub fn delete_queue_item(&self, id: &str) -> bool {
        let deleted = {
            let mut items = self.items.write();
            let before = items.len();
            items.retain(|e| e.item.id != id);
            items.len() < before
        };
        if deleted {
            self.persist();
        }
        deleted
    }

    pub fn delete_history_item(&self, id: &str) -> bool {
        let deleted = {
            let mut hist = self.history.write();
            let before = hist.len();
            hist.retain(|h| h.id != id);
            hist.len() < before
        };
        if deleted {
            self.persist();
        }
        deleted
    }

    pub fn pause_item(&self, id: &str) -> bool {
        let mut changed = false;
        for entry in self.items.write().iter_mut() {
            if entry.item.id == id && entry.item.status == JobStatus::Queued {
                entry.item.status = JobStatus::Paused;
                changed = true;
                break;
            }
        }
        if changed {
            self.persist();
        }
        changed
    }

    pub fn resume_item(&self, id: &str) -> bool {
        let mut changed = false;
        for entry in self.items.write().iter_mut() {
            if entry.item.id == id && entry.item.status == JobStatus::Paused {
                entry.item.status = JobStatus::Queued;
                changed = true;
                break;
            }
        }
        if changed {
            self.persist();
        }
        changed
    }

    /// Claims the highest-priority queued job with an attached NzbDocument and marks it Downloading.
    pub fn claim_next_job(&self) -> Option<(QueueItem, NzbDocument)> {
        if *self.paused.read() {
            return None;
        }
        let mut items = self.items.write();
        let best_idx = items
            .iter()
            .enumerate()
            .filter(|(_, e)| e.item.status == JobStatus::Queued && e.document.is_some())
            .max_by_key(|(_, e)| e.item.priority)
            .map(|(idx, _)| idx);

        if let Some(idx) = best_idx {
            let entry = &mut items[idx];
            entry.item.status = JobStatus::Downloading;
            return Some((entry.item.clone(), entry.document.clone().unwrap()));
        }
        None
    }

    pub fn add_downloaded_bytes(&self, id: &str, bytes: u64, speed_bps: u64) {
        for entry in self.items.write().iter_mut() {
            if entry.item.id == id {
                entry.item.downloaded_bytes = entry.item.downloaded_bytes.saturating_add(bytes);
                entry.item.speed_bps = speed_bps;
                break;
            }
        }
    }

    pub fn set_status(&self, id: &str, status: JobStatus) {
        for entry in self.items.write().iter_mut() {
            if entry.item.id == id {
                entry.item.status = status;
                break;
            }
        }
    }

    /// Moves a job from the active queue into history.
    pub fn finish_job(&self, id: &str, storage_path: PathBuf, failure_reason: Option<String>) {
        let mut moved = false;
        {
            let mut items = self.items.write();
            if let Some(pos) = items.iter().position(|e| e.item.id == id) {
                let entry = items.remove(pos).unwrap();
                let status = if failure_reason.is_some() {
                    "Failed".to_string()
                } else {
                    "Completed".to_string()
                };
                self.history.write().insert(
                    0,
                    HistoryItem {
                        id: entry.item.id,
                        name: entry.item.filename,
                        size: entry.item.total_bytes.max(entry.item.downloaded_bytes),
                        category: entry.item.category,
                        status,
                        completed_time: Utc::now(),
                        storage_path,
                        failure_reason,
                    },
                );
                moved = true;
            }
        }
        if moved {
            self.persist();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nzb_parse() {
        let xml = r#"<?xml version="1.0" encoding="utf-8" ?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
 <file poster="poster@example.com" date="1609459200" subject="Ubuntu-24.04-LTS [01/10] - &quot;ubuntu.iso&quot; yEnc">
  <groups>
   <group>alt.binaries.test</group>
  </groups>
  <segments>
   <segment bytes="500000" number="1">msg1@news.example.com</segment>
   <segment bytes="500000" number="2">msg2@news.example.com</segment>
  </segments>
 </file>
</nzb>"#;

        let doc = NzbDocument::parse(xml).expect("Should parse NZB successfully");
        assert_eq!(doc.files.len(), 1);
        assert_eq!(doc.files[0].poster, "poster@example.com");
        assert_eq!(doc.files[0].groups, vec!["alt.binaries.test"]);
        assert_eq!(doc.files[0].segments.len(), 2);
        assert_eq!(doc.total_bytes(), 1_000_000);
    }

    #[test]
    fn test_queue_lifecycle() {
        let q = DownloadQueue::new();
        let doc = NzbDocument { files: vec![] };
        let item = q.add_nzb_with_doc("test.nzb".to_string(), 1024, "tv".to_string(), Some(doc));
        assert_eq!(q.list().len(), 1);

        q.pause();
        assert!(q.claim_next_job().is_none());
        q.resume();

        let (claimed, _) = q.claim_next_job().expect("Should claim queued job");
        assert_eq!(claimed.id, item.id);

        q.add_downloaded_bytes(&item.id, 512, 1000);
        assert_eq!(q.list()[0].downloaded_bytes, 512);

        q.finish_job(&item.id, PathBuf::from("/tmp/done"), None);
        assert!(q.list().is_empty());
        assert_eq!(q.history().len(), 1);
        assert_eq!(q.history()[0].status, "Completed");
    }

    #[test]
    fn test_queue_persistence_and_crash_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("state.json");

        {
            let q = DownloadQueue::new_persistent(state_path.clone());
            let doc = NzbDocument { files: vec![] };
            let item = q.add_nzb_with_doc("persist.nzb".to_string(), 2048, "movies".to_string(), Some(doc));
            let _ = q.claim_next_job();
            q.finish_job(&item.id, PathBuf::from("/tmp/movies/persist"), None);

            let doc2 = NzbDocument { files: vec![] };
            let item2 = q.add_nzb_with_doc("inflight.nzb".to_string(), 4096, "tv".to_string(), Some(doc2));
            let _ = q.claim_next_job();
            // Simulate crash while Downloading (force persist via pause/resume or status)
            q.set_status(&item2.id, JobStatus::Downloading);
            q.persist();
        }

        let recovered = DownloadQueue::new_persistent(state_path);
        assert_eq!(recovered.history().len(), 1);
        assert_eq!(recovered.history()[0].name, "persist.nzb");
        assert_eq!(recovered.list().len(), 1);
        assert_eq!(recovered.list()[0].filename, "inflight.nzb");
        assert_eq!(recovered.list()[0].status, JobStatus::Queued);
    }
}
