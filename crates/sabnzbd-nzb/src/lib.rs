use quick_xml::events::Event;
use quick_xml::reader::Reader;
use sabnzbd_core::{JobStatus, QueueItem};
use std::collections::VecDeque;
use std::sync::Arc;
use parking_lot::RwLock;
use thiserror::Error;
use uuid::Uuid;
use chrono::Utc;

#[derive(Error, Debug)]
pub enum NzbError {
    #[error("XML parsing error: {0}")]
    Xml(#[from] quick_xml::Error),
    #[error("Invalid NZB: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone)]
pub struct NzbSegment {
    pub bytes: u64,
    pub number: u32,
    pub message_id: String,
}

#[derive(Debug, Clone)]
pub struct NzbFile {
    pub poster: String,
    pub date: u64,
    pub subject: String,
    pub groups: Vec<String>,
    pub segments: Vec<NzbSegment>,
}

#[derive(Debug, Clone)]
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

pub struct DownloadQueue {
    items: Arc<RwLock<VecDeque<QueueItem>>>,
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
        }
    }

    pub fn add_nzb(&self, filename: String, total_bytes: u64, category: String) -> QueueItem {
        let item = QueueItem {
            id: format!("SABnzbd_nzo_{}", &Uuid::new_v4().to_string().replace("-", "")[..10]),
            filename,
            total_bytes,
            downloaded_bytes: 0,
            status: JobStatus::Queued,
            priority: 0,
            category,
            eta_seconds: None,
            speed_bps: 0,
            time_added: Utc::now(),
        };

        self.items.write().push_back(item.clone());
        item
    }

    pub fn list(&self) -> Vec<QueueItem> {
        self.items.read().iter().cloned().collect()
    }

    pub fn pause(&self) {
        for item in self.items.write().iter_mut() {
            if item.status == JobStatus::Downloading || item.status == JobStatus::Queued {
                item.status = JobStatus::Paused;
            }
        }
    }

    pub fn resume(&self) {
        for item in self.items.write().iter_mut() {
            if item.status == JobStatus::Paused {
                item.status = JobStatus::Queued;
            }
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
}
