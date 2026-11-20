use bytes::Bytes;
use sabnzbd_core::ServerConfig;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, info};

#[derive(Error, Debug)]
pub enum NntpError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("NNTP command rejected: {code} {message}")]
    Rejected { code: u32, message: String },
    #[error("Article not found: {0}")]
    ArticleNotFound(String),
    #[error("Authentication failed: {0}")]
    AuthFailed(String),
    #[error("Unexpected response: {0}")]
    UnexpectedResponse(String),
}

#[allow(dead_code)]
pub struct NntpConnection {
    config: ServerConfig,
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl NntpConnection {
    pub async fn connect(config: ServerConfig) -> Result<Self, NntpError> {
        let addr = format!("{}:{}", config.host, config.port);
        debug!("Connecting to NNTP server at {}", addr);
        let stream = TcpStream::connect(&addr).await?;
        let (read_half, mut writer) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        let mut banner = String::new();
        reader.read_line(&mut banner).await?;
        info!("NNTP banner: {}", banner.trim());

        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            writer.write_all(format!("AUTHINFO USER {}\r\n", user).as_bytes()).await?;
            let mut res = String::new();
            reader.read_line(&mut res).await?;
            if res.starts_with("381") {
                writer.write_all(format!("AUTHINFO PASS {}\r\n", pass).as_bytes()).await?;
                res.clear();
                reader.read_line(&mut res).await?;
                if !res.starts_with("281") {
                    return Err(NntpError::AuthFailed(res));
                }
            }
        }

        Ok(Self {
            config,
            reader,
            writer,
        })
    }

    /// Fetches article body by Message-ID (RFC 3977 BODY command)
    pub async fn fetch_body(&mut self, message_id: &str) -> Result<Bytes, NntpError> {
        let cmd = format!("BODY <{}>\r\n", message_id.trim_matches(|c| c == '<' || c == '>'));
        self.writer.write_all(cmd.as_bytes()).await?;
        self.writer.flush().await?;

        let mut line = String::new();
        self.reader.read_line(&mut line).await?;
        let trimmed = line.trim();

        if trimmed.starts_with("430") {
            return Err(NntpError::ArticleNotFound(message_id.to_string()));
        }
        if !trimmed.starts_with("222") {
            return Err(NntpError::UnexpectedResponse(trimmed.to_string()));
        }

        // Multi-line block ended by `.\r\n`
        let mut article_data = Vec::new();
        loop {
            line.clear();
            let bytes_read = self.reader.read_line(&mut line).await?;
            if bytes_read == 0 || line == ".\r\n" || line == ".\n" {
                break;
            }
            // Dot un-stuffing
            let content = if line.starts_with("..") {
                &line[1..]
            } else {
                &line[..]
            };
            article_data.extend_from_slice(content.as_bytes());
        }

        Ok(Bytes::from(article_data))
    }
}

#[allow(dead_code)]
pub struct NntpPool {
    config: ServerConfig,
    connections: Vec<Arc<Mutex<NntpConnection>>>,
}

impl NntpPool {
    pub async fn new(config: ServerConfig) -> Result<Self, NntpError> {
        let mut connections = Vec::new();
        for _ in 0..config.connections.min(1) { // lazy warm
            let conn = NntpConnection::connect(config.clone()).await?;
            connections.push(Arc::new(Mutex::new(conn)));
        }
        Ok(Self {
            config,
            connections,
        })
    }
}
