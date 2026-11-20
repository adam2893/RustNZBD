use bytes::Bytes;
use rustls::pki_types::ServerName;
use sabnzbd_core::ServerConfig;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use tracing::{debug, info, warn};

#[derive(Error, Debug)]
pub enum NntpError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLS error: {0}")]
    Tls(String),
    #[error("NNTP command rejected: {code} {message}")]
    Rejected { code: u32, message: String },
    #[error("Article not found: {0}")]
    ArticleNotFound(String),
    #[error("Authentication failed: {0}")]
    AuthFailed(String),
    #[error("Unexpected response: {0}")]
    UnexpectedResponse(String),
    #[error("No servers configured or enabled")]
    NoServers,
}

enum NntpStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for NntpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            NntpStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            NntpStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for NntpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            NntpStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            NntpStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            NntpStream::Plain(s) => Pin::new(s).poll_flush(cx),
            NntpStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            NntpStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            NntpStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

fn build_tls_connector() -> TlsConnector {
    let root_store = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

pub struct NntpConnection {
    reader: BufReader<ReadHalf<NntpStream>>,
    writer: WriteHalf<NntpStream>,
}

impl NntpConnection {
    async fn connect_via_socks5(
        proxy_addr: &str,
        target_host: &str,
        target_port: u16,
    ) -> Result<TcpStream, NntpError> {
        let mut stream = TcpStream::connect(proxy_addr).await?;
        // Greeting: VER=5, NMETHODS=1, NO AUTH=0
        stream.write_all(&[0x05, 0x01, 0x00]).await?;
        let mut method_resp = [0u8; 2];
        stream.read_exact(&mut method_resp).await?;
        if method_resp != [0x05, 0x00] {
            return Err(NntpError::Io(std::io::Error::other("SOCKS5 auth method rejected")));
        }

        // CONNECT request with domain name (ATYP=0x03)
        let host_bytes = target_host.as_bytes();
        if host_bytes.len() > 255 {
            return Err(NntpError::Io(std::io::Error::other("Target hostname too long for SOCKS5")));
        }
        let mut req = Vec::with_capacity(7 + host_bytes.len());
        req.extend_from_slice(&[0x05, 0x01, 0x00, 0x03, host_bytes.len() as u8]);
        req.extend_from_slice(host_bytes);
        req.extend_from_slice(&target_port.to_be_bytes());
        stream.write_all(&req).await?;

        let mut head = [0u8; 4];
        stream.read_exact(&mut head).await?;
        if head[1] != 0x00 {
            return Err(NntpError::Io(std::io::Error::other(format!(
                "SOCKS5 connect failed with code {}",
                head[1]
            ))));
        }
        match head[3] {
            0x01 => {
                let mut skip = [0u8; 6];
                stream.read_exact(&mut skip).await?;
            }
            0x03 => {
                let mut len = [0u8; 1];
                stream.read_exact(&mut len).await?;
                let mut skip = vec![0u8; len[0] as usize + 2];
                stream.read_exact(&mut skip).await?;
            }
            0x04 => {
                let mut skip = [0u8; 18];
                stream.read_exact(&mut skip).await?;
            }
            _ => {}
        }
        Ok(stream)
    }

    pub async fn connect(config: &ServerConfig) -> Result<Self, NntpError> {
        let addr = format!("{}:{}", config.host, config.port);
        debug!("Connecting to NNTP server at {} (ssl={})", addr, config.ssl);
        let tcp = if let Some(proxy) = config.socks5_proxy.as_deref().filter(|p| !p.trim().is_empty()) {
            Self::connect_via_socks5(proxy.trim(), &config.host, config.port).await?
        } else {
            TcpStream::connect(&addr).await?
        };

        let stream = if config.ssl {
            let connector = build_tls_connector();
            let server_name = ServerName::try_from(config.host.clone())
                .map_err(|e| NntpError::Tls(format!("Invalid DNS name {}: {}", config.host, e)))?;
            let tls = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| NntpError::Tls(e.to_string()))?;
            NntpStream::Tls(Box::new(tls))
        } else {
            NntpStream::Plain(tcp)
        };

        let (read_half, mut writer) = tokio::io::split(stream);
        let mut reader = BufReader::new(read_half);

        let mut banner = String::new();
        reader.read_line(&mut banner).await?;
        let trimmed_banner = banner.trim();
        if !trimmed_banner.starts_with("200") && !trimmed_banner.starts_with("201") {
            return Err(NntpError::UnexpectedResponse(trimmed_banner.to_string()));
        }
        info!("NNTP banner: {}", trimmed_banner);

        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            writer
                .write_all(format!("AUTHINFO USER {}\r\n", user).as_bytes())
                .await?;
            writer.flush().await?;
            let mut res = String::new();
            reader.read_line(&mut res).await?;
            if res.starts_with("381") {
                writer
                    .write_all(format!("AUTHINFO PASS {}\r\n", pass).as_bytes())
                    .await?;
                writer.flush().await?;
                res.clear();
                reader.read_line(&mut res).await?;
                if !res.starts_with("281") {
                    return Err(NntpError::AuthFailed(res.trim().to_string()));
                }
            } else if !res.starts_with("281") {
                return Err(NntpError::AuthFailed(res.trim().to_string()));
            }
        }

        Ok(Self { reader, writer })
    }

    /// Checks if an article exists without transferring its body (RFC 3977 STAT).
    pub async fn stat(&mut self, message_id: &str) -> Result<bool, NntpError> {
        let cmd = format!(
            "STAT <{}>\r\n",
            message_id.trim_matches(|c| c == '<' || c == '>')
        );
        self.writer.write_all(cmd.as_bytes()).await?;
        self.writer.flush().await?;

        let mut line = String::new();
        self.reader.read_line(&mut line).await?;
        let trimmed = line.trim();
        if trimmed.starts_with("223") {
            Ok(true)
        } else if trimmed.starts_with("430") {
            Ok(false)
        } else {
            Err(NntpError::UnexpectedResponse(trimmed.to_string()))
        }
    }

    /// Fetches article body by Message-ID (RFC 3977 BODY command).
    pub async fn fetch_body(&mut self, message_id: &str) -> Result<Bytes, NntpError> {
        let cmd = format!(
            "BODY <{}>\r\n",
            message_id.trim_matches(|c| c == '<' || c == '>')
        );
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

        // Multi-line block ended by `.\r\n` (preserve raw bytes, dot-unstuff leading `..`)
        let mut article_data = Vec::new();
        let mut raw_line = Vec::new();
        loop {
            raw_line.clear();
            let bytes_read = self.reader.read_until(b'\n', &mut raw_line).await?;
            if bytes_read == 0 || raw_line == b".\r\n" || raw_line == b".\n" {
                break;
            }
            let content = if raw_line.starts_with(b"..") {
                &raw_line[1..]
            } else {
                &raw_line[..]
            };
            article_data.extend_from_slice(content);
        }

        Ok(Bytes::from(article_data))
    }

    pub async fn quit(&mut self) -> Result<(), NntpError> {
        let _ = self.writer.write_all(b"QUIT\r\n").await;
        let _ = self.writer.flush().await;
        Ok(())
    }
}

pub struct NntpPool {
    config: ServerConfig,
    idle: Mutex<VecDeque<NntpConnection>>,
    semaphore: Arc<Semaphore>,
}

impl NntpPool {
    pub fn new(config: ServerConfig) -> Self {
        let max_conns = config.connections.max(1);
        Self {
            config,
            idle: Mutex::new(VecDeque::with_capacity(max_conns)),
            semaphore: Arc::new(Semaphore::new(max_conns)),
        }
    }

    pub fn server_name(&self) -> &str {
        &self.config.name
    }

    pub async fn fetch_body(&self, message_id: &str) -> Result<Bytes, NntpError> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| NntpError::Io(std::io::Error::other(e.to_string())))?;

        let mut conn = match self.idle.lock().await.pop_front() {
            Some(c) => c,
            None => NntpConnection::connect(&self.config).await?,
        };

        match conn.fetch_body(message_id).await {
            Ok(bytes) => {
                self.idle.lock().await.push_back(conn);
                Ok(bytes)
            }
            Err(e @ NntpError::ArticleNotFound(_)) => {
                self.idle.lock().await.push_back(conn);
                Err(e)
            }
            Err(first_err) => {
                // Connection may have dropped; reconnect once and retry
                warn!("NNTP fetch failed ({}), reconnecting once", first_err);
                let mut fresh = NntpConnection::connect(&self.config).await?;
                let res = fresh.fetch_body(message_id).await;
                if res.is_ok() || matches!(res, Err(NntpError::ArticleNotFound(_))) {
                    self.idle.lock().await.push_back(fresh);
                }
                res
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_nntp_pool_auth_and_fetch_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (read_half, mut write_half) = socket.into_split();
            let mut reader = BufReader::new(read_half);

            write_half.write_all(b"200 MockNNTP Ready\r\n").await.unwrap();

            let mut line = String::new();
            while reader.read_line(&mut line).await.unwrap() > 0 {
                let cmd = line.trim();
                if cmd == "AUTHINFO USER alice" {
                    write_half.write_all(b"381 PASS required\r\n").await.unwrap();
                } else if cmd == "AUTHINFO PASS secret" {
                    write_half.write_all(b"281 Ok\r\n").await.unwrap();
                } else if cmd == "BODY <msg1@test>" {
                    write_half
                        .write_all(b"222 0 <msg1@test> body\r\n..dot-stuffed\r\nhello\r\n.\r\n")
                        .await
                        .unwrap();
                } else if cmd == "BODY <missing@test>" {
                    write_half.write_all(b"430 No such article\r\n").await.unwrap();
                } else if cmd == "QUIT" {
                    write_half.write_all(b"205 Bye\r\n").await.unwrap();
                    break;
                }
                line.clear();
            }
        });

        let cfg = ServerConfig {
            host: "127.0.0.1".to_string(),
            port,
            ssl: false,
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
            connections: 2,
            enable: true,
            ..Default::default()
        };

        let pool = NntpPool::new(cfg);
        let body = pool.fetch_body("msg1@test").await.unwrap();
        assert_eq!(&body[..], b".dot-stuffed\r\nhello\r\n");

        let err = pool.fetch_body("<missing@test>").await.unwrap_err();
        assert!(matches!(err, NntpError::ArticleNotFound(_)));
    }

    #[tokio::test]
    async fn test_socks5_nntp_tunnel() {
        let socks_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socks_port = socks_listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let (mut sock, _) = socks_listener.accept().await.unwrap();
            let mut hello = [0u8; 3];
            sock.read_exact(&mut hello).await.unwrap();
            assert_eq!(hello, [0x05, 0x01, 0x00]);
            sock.write_all(&[0x05, 0x00]).await.unwrap();

            let mut head = [0u8; 5];
            sock.read_exact(&mut head).await.unwrap();
            let domain_len = head[4] as usize;
            let mut rest = vec![0u8; domain_len + 2];
            sock.read_exact(&mut rest).await.unwrap();
            assert_eq!(&rest[..domain_len], b"news.internal");

            // Reply success (0x01 IPv4 0.0.0.0:0)
            sock.write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0]).await.unwrap();
            sock.write_all(b"200 ProxiedNNTP\r\n").await.unwrap();

            let mut buf = [0u8; 64];
            let n = sock.read(&mut buf).await.unwrap();
            assert!(std::str::from_utf8(&buf[..n]).unwrap().contains("STAT <x@y>"));
            sock.write_all(b"223 0 <x@y> status\r\n").await.unwrap();
        });

        let cfg = ServerConfig {
            host: "news.internal".to_string(),
            port: 119,
            ssl: false,
            socks5_proxy: Some(format!("127.0.0.1:{socks_port}")),
            enable: true,
            ..Default::default()
        };

        let mut conn = NntpConnection::connect(&cfg).await.unwrap();
        assert!(conn.stat("x@y").await.unwrap());
    }
}
