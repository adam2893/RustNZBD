use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("Missing =ybegin header")]
    MissingHeader,
    #[error("Corrupt yEnc stream")]
    CorruptedData,
    #[error("CRC32 mismatch: expected {expected:08x}, got {actual:08x}")]
    CrcMismatch { expected: u32, actual: u32 },
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Default)]
pub struct YencHeader {
    pub name: String,
    pub line: usize,
    pub size: usize,
    pub part: Option<usize>,
    pub total: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct DecodedArticle {
    pub header: YencHeader,
    pub part_offset: usize,
    pub part_size: usize,
    pub data: Vec<u8>,
    pub crc32: u32,
}

pub struct YencDecoder;

impl YencDecoder {
    /// Decodes raw yEnc bytes into unescaped payload with CRC validation.
    pub fn decode(raw: &[u8]) -> Result<DecodedArticle, DecodeError> {
        // 1. Locate =ybegin
        let begin_pos = Self::find_subslice(raw, b"=ybegin")
            .ok_or(DecodeError::MissingHeader)?;

        let after_begin = &raw[begin_pos..];
        let first_nl = after_begin
            .iter()
            .position(|&b| b == b'\n')
            .unwrap_or(after_begin.len());
        let ybegin_line = String::from_utf8_lossy(&after_begin[..first_nl]);
        let mut header = YencHeader::default();
        Self::parse_ybegin(&ybegin_line, &mut header);

        let mut rest = if first_nl < after_begin.len() {
            &after_begin[first_nl + 1..]
        } else {
            &[]
        };

        // Check for optional =ypart
        let mut part_offset = 0;
        let mut part_size = header.size;
        let trimmed_rest = Self::trim_start_newlines(rest);
        if trimmed_rest.starts_with(b"=ypart ") {
            let part_nl = trimmed_rest
                .iter()
                .position(|&b| b == b'\n')
                .unwrap_or(trimmed_rest.len());
            let ypart_line = String::from_utf8_lossy(&trimmed_rest[..part_nl]);
            Self::parse_ypart(&ypart_line, &mut part_offset, &mut part_size);
            rest = if part_nl < trimmed_rest.len() {
                &trimmed_rest[part_nl + 1..]
            } else {
                &[]
            };
        }

        // Find =yend
        let (body_bytes, trailer) = Self::split_at_yend(rest);
        let expected_crc = trailer.as_deref().and_then(Self::extract_crc);

        // Fast byte-level unescape:
        // Regular byte: (b - 42) % 256
        // Escaped byte after '=': (next - 42 - 64) % 256
        let mut output = Vec::with_capacity(body_bytes.len());
        let mut i = 0;
        while i < body_bytes.len() {
            let b = body_bytes[i];
            if b == b'\r' || b == b'\n' {
                i += 1;
                continue;
            }
            if b == b'=' {
                i += 1;
                if i < body_bytes.len() {
                    let next = body_bytes[i];
                    output.push(next.wrapping_sub(42).wrapping_sub(64));
                }
            } else {
                output.push(b.wrapping_sub(42));
            }
            i += 1;
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&output);
        let actual_crc = hasher.finalize();

        if let Some(expected) = expected_crc {
            if actual_crc != expected {
                return Err(DecodeError::CrcMismatch {
                    expected,
                    actual: actual_crc,
                });
            }
        }

        Ok(DecodedArticle {
            header,
            part_offset,
            part_size,
            data: output,
            crc32: actual_crc,
        })
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|window| window == needle)
    }

    fn trim_start_newlines(bytes: &[u8]) -> &[u8] {
        let mut start = 0;
        while start < bytes.len() && (bytes[start] == b'\r' || bytes[start] == b'\n') {
            start += 1;
        }
        &bytes[start..]
    }

    fn split_at_yend(bytes: &[u8]) -> (&[u8], Option<String>) {
        if let Some(pos) = Self::find_subslice(bytes, b"=yend") {
            let body = &bytes[..pos];
            let trailer = String::from_utf8_lossy(&bytes[pos..]).to_string();
            (body, Some(trailer))
        } else {
            (bytes, None)
        }
    }

    fn parse_ybegin(line: &str, header: &mut YencHeader) {
        // `name=` is always the final field in yEnc =ybegin and may contain spaces.
        let (head, name_part) = if let Some(idx) = line.find(" name=") {
            (&line[..idx], Some(line[idx + 6..].trim()))
        } else {
            (line, None)
        };

        if let Some(name) = name_part {
            header.name = name.to_string();
        }

        for token in head.split_whitespace() {
            if let Some((k, v)) = token.split_once('=') {
                match k {
                    "line" => header.line = v.parse().unwrap_or(128),
                    "size" => header.size = v.parse().unwrap_or(0),
                    "part" => header.part = v.parse().ok(),
                    "total" => header.total = v.parse().ok(),
                    _ => {}
                }
            }
        }
    }

    fn parse_ypart(line: &str, offset: &mut usize, size: &mut usize) {
        for token in line.split_whitespace() {
            if let Some((k, v)) = token.split_once('=') {
                match k {
                    "begin" => *offset = v.parse::<usize>().unwrap_or(1).saturating_sub(1),
                    "end" => {
                        let end = v.parse::<usize>().unwrap_or(0);
                        if end > *offset {
                            *size = end - *offset;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    fn extract_crc(trailer: &str) -> Option<u32> {
        let mut full_crc = None;
        for token in trailer.split_whitespace() {
            if let Some((k, v)) = token.split_once('=') {
                // Prefer part crc32 (`pcrc32`) when present on multi-part articles
                if k == "pcrc32" {
                    return u32::from_str_radix(v.trim(), 16).ok();
                }
                if k == "crc32" {
                    full_crc = u32::from_str_radix(v.trim(), 16).ok();
                }
            }
        }
        full_crc
    }
}

/// Unified Usenet article decoder: dispatches yEnc, uuencode, or base64 fallback.
pub struct ArticleDecoder;

impl ArticleDecoder {
    pub fn decode(raw: &[u8]) -> Result<DecodedArticle, DecodeError> {
        if YencDecoder::find_subslice(raw, b"=ybegin").is_some() {
            return YencDecoder::decode(raw);
        }
        if UuDecoder::find_begin_line(raw).is_some() {
            return UuDecoder::decode(raw);
        }
        Base64Decoder::decode(raw)
    }
}

/// Decodes traditional Usenet `uuencode` (`begin <mode> <name> ... end`) articles.
pub struct UuDecoder;

impl UuDecoder {
    fn find_begin_line(raw: &[u8]) -> Option<usize> {
        let text = String::from_utf8_lossy(raw);
        let mut offset = 0usize;
        for line in text.split_inclusive('\n') {
            let trimmed = line.trim();
            if trimmed.starts_with("begin ") {
                let parts: Vec<&str> = trimmed.splitn(3, ' ').collect();
                if parts.len() == 3 && parts[1].chars().all(|c| c.is_ascii_digit()) {
                    return Some(offset);
                }
            }
            offset += line.len();
        }
        None
    }

    pub fn decode(raw: &[u8]) -> Result<DecodedArticle, DecodeError> {
        let text = String::from_utf8_lossy(raw);
        let mut filename = String::from("uuencoded.bin");
        let mut started = false;
        let mut output = Vec::new();

        for line in text.lines() {
            let trimmed = line.trim();
            if !started {
                if trimmed.starts_with("begin ") {
                    let parts: Vec<&str> = trimmed.splitn(3, ' ').collect();
                    if parts.len() == 3 {
                        filename = parts[2].trim().to_string();
                        started = true;
                    }
                }
                continue;
            }
            if trimmed == "end" {
                break;
            }
            if trimmed == "`" {
                continue;
            }
            let bytes = trimmed.as_bytes();
            if bytes.is_empty() {
                continue;
            }
            let len = ((bytes[0].wrapping_sub(32)) & 0x3f) as usize;
            if len == 0 {
                continue;
            }
            let mut decoded_line = Vec::with_capacity(len);
            for chunk in bytes[1..].chunks(4) {
                if chunk.len() < 4 {
                    break;
                }
                let c0 = chunk[0].wrapping_sub(32) & 0x3f;
                let c1 = chunk[1].wrapping_sub(32) & 0x3f;
                let c2 = chunk[2].wrapping_sub(32) & 0x3f;
                let c3 = chunk[3].wrapping_sub(32) & 0x3f;
                decoded_line.push((c0 << 2) | (c1 >> 4));
                decoded_line.push((c1 << 4) | (c2 >> 2));
                decoded_line.push((c2 << 6) | c3);
            }
            decoded_line.truncate(len);
            output.extend_from_slice(&decoded_line);
        }

        if !started {
            return Err(DecodeError::MissingHeader);
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&output);
        let crc32 = hasher.finalize();

        Ok(DecodedArticle {
            header: YencHeader {
                name: filename,
                line: 45,
                size: output.len(),
                part: None,
                total: None,
            },
            part_offset: 0,
            part_size: output.len(),
            data: output,
            crc32,
        })
    }
}

/// Decodes MIME/base64-encoded Usenet articles.
pub struct Base64Decoder;

impl Base64Decoder {
    pub fn decode(raw: &[u8]) -> Result<DecodedArticle, DecodeError> {
        let text = String::from_utf8_lossy(raw);
        let mut output = Vec::new();
        let mut buf: u32 = 0;
        let mut bits: u8 = 0;

        for b in text.bytes() {
            let val = match b {
                b'A'..=b'Z' => b - b'A',
                b'a'..=b'z' => b - b'a' + 26,
                b'0'..=b'9' => b - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                b'=' => break,
                b'\r' | b'\n' | b' ' | b'\t' => continue,
                _ => return Err(DecodeError::CorruptedData),
            };
            buf = (buf << 6) | (val as u32);
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                output.push(((buf >> bits) & 0xff) as u8);
            }
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&output);
        let crc32 = hasher.finalize();

        Ok(DecodedArticle {
            header: YencHeader {
                name: "base64.bin".to_string(),
                line: 76,
                size: output.len(),
                part: None,
                total: None,
            },
            part_offset: 0,
            part_size: output.len(),
            data: output,
            crc32,
        })
    }
}

/// Writes decoded yEnc parts at their byte offsets into target files.
pub struct FileAssembler;

impl FileAssembler {
    /// Sanitizes an untrusted yEnc filename to prevent path traversal.
    pub fn safe_filename(raw_name: &str, fallback: &str) -> String {
        let base = Path::new(raw_name)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .trim();
        if base.is_empty() || base == "." || base == ".." {
            fallback.to_string()
        } else {
            base.to_string()
        }
    }

    /// Writes a decoded article segment into `target_dir/<filename>` at `part_offset`.
    pub async fn write_article(
        target_dir: &Path,
        fallback_name: &str,
        article: &DecodedArticle,
    ) -> Result<PathBuf, DecodeError> {
        tokio::fs::create_dir_all(target_dir).await?;
        let filename = Self::safe_filename(&article.header.name, fallback_name);
        let file_path = target_dir.join(filename);

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&file_path)
            .await?;

        file.seek(SeekFrom::Start(article.part_offset as u64)).await?;
        file.write_all(&article.data).await?;
        file.flush().await?;

        Ok(file_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn yenc_encode(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(data.len());
        for &b in data {
            let shifted = b.wrapping_add(42);
            if matches!(shifted, 0 | b'\n' | b'\r' | b'=') {
                out.push(b'=');
                out.push(shifted.wrapping_add(64));
            } else {
                out.push(shifted);
            }
        }
        out
    }

    #[test]
    fn test_yenc_decode_single_part() {
        let text = b"Hello World!";
        let encoded = yenc_encode(text);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(text);
        let crc = hasher.finalize();

        let mut raw = Vec::new();
        raw.extend_from_slice(b"=ybegin line=128 size=12 name=test file.txt\r\n");
        raw.extend_from_slice(&encoded);
        raw.extend_from_slice(format!("\r\n=yend size=12 crc32={crc:08x}\r\n").as_bytes());

        let res = YencDecoder::decode(&raw).unwrap();
        assert_eq!(res.header.name, "test file.txt");
        assert_eq!(res.header.size, 12);
        assert_eq!(res.data, text);
    }

    #[tokio::test]
    async fn test_multipart_decode_and_out_of_order_assembly() {
        let dir = tempfile::tempdir().unwrap();
        let part1 = b"Hello ";
        let part2 = b"RustNZBD!\n";

        let mut h1 = crc32fast::Hasher::new();
        h1.update(part1);
        let crc1 = h1.finalize();

        let mut h2 = crc32fast::Hasher::new();
        h2.update(part2);
        let crc2 = h2.finalize();

        let mut raw1 = Vec::new();
        raw1.extend_from_slice(b"=ybegin part=1 total=2 line=128 size=16 name=../../evil/out.bin\r\n");
        raw1.extend_from_slice(b"=ypart begin=1 end=6\r\n");
        raw1.extend_from_slice(&yenc_encode(part1));
        raw1.extend_from_slice(format!("\r\n=yend size=6 part=1 pcrc32={crc1:08x}\r\n").as_bytes());

        let mut raw2 = Vec::new();
        raw2.extend_from_slice(b"=ybegin part=2 total=2 line=128 size=16 name=../../evil/out.bin\r\n");
        raw2.extend_from_slice(b"=ypart begin=7 end=16\r\n");
        raw2.extend_from_slice(&yenc_encode(part2));
        raw2.extend_from_slice(format!("\r\n=yend size=10 part=2 pcrc32={crc2:08x}\r\n").as_bytes());

        let dec1 = YencDecoder::decode(&raw1).unwrap();
        let dec2 = YencDecoder::decode(&raw2).unwrap();

        // Write part 2 before part 1
        let path2 = FileAssembler::write_article(dir.path(), "fallback.bin", &dec2)
            .await
            .unwrap();
        let path1 = FileAssembler::write_article(dir.path(), "fallback.bin", &dec1)
            .await
            .unwrap();

        assert_eq!(path1, dir.path().join("out.bin"));
        assert_eq!(path1, path2);
        let assembled = tokio::fs::read(&path1).await.unwrap();
        assert_eq!(assembled, b"Hello RustNZBD!\n");
    }

    #[test]
    fn test_uuencode_and_base64_fallback() {
        // "Cat" uuencoded: length 3 ('#'), bytes C(67) a(97) t(116) -> 0x436174 -> 010000 110110 000101 110100 -> 16,54,5,52 -> +32 = 48('0'), 86('V'), 37('%'), 84('T')
        let uu = b"begin 644 cat.txt\r\n#0V%T\r\n`\r\nend\r\n";
        let dec = ArticleDecoder::decode(uu).unwrap();
        assert_eq!(dec.header.name, "cat.txt");
        assert_eq!(dec.data, b"Cat");

        // "Hello!" in base64: SGVsbG8h
        let b64 = b"SGVs\r\nbG8h\r\n";
        let dec2 = ArticleDecoder::decode(b64).unwrap();
        assert_eq!(dec2.data, b"Hello!");
    }
}
