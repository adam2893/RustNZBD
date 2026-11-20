use thiserror::Error;

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("Missing =ybegin header")]
    MissingHeader,
    #[error("Corrupt yEnc stream")]
    CorruptedData,
    #[error("CRC32 mismatch: expected {expected:08x}, got {actual:08x}")]
    CrcMismatch { expected: u32, actual: u32 },
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
        let first_nl = after_begin.iter().position(|&b| b == b'\n').unwrap_or(after_begin.len());
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
            let part_nl = trimmed_rest.iter().position(|&b| b == b'\n').unwrap_or(trimmed_rest.len());
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
        for token in line.split_whitespace() {
            if let Some((k, v)) = token.split_once('=') {
                match k {
                    "name" => header.name = v.to_string(),
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
        for token in trailer.split_whitespace() {
            if let Some((k, v)) = token.split_once('=') {
                if k == "crc32" || k == "pcrc32" {
                    return u32::from_str_radix(v.trim(), 16).ok();
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_yenc_decode_single_part() {
        let text = b"Hello World!";
        let mut encoded = Vec::new();
        for &b in text {
            encoded.push(b.wrapping_add(42));
        }

        let mut raw = Vec::new();
        raw.extend_from_slice(b"=ybegin line=128 size=12 name=test.txt\r\n");
        raw.extend_from_slice(&encoded);
        raw.extend_from_slice(b"\r\n=yend size=12\r\n");

        let res = YencDecoder::decode(&raw).unwrap();
        assert_eq!(res.header.name, "test.txt");
        assert_eq!(res.header.size, 12);
        assert_eq!(res.data, b"Hello World!");
    }
}
