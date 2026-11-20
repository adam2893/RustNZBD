# RustNZBD

High-performance Usenet (NZB) downloader written in Rust, designed as a drop-in SABnzbd REST API replacement for Sonarr, Radarr, Lidarr, Readarr, and NZB360.

## Workspace Crates

- `sabnzbd-core`: TOML configuration (`rustnzbd.toml`), category path resolution, queue & history models.
- `sabnzbd-nntp`: Async NNTP client (`tokio-rustls` + `webpki-roots` TLS and plain TCP), semaphore-bounded connection pooling with auto-reconnect, `BODY`, `STAT`, `QUIT`, `AUTHINFO`.
- `sabnzbd-nzb`: Streaming XML NZB parser (`quick-xml`), priority download queue, atomic JSON state persistence (`rustnzbd_state.json`) with crash recovery.
- `sabnzbd-decode`: Binary-safe yEnc decoder (`pcrc32` / `crc32` validation) and offset-based `FileAssembler` with path-traversal protection.
- `sabnzbd-postproc`: PAR2 verification/repair (`par2`) and archive extraction (`unrar`, `unzip`, `7z`).
- `sabnzbd-api`: Axum HTTP server with SABnzbd-compatible `/api` endpoints (`version`, `queue`, `history`, `addfile`, `addurl`, `pause`, `resume`, `config`, `get_config`, `add_server`, `get_cats`, `warnings`, `auth`) and built-in Web UI at `/`.
- `sabnzbd-cli`: `rustnzbd` daemon with concurrent download worker, speed-limit throttling, watched `.nzb` directory scanner, and graceful Ctrl-C shutdown.

## Quick Start

```sh
cargo run --release
```

On first launch, `rustnzbd.toml` is created in the working directory and the generated `api_key` is printed to the console.

- Web UI: `http://127.0.0.1:8080/`
- SABnzbd API URL: `http://127.0.0.1:8080/api?apikey=<API_KEY>`

Override file paths via environment variables:

```sh
RUSTNZBD_CONFIG=/path/to/rustnzbd.toml RUSTNZBD_STATE=/path/to/state.json cargo run --release
```

## Tests

```sh
cargo test
```
