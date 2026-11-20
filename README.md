# RustNZBD

High-performance Usenet (NZB) downloader written in Rust — a single-binary, drop-in SABnzbd replacement compatible with Sonarr, Radarr, Lidarr, Readarr, Prowlarr, and NZB360.

## Size & Startup Speed vs. Original SABnzbd

Measured side-by-side on macOS against [adam2893/sabnzbd](https://github.com/adam2893/sabnzbd) (`SABnzbd.py` running in Python 3.12 venv with `requirements.txt`):

| Metric | RustNZBD | Original SABnzbd | Difference |
|---|---|---|---|
| **Core Source LOC** | **5,281 lines** (`crates/` + `ui.html`) | **41,109 lines** (`SABnzbd.py` + `sabnzbd/*.py`) | **7.8x smaller** |
| **Source Disk Size** | **240 KB** (`crates/`) | **3.6 MB** (`1.6 MB` `sabnzbd/` + `2.0 MB` `interfaces/`) | **15x smaller** |
| **Runtime Artifact** | **5.3 MB** single LTO+stripped binary | 3.6 MB source/templates + Python + pip deps | Zero runtime deps |
| **Cold Startup** (run 1 -> HTTP 200) | **23.13 ms** | 589.24 ms | **25.5x faster** |
| **Warm Startup** (runs 2–5 -> HTTP 200) | **7.36 / 7.02 / 6.98 / 7.08 ms** | 368.71 / 359.20 / 361.35 / 363.19 ms | **~51x faster** |
| **5-Run Average Startup** | **10.31 ms** | 408.34 ms | **39.6x faster** |

## What Was Built (8-Crate Workspace)

- **`sabnzbd-core`**: TOML config (`rustnzbd.toml`), legacy `sabnzbd.ini` importer, category path routing, inline `{{password}}` extraction, atomic per-server `StatsTracker`, and GNU gettext `.po` i18n catalog (`I18nCatalog`).
- **`sabnzbd-nntp`**: Async NNTP client with `tokio-rustls` + `webpki-roots` TLS, plain TCP, RFC 1928 SOCKS5 proxy tunneling, and semaphore-bounded connection pooling with auto-reconnect.
- **`sabnzbd-nzb`**: Streaming XML NZB parser (`quick-xml`), priority download queue, duplicate detection, and atomic JSON state persistence (`rustnzbd_state.json`) with crash recovery.
- **`sabnzbd-decode`**: Binary-safe yEnc decoder (`crc32fast` `crc32`/`pcrc32` validation), `uuencode` and `base64` fallback decoders, and offset-based `FileAssembler` with path-traversal protection.
- **`sabnzbd-postproc`**: PAR2 verification/repair (`par2`), passworded archive extraction (`unrar`, `unzip`, `7z`), primary RAR volume detection, clutter cleanup, obfuscated filename renaming, `SAB_*` post-processing scripts, and TV/Movie/Date media sorting.
- **`sabnzbd-services`**: RSS 2.0 feed poller with accept/reject filters, minute-accurate `Scheduler` (`Pause`, `Resume`, `SpeedLimit`, `ScanRss`), JSON webhook dispatcher, and async SMTP email notifier.
- **`sabnzbd-api`**: Axum HTTP/HTTPS server implementing SABnzbd `/api` endpoints (`version`, `queue`, `history`, `addfile`, `addurl`, `addlocalfile`, `get_files`, `get_cats`, `change_cat`, `pause`, `resume`, `server_stats`, `config`, `get_config`, `save_settings`, `add_server`, `delete_server`, `get_lang`, `warnings`, `auth`, `shutdown`, `output=xml`) and embedded Web UI at `/`.
- **`sabnzbd-cli`**: `rustnzbd` binary with concurrent segment workers, Direct Unpack, speed-limit throttling, watched `.nzb` directory scanner, optional HTTPS listener, `--import-ini`, `--tray` desktop menu (`tray-icon` + `tao`), and graceful shutdown.

## Quick Start

```sh
cargo build --release
./target/release/rustnzbd
```

On first launch, `rustnzbd.toml` is created in the working directory and the generated `api_key` is printed to the console.

- Web UI: `http://127.0.0.1:8080/`
- SABnzbd API URL: `http://127.0.0.1:8080/api?apikey=<API_KEY>`

### Options & Migration

```sh
# Launch with desktop system tray (Open Web UI, Pause, Resume, Quit)
./target/release/rustnzbd --tray

# Migrate an existing SABnzbd configuration
./target/release/rustnzbd --import-ini /path/to/sabnzbd.ini

# Override config/state paths
RUSTNZBD_CONFIG=/path/to/rustnzbd.toml RUSTNZBD_STATE=/path/to/state.json ./target/release/rustnzbd
```

## Tests

27 unit and end-to-end integration tests across all 8 crates:

```sh
cargo test
```
