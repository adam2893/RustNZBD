use sabnzbd_core::SortingConfig;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::process::Command;
use tracing::{info, warn};

#[derive(Error, Debug)]
pub enum PostProcError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("PAR2 repair failed with exit code: {0:?}")]
    Par2Failed(Option<i32>),
    #[error("Unpack failed with exit code: {0:?}")]
    UnpackFailed(Option<i32>),
    #[error("Post-processing script failed with exit code: {0:?}")]
    ScriptFailed(Option<i32>),
}

pub struct PostProcessor;

impl PostProcessor {
    /// Returns true if `path` is a standalone `.rar` or the first volume (`.part01.rar` / `.part1.rar`).
    pub fn is_primary_rar_volume(path: &Path) -> bool {
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            return false;
        };
        let lower = name.to_ascii_lowercase();
        if !lower.ends_with(".rar") {
            return false;
        }
        let stem = &lower[..lower.len() - 4];
        if let Some(pos) = stem.rfind(".part") {
            let num_part = &stem[pos + 5..];
            if !num_part.is_empty() && num_part.chars().all(|c| c.is_ascii_digit()) {
                return num_part.parse::<u32>().ok() == Some(1);
            }
        }
        true
    }

    /// Runs par2 repair on a directory containing par2 files if par2cmdline is installed.
    pub async fn verify_and_repair(dir: &Path) -> Result<bool, PostProcError> {
        let mut entries = tokio::fs::read_dir(dir).await?;
        let mut par2_file: Option<PathBuf> = None;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()).map(|s| s.eq_ignore_ascii_case("par2")) == Some(true) {
                par2_file = Some(path);
                break;
            }
        }

        let Some(par_path) = par2_file else {
            return Ok(true);
        };

        info!("Starting PAR2 repair on {:?}", par_path);
        let output = Command::new("par2")
            .arg("repair")
            .arg(&par_path)
            .output()
            .await;

        match output {
            Ok(out) if out.status.success() => {
                info!("PAR2 verification & repair succeeded");
                Ok(true)
            }
            Ok(out) => {
                warn!("PAR2 repair finished with non-zero status: {:?}", out.status.code());
                Err(PostProcError::Par2Failed(out.status.code()))
            }
            Err(e) => {
                warn!("par2 executable not found or failed to spawn: {}", e);
                Ok(false)
            }
        }
    }

    /// Extracts an archive (rar, zip, 7z), trying no-password first and then each candidate password.
    pub async fn extract_archive_with_passwords(
        archive: &Path,
        dest_dir: &Path,
        passwords: &[String],
    ) -> Result<(), PostProcError> {
        tokio::fs::create_dir_all(dest_dir).await?;
        let ext = archive
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();

        if !matches!(ext.as_str(), "rar" | "zip" | "7z") {
            return Ok(());
        }

        let mut candidates: Vec<Option<&str>> = Vec::with_capacity(1 + passwords.len());
        candidates.push(None);
        for p in passwords {
            if !p.is_empty() {
                candidates.push(Some(p.as_str()));
            }
        }

        let mut last_code = None;
        for pass in candidates {
            let (cmd, args) = Self::build_extract_cmd(&ext, archive, dest_dir, pass);
            info!("Extracting {:?} with {} (password={})", archive, cmd, pass.is_some());
            let status = Command::new(cmd).args(&args).status().await?;
            if status.success() {
                return Ok(());
            }
            last_code = status.code();
        }

        Err(PostProcError::UnpackFailed(last_code))
    }

    /// Convenience wrapper without passwords.
    pub async fn extract_archive(archive: &Path, dest_dir: &Path) -> Result<(), PostProcError> {
        Self::extract_archive_with_passwords(archive, dest_dir, &[]).await
    }

    fn build_extract_cmd(
        ext: &str,
        archive: &Path,
        dest_dir: &Path,
        password: Option<&str>,
    ) -> (&'static str, Vec<OsString>) {
        match ext {
            "rar" => {
                let p_flag = match password {
                    Some(p) => OsString::from(format!("-p{p}")),
                    None => OsString::from("-p-"),
                };
                (
                    "unrar",
                    vec![
                        "x".into(),
                        "-o+".into(),
                        "-y".into(),
                        p_flag,
                        archive.as_os_str().to_os_string(),
                        dest_dir.as_os_str().to_os_string(),
                    ],
                )
            }
            "zip" => {
                let mut args: Vec<OsString> = vec!["-o".into()];
                if let Some(p) = password {
                    args.push("-P".into());
                    args.push(OsString::from(p));
                }
                args.push(archive.as_os_str().to_os_string());
                args.push("-d".into());
                args.push(dest_dir.as_os_str().to_os_string());
                ("unzip", args)
            }
            "7z" => {
                let mut o_flag = OsString::from("-o");
                o_flag.push(dest_dir.as_os_str());
                let p_flag = match password {
                    Some(p) => OsString::from(format!("-p{p}")),
                    None => OsString::from("-p"),
                };
                (
                    "7z",
                    vec![
                        "x".into(),
                        "-y".into(),
                        p_flag,
                        o_flag,
                        archive.as_os_str().to_os_string(),
                    ],
                )
            }
            _ => ("true", vec![]),
        }
    }

    /// Deletes archive, parity, and metadata clutter files after extraction.
    pub async fn cleanup_processed_files(dir: &Path) -> Result<usize, PostProcError> {
        let mut removed = 0usize;
        let mut entries = tokio::fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let ext = path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            let is_split_rar = ext.starts_with('r')
                && ext.len() >= 3
                && ext[1..].chars().all(|c| c.is_ascii_digit());

            if matches!(ext.as_str(), "par2" | "rar" | "zip" | "7z" | "sfv" | "nzb") || is_split_rar {
                if tokio::fs::remove_file(&path).await.is_ok() {
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    /// Heuristic matching SABnzbd's obfuscated filename detection (hex/random hashes or pure digits).
    pub fn is_obfuscated_stem(stem: &str) -> bool {
        let s = stem.trim();
        if s.is_empty() {
            return false;
        }
        if s.chars().all(|c| c.is_ascii_digit()) {
            return true;
        }
        if s.len() >= 16 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            return true;
        }
        if s.len() >= 24 && !s.contains(' ') && !s.contains('.') && s.chars().all(|c| c.is_ascii_alphanumeric()) {
            let has_digit = s.chars().any(|c| c.is_ascii_digit());
            let has_upper = s.chars().any(|c| c.is_ascii_uppercase());
            let has_lower = s.chars().any(|c| c.is_ascii_lowercase());
            return has_digit && has_upper && has_lower;
        }
        false
    }

    /// Renames the largest obfuscated media file in `dir` to match `job_name`.
    pub async fn deobfuscate_dir(dir: &Path, job_name: &str) -> Result<Option<PathBuf>, PostProcError> {
        let clean_job = job_name
            .trim_end_matches(".nzb")
            .trim_end_matches(".NZB")
            .trim();
        if clean_job.is_empty() {
            return Ok(None);
        }

        let mut largest: Option<(u64, PathBuf, String)> = None;
        let mut entries = tokio::fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let meta = entry.metadata().await?;
            let ext = path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            if Self::is_obfuscated_stem(stem) {
                if largest.as_ref().map(|(sz, _, _)| meta.len() > *sz).unwrap_or(true) {
                    largest = Some((meta.len(), path, ext));
                }
            }
        }

        let Some((_, src_path, ext)) = largest else {
            return Ok(None);
        };

        let target_name = if ext.is_empty() {
            clean_job.to_string()
        } else {
            format!("{clean_job}.{ext}")
        };
        let target_path = dir.join(target_name);
        if !target_path.exists() {
            info!("Deobfuscating {:?} -> {:?}", src_path, target_path);
            tokio::fs::rename(&src_path, &target_path).await?;
            return Ok(Some(target_path));
        }
        Ok(None)
    }

    /// Runs a user post-processing script with SABnzbd's standard environment variables.
    pub async fn run_postproc_script(
        script_path: &Path,
        final_dir: &Path,
        job_name: &str,
        category: &str,
        status_ok: bool,
    ) -> Result<(), PostProcError> {
        let status_code = if status_ok { "0" } else { "1" };
        info!("Running post-processing script {:?} on {:?}", script_path, final_dir);
        let status = Command::new(script_path)
            .arg(final_dir)
            .arg(job_name)
            .arg(job_name)
            .arg("")
            .arg(category)
            .arg("")
            .arg(status_code)
            .env("SAB_FINAL_DIR", final_dir)
            .env("SAB_ORIG_NZB", job_name)
            .env("SAB_CAT", category)
            .env("SAB_STATUS", status_code)
            .status()
            .await?;

        if status.success() {
            Ok(())
        } else {
            Err(PostProcError::ScriptFailed(status.code()))
        }
    }
}

/// SABnzbd-compatible TV / Date / Movie media sorter.
pub struct Sorter;

impl Sorter {
    /// Computes a relative sorted path (without extension) from `job_name` and `SortingConfig`.
    pub fn compute_sorted_relpath(job_name: &str, cfg: &SortingConfig) -> Option<PathBuf> {
        let clean = job_name.trim_end_matches(".nzb").trim_end_matches(".NZB");
        let tokens: Vec<&str> = clean
            .split(|c: char| matches!(c, '.' | ' ' | '_'))
            .filter(|s| !s.is_empty())
            .collect();

        if cfg.enable_tv_sorting {
            if let Some((idx, season, episode)) = Self::find_tv_ep(&tokens) {
                let show = Self::title_case(&tokens[..idx].join(" "));
                if !show.is_empty() {
                    let rendered = cfg
                        .tv_sort_string
                        .replace("{show}", &show)
                        .replace("{season:02}", &format!("{season:02}"))
                        .replace("{episode:02}", &format!("{episode:02}"))
                        .replace("{season}", &season.to_string())
                        .replace("{episode}", &episode.to_string());
                    return Some(PathBuf::from(rendered));
                }
            }
        }

        if cfg.enable_date_sorting {
            if let Some((idx, y, m, d)) = Self::find_date(&tokens) {
                let show = Self::title_case(&tokens[..idx].join(" "));
                if !show.is_empty() {
                    let rendered = cfg
                        .date_sort_string
                        .replace("{show}", &show)
                        .replace("{year}", &format!("{y:04}"))
                        .replace("{month:02}", &format!("{m:02}"))
                        .replace("{day:02}", &format!("{d:02}"));
                    return Some(PathBuf::from(rendered));
                }
            }
        }

        if cfg.enable_movie_sorting {
            if let Some((idx, year)) = Self::find_movie_year(&tokens) {
                let title = Self::title_case(&tokens[..idx].join(" "));
                if !title.is_empty() {
                    let rendered = cfg
                        .movie_sort_string
                        .replace("{title}", &title)
                        .replace("{year}", &year.to_string());
                    return Some(PathBuf::from(rendered));
                }
            }
        }

        None
    }

    /// Moves/renames the largest file in `dir` according to `SortingConfig` relative to `dest_root`.
    pub async fn sort_media_file(
        dir: &Path,
        dest_root: &Path,
        job_name: &str,
        cfg: &SortingConfig,
    ) -> Result<Option<PathBuf>, PostProcError> {
        let Some(rel) = Self::compute_sorted_relpath(job_name, cfg) else {
            return Ok(None);
        };

        let mut largest: Option<(u64, PathBuf, String)> = None;
        let mut entries = tokio::fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                let meta = entry.metadata().await?;
                let ext = path
                    .extension()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string();
                if largest.as_ref().map(|(sz, _, _)| meta.len() > *sz).unwrap_or(true) {
                    largest = Some((meta.len(), path, ext));
                }
            }
        }

        let Some((_, src, ext)) = largest else {
            return Ok(None);
        };
        let mut target = dest_root.join(rel);
        if !ext.is_empty() {
            target.set_extension(ext);
        }
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if tokio::fs::rename(&src, &target).await.is_err() {
            tokio::fs::copy(&src, &target).await?;
            let _ = tokio::fs::remove_file(&src).await;
        }
        Ok(Some(target))
    }

    fn find_tv_ep(tokens: &[&str]) -> Option<(usize, u32, u32)> {
        for (i, tok) in tokens.iter().enumerate() {
            let lower = tok.to_ascii_lowercase();
            if let Some(rest) = lower.strip_prefix('s') {
                if let Some((s_str, e_str)) = rest.split_once('e') {
                    let e_digits: String = e_str.chars().take_while(|c| c.is_ascii_digit()).collect();
                    if let (Ok(s), Ok(e)) = (s_str.parse::<u32>(), e_digits.parse::<u32>()) {
                        return Some((i, s, e));
                    }
                }
            }
            if let Some((s_str, e_str)) = lower.split_once('x') {
                if !s_str.is_empty() && !e_str.is_empty() && s_str.chars().all(|c| c.is_ascii_digit()) && e_str.chars().all(|c| c.is_ascii_digit()) {
                    if let (Ok(s), Ok(e)) = (s_str.parse::<u32>(), e_str.parse::<u32>()) {
                        return Some((i, s, e));
                    }
                }
            }
        }
        None
    }

    fn find_date(tokens: &[&str]) -> Option<(usize, u32, u32, u32)> {
        for i in 0..tokens.len() {
            if let Some((y, rest)) = tokens[i].split_once('-') {
                if let Some((m, d)) = rest.split_once('-') {
                    if let (Ok(yv), Ok(mv), Ok(dv)) = (y.parse::<u32>(), m.parse::<u32>(), d.parse::<u32>()) {
                        if (1970..=2100).contains(&yv) && (1..=12).contains(&mv) && (1..=31).contains(&dv) {
                            return Some((i, yv, mv, dv));
                        }
                    }
                }
            }
            if i + 2 < tokens.len() {
                if let (Ok(yv), Ok(mv), Ok(dv)) = (tokens[i].parse::<u32>(), tokens[i + 1].parse::<u32>(), tokens[i + 2].parse::<u32>()) {
                    if (1970..=2100).contains(&yv) && (1..=12).contains(&mv) && (1..=31).contains(&dv) {
                        return Some((i, yv, mv, dv));
                    }
                }
            }
        }
        None
    }

    fn find_movie_year(tokens: &[&str]) -> Option<(usize, u32)> {
        for (i, tok) in tokens.iter().enumerate().skip(1) {
            let trimmed = tok.trim_matches(|c| c == '(' || c == ')');
            if trimmed.len() == 4 {
                if let Ok(y) = trimmed.parse::<u32>() {
                    if (1920..=2099).contains(&y) {
                        return Some((i, y));
                    }
                }
            }
        }
        None
    }

    fn title_case(s: &str) -> String {
        s.split_whitespace()
            .map(|w| {
                let mut chars = w.chars();
                match chars.next() {
                    Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                    None => String::new(),
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primary_rar_volume_detection() {
        assert!(PostProcessor::is_primary_rar_volume(Path::new("movie.rar")));
        assert!(PostProcessor::is_primary_rar_volume(Path::new("movie.part01.rar")));
        assert!(PostProcessor::is_primary_rar_volume(Path::new("movie.part1.rar")));
        assert!(!PostProcessor::is_primary_rar_volume(Path::new("movie.part02.rar")));
        assert!(!PostProcessor::is_primary_rar_volume(Path::new("movie.r00")));
    }

    #[test]
    fn test_obfuscated_stem_detection() {
        assert!(PostProcessor::is_obfuscated_stem("a8f92e1b7c3d4e5f0123456789abcdef"));
        assert!(PostProcessor::is_obfuscated_stem("12345678"));
        assert!(PostProcessor::is_obfuscated_stem("aB3cD9eF1gH2iJ4kL5mN6oP7"));
        assert!(!PostProcessor::is_obfuscated_stem("Ubuntu.24.04.LTS.Desktop"));
    }

    #[tokio::test]
    async fn test_cleanup_and_deobfuscate() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("a8f92e1b7c3d4e5f.mkv"), b"video-bytes").await.unwrap();
        tokio::fs::write(dir.path().join("release.par2"), b"par").await.unwrap();
        tokio::fs::write(dir.path().join("release.r00"), b"rar").await.unwrap();
        tokio::fs::write(dir.path().join("release.rar"), b"rar").await.unwrap();

        let removed = PostProcessor::cleanup_processed_files(dir.path()).await.unwrap();
        assert_eq!(removed, 3);

        let renamed = PostProcessor::deobfuscate_dir(dir.path(), "My.Show.S01E01.nzb").await.unwrap();
        assert_eq!(renamed, Some(dir.path().join("My.Show.S01E01.mkv")));
        assert!(dir.path().join("My.Show.S01E01.mkv").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_run_postproc_script() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("hook.sh");
        let marker = dir.path().join("ran.txt");
        let body = format!("#!/bin/sh\necho \"$SAB_CAT:$SAB_STATUS\" > {}\n", marker.display());
        tokio::fs::write(&script, body).await.unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        PostProcessor::run_postproc_script(&script, dir.path(), "job.nzb", "tv", true)
            .await
            .unwrap();

        let out = tokio::fs::read_to_string(&marker).await.unwrap();
        assert_eq!(out.trim(), "tv:0");
    }

    #[test]
    fn test_sorting_tv_movie_date() {
        let cfg = SortingConfig {
            enable_tv_sorting: true,
            enable_movie_sorting: true,
            enable_date_sorting: true,
            ..Default::default()
        };

        let tv = Sorter::compute_sorted_relpath("the.expanse.s03e04.1080p.nzb", &cfg).unwrap();
        assert_eq!(tv, PathBuf::from("The Expanse/Season 03/The Expanse - S03E04"));

        let date = Sorter::compute_sorted_relpath("daily.show.2026.11.20.720p.nzb", &cfg).unwrap();
        assert_eq!(date, PathBuf::from("Daily Show/2026-11-20/Daily Show - 2026-11-20"));

        let movie = Sorter::compute_sorted_relpath("dune.part.two.2024.2160p.nzb", &cfg).unwrap();
        assert_eq!(movie, PathBuf::from("Dune Part Two (2024)/Dune Part Two (2024)"));
    }
}
