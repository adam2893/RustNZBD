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
}
