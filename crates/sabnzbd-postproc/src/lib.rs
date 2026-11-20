use std::path::{Path, PathBuf};
use std::ffi::OsString;
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
}

pub struct PostProcessor;

impl PostProcessor {
    /// Runs par2 repair on a directory containing par2 files if par2cmdline is installed.
    pub async fn verify_and_repair(dir: &Path) -> Result<bool, PostProcError> {
        let mut entries = tokio::fs::read_dir(dir).await?;
        let mut par2_file: Option<PathBuf> = None;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "par2" {
                    par2_file = Some(path);
                    break;
                }
            }
        }

        let Some(par_path) = par2_file else {
            return Ok(true); // Nothing to repair
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
                // Non-fatal if system has no par2 installed yet
                Ok(false)
            }
        }
    }

    /// Extracts archives (rar, zip, 7z) into the destination directory.
    pub async fn extract_archive(archive: &Path, dest_dir: &Path) -> Result<(), PostProcError> {
        tokio::fs::create_dir_all(dest_dir).await?;
        let ext = archive.extension().and_then(|s| s.to_str()).unwrap_or("");

        let (cmd, args): (&str, Vec<OsString>) = match ext {
            "rar" => (
                "unrar",
                vec![
                    "x".into(),
                    "-o+".into(),
                    "-y".into(),
                    archive.as_os_str().to_os_string(),
                    dest_dir.as_os_str().to_os_string(),
                ],
            ),
            "zip" => (
                "unzip",
                vec![
                    "-o".into(),
                    archive.as_os_str().to_os_string(),
                    "-d".into(),
                    dest_dir.as_os_str().to_os_string(),
                ],
            ),
            "7z" => {
                let mut o_flag = OsString::from("-o");
                o_flag.push(dest_dir.as_os_str());
                (
                    "7z",
                    vec![
                        "x".into(),
                        "-y".into(),
                        o_flag,
                        archive.as_os_str().to_os_string(),
                    ],
                )
            }
            _ => return Ok(()),
        };

        info!("Extracting archive {:?} to {:?}", archive, dest_dir);
        let status = Command::new(cmd).args(&args).status().await?;
        if status.success() {
            Ok(())
        } else {
            Err(PostProcError::UnpackFailed(status.code()))
        }
    }
}
