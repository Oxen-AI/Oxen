use serde::{Deserialize, Serialize};
use std::fmt;

use crate::{error::OxenError, model::LocalRepository, util};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SizeStatus {
    Pending,
    Done,
    Error,
}

impl fmt::Display for SizeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SizeStatus::Pending => write!(f, "pending"),
            SizeStatus::Done => write!(f, "done"),
            SizeStatus::Error => write!(f, "error"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RepoSizeFile {
    pub status: SizeStatus,
    pub size: u64,
}

impl fmt::Display for RepoSizeFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match serde_json::to_string(self) {
            Ok(s) => write!(f, "{s}"),
            Err(_) => write!(f, ""),
        }
    }
}

pub fn update_size(repo: &LocalRepository) -> Result<(), OxenError> {
    let path = repo_size_path(repo);
    let size = match util::fs::read_from_path(&path) {
        Ok(content) => match serde_json::from_str::<RepoSizeFile>(&content) {
            Ok(parsed) => RepoSizeFile {
                status: SizeStatus::Pending,
                size: parsed.size,
            },
            Err(e) => {
                return Err(OxenError::basic_str(format!(
                    "Failed to parse size file: {e}"
                )));
            }
        },
        Err(e) => {
            log::info!("Size file not found, creating it: {e}");

            RepoSizeFile {
                status: SizeStatus::Pending,
                size: 0,
            }
        }
    };

    util::fs::write_to_path(&path, size.to_string())?;

    let repo_path = repo.path.clone();
    let path_clone = path.clone();

    // Spawn background thread for size calculation
    std::thread::spawn(move || {
        let size_result = fs_extra::dir::get_size(&repo_path);
        match size_result {
            Ok(calculated_size) => {
                let size = RepoSizeFile {
                    status: SizeStatus::Done,
                    size: calculated_size,
                };
                if let Err(e) = util::fs::write_to_path(&path_clone, size.to_string()) {
                    log::error!("Failed to write size result: {e}");
                }
            }
            Err(e) => {
                log::error!("Failed to calculate repository size: {e}");
                let size = RepoSizeFile {
                    status: SizeStatus::Error,
                    size: 0,
                };
                let _ = util::fs::write_to_path(&path_clone, size.to_string());
            }
        }
    });

    Ok(())
}

pub fn get_size(repo: &LocalRepository) -> Result<RepoSizeFile, OxenError> {
    let path = repo_size_path(repo);
    let size = util::fs::read_from_path(&path);
    match size {
        Ok(size) => {
            let size: RepoSizeFile = serde_json::from_str(&size)?;
            Ok(size)
        }
        Err(e) => {
            log::info!("Size file not found, creating it: {e}");
            update_size(repo)?;
            get_size(repo)
        }
    }
}

pub fn repo_size_path(repo: &LocalRepository) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path).join("repo_size.toml")
}
