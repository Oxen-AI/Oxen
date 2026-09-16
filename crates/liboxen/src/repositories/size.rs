use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;

use crate::core::repo_locks;
use crate::util::fs::AtomicFile;
use crate::{error::OxenError, model::LocalRepository, util};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, ToSchema)]
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

/// A repository's size in bytes together with the state of the calculation behind it. On a
/// `Pending` or `Error` status the figure is the last one a pass completed, or zero when none has.
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
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

/// Recalculate `repo`'s size on a background thread, leaving the recorded figure readable
/// while it runs. Call it wherever version files become referenced by the merkle tree.
///
/// Each call starts its own pass, and the last one to finish is the figure that sticks. A failed
/// pass records the failure and keeps the figure from before it. No pass starts while a maintenance
/// operation holds `repo`, and the figure already recorded stays as it is.
pub fn update_size(repo: &LocalRepository) -> Result<(), OxenError> {
    // An exclusive maintenance operation drains this write before it runs, so the walk never
    // reads a store that is being deleted or migrated.
    let Ok(write) = repo_locks::begin_write(repo) else {
        log::info!("Skipping a size recalculation while the repository is held for maintenance");
        return Ok(());
    };

    let path = repo_size_path(repo);
    let pending = match util::fs::read_from_path(&path) {
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
            remove_legacy_size_file(repo);

            RepoSizeFile {
                status: SizeStatus::Pending,
                size: 0,
            }
        }
    };

    AtomicFile::new(&path).write(pending.to_string().as_bytes())?;

    let repo = repo.clone();

    // Spawn background thread for size calculation
    std::thread::spawn(move || {
        // The write stays in flight past the walk and the handle the walk opens.
        let _write = write;

        let recorded = match repo.version_bytes() {
            Ok(calculated) => RepoSizeFile {
                status: SizeStatus::Done,
                size: calculated,
            },
            Err(e) => {
                log::error!("Failed to calculate repository size: {e}");
                RepoSizeFile {
                    status: SizeStatus::Error,
                    size: pending.size,
                }
            }
        };

        // Released before the write ends, so a drained operation finds no handle from this walk.
        drop(repo);

        if let Err(e) = AtomicFile::new(&path).write(recorded.to_string().as_bytes()) {
            log::error!("Failed to write the recalculated size: {e}");
        }
    });

    Ok(())
}

/// The figure recorded for `repo`, starting a recalculation when there is none yet. A repository
/// with nothing recorded reads as pending at zero until a pass lands.
pub fn get_size(repo: &LocalRepository) -> Result<RepoSizeFile, OxenError> {
    let path = repo_size_path(repo);
    if let Ok(recorded) = util::fs::read_from_path(&path) {
        return Ok(serde_json::from_str(&recorded)?);
    }

    log::info!("Size file not found, creating it: {path:?}");
    update_size(repo)?;
    match util::fs::read_from_path(&path) {
        Ok(recorded) => Ok(serde_json::from_str(&recorded)?),
        Err(_) => Ok(RepoSizeFile {
            status: SizeStatus::Pending,
            size: 0,
        }),
    }
}

/// Where `repo`'s recorded size is kept, as JSON.
pub fn repo_size_path(repo: &LocalRepository) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path).join("repo_size.json")
}

/// Drop the size an older format kept under a `.toml` name, which nothing reads.
fn remove_legacy_size_file(repo: &LocalRepository) {
    let legacy = util::fs::oxen_hidden_dir(&repo.path).join("repo_size.toml");
    if legacy.exists()
        && let Err(err) = util::fs::remove_file(&legacy)
    {
        log::warn!("Failed to remove {legacy:?}: {err}");
    }
}

/// Poll the figure recorded for `repo` until a recalculation lands, erroring on a recorded failure
/// and panicking past 30s.
#[cfg(test)]
pub(crate) fn wait_for_recorded_size(repo: &LocalRepository) -> Result<u64, OxenError> {
    use std::time::{Duration, Instant};

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let recorded = get_size(repo)?;
        match recorded.status {
            SizeStatus::Done => return Ok(recorded.size),
            SizeStatus::Error => {
                return Err(OxenError::internal_error(
                    "the size recalculation recorded an error, which the log details",
                ));
            }
            SizeStatus::Pending => {}
        }
        assert!(
            Instant::now() < deadline,
            "the size stayed pending past the deadline"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}
