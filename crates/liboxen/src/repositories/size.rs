use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;

use crate::core::repo_locks;
use crate::util::fs::AtomicFile;
use crate::{error::OxenError, model::LocalRepository, util};
use std::path::{Path, PathBuf};

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
    /// Why the last pass failed. Only carried by an `Error` record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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
pub fn update_size(repo: &LocalRepository) -> Result<RepoSizeFile, OxenError> {
    let path = repo_size_path(repo);

    // An exclusive maintenance operation drains this reservation before it runs, so the walk
    // never reads a store that is being deleted or migrated.
    let Ok(write) = repo_locks::acquire_write(repo) else {
        log::info!("Skipping a size recalculation while the repository is held for maintenance");
        // Nothing is recorded, so report the last figure as pending: it stands until maintenance
        // finishes and a later call recalculates.
        return Ok(RepoSizeFile {
            status: SizeStatus::Pending,
            size: read_stored_size(&path).map_or(0, |stored| stored.size),
            error: None,
        });
    };

    let stored = read_stored_size(&path);
    if stored.is_none() {
        remove_legacy_size_file(repo);
    }
    // No record just means there is no earlier figure to carry forward.
    let last_known_size = stored.map_or(0, |stored| stored.size);

    let pending = RepoSizeFile {
        status: SizeStatus::Pending,
        size: last_known_size,
        error: None,
    };
    AtomicFile::new(&path).write(pending.to_string().as_bytes())?;

    let repo = repo.clone();

    // Spawn background thread for size calculation
    std::thread::spawn(move || {
        // The reservation outlives the walk and the handle the walk opens.
        let _write = write;

        let recorded = match repo.version_bytes() {
            Ok(calculated) => RepoSizeFile {
                status: SizeStatus::Done,
                size: calculated,
                error: None,
            },
            // Keep the cause on the record. Nothing else survives this thread, so without it the
            // next reader knows only that the figure is stale, not why.
            Err(e) => RepoSizeFile {
                status: SizeStatus::Error,
                size: last_known_size,
                error: Some(e.to_string()),
            },
        };

        // Released before the reservation, so a drained operation finds no handle from this walk.
        drop(repo);

        if let Err(e) = AtomicFile::new(&path).write(recorded.to_string().as_bytes()) {
            log::error!("Failed to write the recalculated size: {e}");
        }
    });

    Ok(pending)
}

/// The figure recorded for `repo`, starting a recalculation when there is none yet or when a
/// failed pass left one behind. No status is terminal: a repository whose size calculation failed
/// recovers on the next read, and reads as pending at the last figure until the new pass lands.
pub fn get_size(repo: &LocalRepository) -> Result<RepoSizeFile, OxenError> {
    let path = repo_size_path(repo);

    match read_stored_size(&path) {
        Some(stored) if stored.status != SizeStatus::Error => Ok(stored),
        Some(failed) => {
            tracing::error!(
                repo = ?repo.path,
                cause = failed.error.as_deref().unwrap_or("unknown"),
                "Repo size calculation failed, recalculating"
            );
            // `update_size` hands back the record it just wrote, so recovery neither re-reads the
            // file nor recurses.
            update_size(repo)
        }
        None => update_size(repo),
    }
}

/// The recorded figure, or `None` when there is none or it cannot be parsed.
fn read_stored_size(path: &Path) -> Option<RepoSizeFile> {
    let contents = util::fs::read_from_path(path)
        .inspect_err(|e| log::info!("Size file not found: {e}"))
        .ok()?;
    serde_json::from_str(&contents)
        .inspect_err(|e| log::warn!("Size file could not be parsed, recalculating: {e}"))
        .ok()
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

    let path = repo_size_path(repo);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        // Read the record rather than going through `get_size`, which restarts a failed pass and
        // would leave this polling a fresh one instead of reporting the failure.
        if let Some(recorded) = read_stored_size(&path) {
            match recorded.status {
                SizeStatus::Done => return Ok(recorded.size),
                SizeStatus::Error => {
                    return Err(OxenError::internal_error(format!(
                        "the size recalculation recorded an error: {}",
                        recorded.error.as_deref().unwrap_or("unknown")
                    )));
                }
                SizeStatus::Pending => {}
            }
        }
        assert!(
            Instant::now() < deadline,
            "the size stayed pending past the deadline"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;

    #[test]
    fn test_get_size_recalculates_after_a_failed_calculation() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let wedged = RepoSizeFile {
                status: SizeStatus::Error,
                size: 42,
                error: Some("No such file or directory".to_string()),
            };
            util::fs::write_to_path(repo_size_path(&repo), wedged.to_string())?;

            let size = get_size(&repo)?;
            assert_ne!(
                size.status,
                SizeStatus::Error,
                "an error status must not stick"
            );
            assert_eq!(size.size, 42, "the last figure carries into the retry");

            // The restarted pass runs to completion rather than just flipping the status.
            assert_eq!(wait_for_recorded_size(&repo)?, repo.version_bytes()?);

            Ok(())
        })
    }
}
