use parking_lot::Mutex;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::LazyLock;
use utoipa::ToSchema;

use crate::core::repo_locks;
use crate::error::OxenError;
use crate::util::fs::AtomicFile;
use crate::{model::LocalRepository, util};
use std::path::PathBuf;

#[derive(Serialize, Debug, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SizeStatus {
    Pending,
    Done,
    Error,
}

/// A repository's size in bytes together with the state of the calculation behind it. The figure is
/// the last one a pass completed, or zero when none has.
#[derive(Serialize, Debug, Clone, ToSchema)]
pub struct RepoSizeFile {
    pub status: SizeStatus,
    pub size: u64,
}

/// What only this process knows about a repository's size: how many passes are walking it, and
/// whether the last one to finish failed. Neither survives the process, so a read after a restart
/// reports the last figure a walk completed.
#[derive(Default)]
struct PassState {
    walking: usize,
    last_failed: bool,
}

/// One entry per repository with a pass walking it or a failure left to report, and nothing for any
/// other repository.
static PASSES: LazyLock<Mutex<HashMap<PathBuf, PassState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Counts a repository as walking until it drops, reporting a failure unless
/// [`PassMarker::finish`] says otherwise, so a walk killed mid-pass both frees the repository and
/// is reported as one that did not land.
struct PassMarker {
    repo_path: PathBuf,
    failed: bool,
}

impl PassMarker {
    fn new(repo_path: PathBuf) -> Self {
        PASSES.lock().entry(repo_path.clone()).or_default().walking += 1;
        Self {
            repo_path,
            failed: true,
        }
    }

    /// Publish how the pass ended and release the repository.
    fn finish(mut self, failed: bool) {
        self.failed = failed;
    }
}

impl Drop for PassMarker {
    fn drop(&mut self) {
        let mut passes = PASSES.lock();
        let Some(state) = passes.get_mut(&self.repo_path) else {
            return;
        };
        state.walking = state.walking.saturating_sub(1);
        state.last_failed = self.failed;
        if state.walking == 0 && !state.last_failed {
            passes.remove(&self.repo_path);
        }
    }
}

/// Recalculate `repo`'s size on a background thread, leaving the figure already recorded readable
/// while it runs. Call it wherever version files become referenced by the merkle tree.
///
/// Only a completed pass records anything, so the figure on disk is always one a walk finished.
/// Each call starts its own pass and the last to finish is the figure that sticks. Returns
/// [`OxenError::LockTimeout`] without starting a pass while a maintenance operation holds `repo`,
/// leaving the figure already recorded as it is.
pub fn update_size(repo: &LocalRepository) -> Result<(), OxenError> {
    // An exclusive maintenance operation drains this write before it runs, so the walk never
    // reads a store that is being deleted or migrated.
    let write = repo_locks::begin_write(repo)?;

    let marker = PassMarker::new(repo.path.clone());
    let repo = repo.clone();

    // Spawn background thread for size calculation
    std::thread::spawn(move || {
        // The write stays in flight past the walk and the handle the walk opens.
        let _write = write;

        let failed = match repo.version_bytes() {
            Ok(total) => {
                let recorded =
                    AtomicFile::new(repo_size_path(&repo)).write(total.to_string().as_bytes());
                match recorded {
                    Ok(()) => {
                        remove_legacy_size_file(&repo);
                        false
                    }
                    Err(e) => {
                        tracing::error!(
                            repo = ?repo.path,
                            cause = ?e,
                            "Could not record a repository's recalculated size"
                        );
                        true
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    repo = ?repo.path,
                    cause = ?e,
                    "Could not calculate a repository's size"
                );
                true
            }
        };

        // Released before the write ends, so a drained operation finds no handle from this walk.
        drop(repo);

        marker.finish(failed);
    });

    Ok(())
}

/// The figure recorded for `repo` and the state of the calculation behind it, starting a
/// recalculation when nothing is recorded.
///
/// `Pending` and `Error` describe passes in this process, so a repository whose walk a restart
/// killed reports the figure that walk was replacing. A recalculation a maintenance operation
/// refuses is `Error` as well.
pub fn get_size(repo: &LocalRepository) -> RepoSizeFile {
    let (walking, last_failed) = PASSES
        .lock()
        .get(&repo.path)
        .map_or((0, false), |state| (state.walking, state.last_failed));
    // Absent when there is no record, and `Some(Err(..))` for a record holding something other
    // than a figure, which counts the same as nothing recorded.
    let recorded = util::fs::read_from_path(repo_size_path(repo))
        .ok()
        .map(|content| content.trim().parse::<u64>());
    let figure = recorded
        .as_ref()
        .and_then(|parsed| parsed.as_ref().ok())
        .copied();

    let status = if walking > 0 {
        SizeStatus::Pending
    } else if last_failed {
        SizeStatus::Error
    } else if figure.is_some() {
        SizeStatus::Done
    } else {
        match &recorded {
            Some(Err(cause)) => tracing::error!(
                repo = ?repo.path,
                ?cause,
                "Replacing a recorded repository size that is not a figure"
            ),
            _ => log::info!(
                "No size recorded for {:?}, starting a recalculation",
                repo.path
            ),
        }
        match update_size(repo) {
            Ok(()) => SizeStatus::Pending,
            Err(cause) => {
                tracing::error!(
                    repo = ?repo.path,
                    ?cause,
                    "Could not start the recalculation a repository with no figure needs"
                );
                SizeStatus::Error
            }
        }
    };

    RepoSizeFile {
        status,
        size: figure.unwrap_or(0),
    }
}

/// Where `repo`'s recorded size is kept, as a JSON number.
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

/// Poll the figure recorded for `repo` until a recalculation lands, erroring on a reported failure
/// and panicking past 30s.
#[cfg(test)]
pub(crate) fn wait_for_recorded_size(repo: &LocalRepository) -> Result<u64, OxenError> {
    use std::time::{Duration, Instant};

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let recorded = get_size(repo);
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
