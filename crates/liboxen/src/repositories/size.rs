use parking_lot::Mutex;
use serde::Serialize;
use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::LazyLock;
use std::thread;
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

/// What only this process knows about a repository's size: whether a pass is walking it, whether a
/// call is waiting on another walk, and whether a failure is left to report. None of it survives
/// the process, so a read after a restart reports the last figure a walk completed.
#[derive(Default)]
struct PassState {
    running: bool,
    /// Set by a call made while `running`: the pass walks once more, with this handle, before it
    /// ends.
    rerun: Option<LocalRepository>,
    last_failed: bool,
}

/// One entry per repository with a pass walking it or a failure left to report, and nothing for any
/// other repository.
static PASSES: LazyLock<Mutex<HashMap<PathBuf, PassState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Recalculate `repo`'s size on a background thread, leaving the figure already recorded readable
/// while it runs. Call it wherever version files become referenced by the merkle tree.
///
/// Only a completed walk records anything, so the figure on disk is always one a walk finished.
/// A repository has at most one pass at a time. A call made while one runs starts nothing: the pass
/// walks once more before it ends, covering every call made in the meantime, so the figure that
/// sticks comes from a walk that began after the latest call, and a failure reported is that of the
/// pass's last walk. Returns [`OxenError::LockTimeout`] without starting or requesting a walk while a
/// maintenance operation holds `repo`, leaving the figure already recorded as it is.
pub fn update_size(repo: &LocalRepository) -> Result<(), OxenError> {
    // An exclusive maintenance operation drains this write before it runs, so no walk reads a store
    // that is being deleted or migrated. A pass holds the write of the call that started it until
    // its last walk ends; a call that only asks for another walk needs no write of its own.
    let write = repo_locks::begin_write(repo)?;
    {
        let mut passes = PASSES.lock();
        let state = passes.entry(repo.path.clone()).or_default();
        if state.running {
            state.rerun.get_or_insert_with(|| repo.clone());
            return Ok(());
        }
        state.running = true;
    }

    let mut next = Some(repo.clone());
    let spawned = thread::Builder::new().spawn(move || {
        let _write = write;
        while let Some(repo) = next.take() {
            // A panicking walk counts as a failed one, so the pass still ends and runs any walk a
            // call asked for.
            let failed =
                panic::catch_unwind(AssertUnwindSafe(|| walk_and_record(&repo))).unwrap_or(true);
            let path = repo.path.clone();
            // Released before the pass reads as over and before the write ends, so neither a read
            // that sees the pass end nor a drained operation finds a handle from this walk.
            drop(repo);

            let mut passes = PASSES.lock();
            let Some(state) = passes.get_mut(&path) else {
                break;
            };
            next = state.rerun.take();
            if next.is_none() {
                state.running = false;
                state.last_failed = failed;
                if !failed {
                    passes.remove(&path);
                }
            }
        }
    });

    if let Err(cause) = spawned {
        let mut passes = PASSES.lock();
        if let Some(state) = passes.get_mut(&repo.path) {
            state.running = false;
            state.rerun = None;
            if !state.last_failed {
                passes.remove(&repo.path);
            }
        }
        return Err(cause.into());
    }
    Ok(())
}

/// Walk `repo` and record its size, returning whether either step failed.
fn walk_and_record(repo: &LocalRepository) -> bool {
    match repo.version_bytes() {
        Ok(total) => {
            #[cfg(test)]
            tests::after_walk(&repo.path);

            match AtomicFile::new(repo_size_path(repo)).write(total.to_string().as_bytes()) {
                Ok(()) => {
                    remove_legacy_size_file(repo);
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
    }
}

/// The figure recorded for `repo` and the state of the calculation behind it, starting a
/// recalculation when nothing is recorded.
///
/// `Pending` and `Error` describe passes in this process, so a repository whose walk a restart
/// killed reports the figure that walk was replacing. A recalculation a maintenance operation
/// refuses is `Error` as well.
pub fn get_size(repo: &LocalRepository) -> RepoSizeFile {
    let (running, last_failed) = PASSES
        .lock()
        .get(&repo.path)
        .map_or((false, false), |state| (state.running, state.last_failed));
    // A record holding anything but a figure counts the same as none.
    let figure = util::fs::read_from_path(repo_size_path(repo))
        .ok()
        .and_then(|content| content.trim().parse::<u64>().ok());

    let status = if running {
        SizeStatus::Pending
    } else if last_failed {
        SizeStatus::Error
    } else if figure.is_some() {
        SizeStatus::Done
    } else {
        tracing::info!(repo = ?repo.path, "No size figure recorded, starting a recalculation");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repositories;
    use crate::test;
    use std::collections::HashSet;
    use std::path::Path;
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::time::Duration;

    /// Walks that completed per repository, the holds `hold_next_walk` installed, and the
    /// repositories whose next walk `panic_next_walk` made panic.
    static WALKS: LazyLock<Mutex<HashMap<PathBuf, usize>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));
    type Hold = (Sender<()>, Receiver<()>);
    static HOLDS: LazyLock<Mutex<HashMap<PathBuf, Hold>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));
    static PANICS: LazyLock<Mutex<HashSet<PathBuf>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

    /// Called by a pass once its walk has summed the repository, before it records the figure.
    /// Counts the walk and, if a hold is installed for the repository, signals and waits to be
    /// released, then panics if `panic_next_walk` asked it to. Holds and panics are one-shot.
    pub(super) fn after_walk(repo_path: &Path) {
        *WALKS.lock().entry(repo_path.to_path_buf()).or_default() += 1;
        let hold = HOLDS.lock().remove(repo_path);
        if let Some((reached, release)) = hold {
            reached
                .send(())
                .expect("the test waiting on the hold should still be listening");
            release
                .recv()
                .expect("the test should release the held pass");
        }
        if PANICS.lock().remove(repo_path) {
            panic!("panic_next_walk asked this walk to panic");
        }
    }

    /// Makes the next walk over the repository at `repo_path` panic before it records anything.
    fn panic_next_walk(repo_path: &Path) {
        PANICS.lock().insert(repo_path.to_path_buf());
    }

    /// Makes the next pass over the repository at `repo_path` hold once it has walked. Returns a
    /// receiver that fires once it holds and a sender that releases it.
    fn hold_next_walk(repo_path: &Path) -> (Receiver<()>, Sender<()>) {
        let (reached_tx, reached_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        HOLDS
            .lock()
            .insert(repo_path.to_path_buf(), (reached_tx, release_rx));
        (reached_rx, release_tx)
    }

    // Calls made while a pass walks cost one more walk between them, and the figure that walk
    // records includes what changed after the walking pass had summed the repository.
    #[tokio::test]
    async fn test_calls_during_a_pass_coalesce_into_one_follow_up() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let (reached, release) = hold_next_walk(&repo.path);
            update_size(&repo)?;
            reached
                .recv_timeout(Duration::from_secs(30))
                .expect("the pass should hold once it has walked");

            let added = repo.path.join("added.txt");
            util::fs::write_to_path(&added, "added while a pass was walking")?;
            repositories::add(&repo, &added).await?;
            repositories::commit(&repo, "Add a file while a pass walks")?;
            let current = repo.version_bytes()?;

            for _ in 0..3 {
                update_size(&repo)?;
            }
            release
                .send(())
                .expect("the held pass should be waiting for its release");

            assert_eq!(
                wait_for_recorded_size(&repo)?,
                current,
                "the follow-up pass should record the size after the commit"
            );
            assert_eq!(
                WALKS.lock().get(&repo.path).copied(),
                Some(2),
                "three calls during the held pass should run exactly one follow-up pass"
            );

            // A walk that panics still ends its pass: a walk a call asked for meanwhile runs, and a
            // pass whose last walk panicked reports a failure.
            let (reached, release) = hold_next_walk(&repo.path);
            panic_next_walk(&repo.path);
            update_size(&repo)?;
            reached
                .recv_timeout(Duration::from_secs(30))
                .expect("the pass should hold once it has walked");
            update_size(&repo)?;
            release
                .send(())
                .expect("the held pass should be waiting for its release");

            assert_eq!(
                wait_for_recorded_size(&repo)?,
                repo.version_bytes()?,
                "the walk asked for during the panicking one should run and record the size"
            );
            assert_eq!(
                WALKS.lock().get(&repo.path).copied(),
                Some(4),
                "the pass should walk once more after its walk panicked"
            );

            panic_next_walk(&repo.path);
            update_size(&repo)?;
            assert!(
                wait_for_recorded_size(&repo).is_err(),
                "a pass whose last walk panicked should report a failure"
            );
            PASSES.lock().remove(&repo.path);
            Ok(())
        })
        .await
    }
}
