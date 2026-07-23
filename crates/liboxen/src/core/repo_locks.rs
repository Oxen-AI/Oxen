//! Process-global whole-repo exclusive lock — lets a maintenance operation (migration, prune,
//! fsck, …) run with no other write touching the repository.
//!
//! # The model
//!
//! Each repo has one gate with two sides:
//!
//! - **Writers** take a *shared* [`RepoWriteGuard`] via [`acquire_write`]. Any number of writers
//!   can hold one at once; they exist so an exclusive operation has something to wait for.
//! - **One exclusive operation at a time** runs inside [`with_repo_exclusive`]. It blocks new
//!   writers, waits for the in-flight ones to finish, then runs with the repo to itself.
//!
//! While an exclusive operation holds a repo, [`acquire_write`] returns [`OxenError::LockTimeout`]
//! (oxen-server maps it to HTTP 429 + `Retry-After`, so clients back off and retry). If in-flight
//! writes don't drain within the timeout, the exclusive acquire itself fails with `LockTimeout`
//! rather than blocking forever.
//!
//! # Wiring a write entry point
//!
//! Acquire the guard at the start of a write and hold it until the write finishes — dropping the
//! guard is what signals the write is done:
//!
//! ```ignore
//! let _write = repo_locks::acquire_write(repo)?; // 429 if a maintenance op holds the repo
//! // ... perform the write; `_write` drops at end of scope, releasing the reservation ...
//! ```
//!
//! # Running an exclusive operation
//!
//! Wrap the work in a future; new writers are rejected and in-flight ones drained before it runs:
//!
//! ```ignore
//! repo_locks::with_repo_exclusive(repo, async {
//!     // ... migration / prune sweep / fsck; no other write can land while this runs ...
//!     Ok(())
//! })
//! .await?;
//! ```
//!
//! # Invariant (deadlock avoidance)
//!
//! Never call [`with_repo_exclusive`] while holding a [`RepoWriteGuard`], and never take a guard
//! from inside the exclusive future: the exclusive side waits for guards to drop, so a guard held
//! across it can never drain (and a guard taken inside it self-rejects with `LockTimeout`). Today
//! the exclusive consumers call no write ops, so the invariant holds; keep it that way.
//!
//! Gates are keyed by repo path and the registry never evicts (one small gate per repo the process
//! has touched).

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::time::timeout;

use crate::error::OxenError;
use crate::model::LocalRepository;

/// Longest `with_repo_exclusive` waits for in-flight writes to drain before returning
/// `LockTimeout`. Bounds only the wait to acquire the lock (writes are short); the exclusive
/// operation itself then runs unbounded. Tune once op-latency logging exists.
const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

struct GateState {
    exclusive: bool,
    active_writes: usize,
}

struct RepoGate {
    state: Mutex<GateState>,
    drained: Notify,
    exclusive_slot: AsyncMutex<()>,
}

static REGISTRY: LazyLock<Mutex<HashMap<PathBuf, Arc<RepoGate>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn gate_for(repo: &LocalRepository) -> Arc<RepoGate> {
    REGISTRY
        .lock()
        .entry(repo.path.clone())
        .or_insert_with(|| {
            Arc::new(RepoGate {
                state: Mutex::new(GateState {
                    exclusive: false,
                    active_writes: 0,
                }),
                drained: Notify::new(),
                exclusive_slot: AsyncMutex::new(()),
            })
        })
        .clone()
}

fn lock_timeout() -> OxenError {
    OxenError::LockTimeout("The repository is locked for maintenance. Try again later.".into())
}

/// A held write reservation on a repo. An exclusive acquire on the same repo waits for every
/// outstanding guard to drop. Acquire it at a write entry point and hold it for the whole write.
pub struct RepoWriteGuard {
    gate: Arc<RepoGate>,
}

impl Drop for RepoWriteGuard {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock();
        state.active_writes -= 1;
        if state.active_writes == 0 {
            self.gate.drained.notify_waiters();
        }
    }
}

/// Reserve a write on `repo`, or `LockTimeout` if an exclusive operation currently holds it.
pub fn acquire_write(repo: &LocalRepository) -> Result<RepoWriteGuard, OxenError> {
    let gate = gate_for(repo);
    let mut state = gate.state.lock();
    if state.exclusive {
        return Err(lock_timeout());
    }
    state.active_writes += 1;
    drop(state);
    Ok(RepoWriteGuard { gate })
}

/// Sets the exclusive marker on construction and clears it on drop, so an early return or panic in
/// the exclusive section can never leave a repo wedged.
struct ExclusiveMarker {
    gate: Arc<RepoGate>,
}

impl ExclusiveMarker {
    fn set(gate: Arc<RepoGate>) -> Self {
        gate.state.lock().exclusive = true;
        Self { gate }
    }
}

impl Drop for ExclusiveMarker {
    fn drop(&mut self) {
        self.gate.state.lock().exclusive = false;
    }
}

/// Run `work` with exclusive access to `repo`: block new writers, drain in-flight ones (up to
/// `DEFAULT_DRAIN_TIMEOUT`), then run it. Returns `LockTimeout` if the drain deadline passes.
pub async fn with_repo_exclusive<T>(
    repo: &LocalRepository,
    work: impl Future<Output = Result<T, OxenError>>,
) -> Result<T, OxenError> {
    with_repo_exclusive_with_timeout(repo, DEFAULT_DRAIN_TIMEOUT, work).await
}

/// `with_repo_exclusive` with an explicit drain deadline.
pub(crate) async fn with_repo_exclusive_with_timeout<T>(
    repo: &LocalRepository,
    drain_timeout: Duration,
    work: impl Future<Output = Result<T, OxenError>>,
) -> Result<T, OxenError> {
    let gate = gate_for(repo);
    // At most one exclusive holder per repo at a time.
    let _slot = gate.exclusive_slot.lock().await;
    // Block new writers, then wait for in-flight ones to finish.
    let _marker = ExclusiveMarker::set(gate.clone());
    timeout(drain_timeout, drain(&gate))
        .await
        .map_err(|_| lock_timeout())?;
    work.await
}

/// Resolve once `gate` has no in-flight writes. Registers for the drain wake before checking the
/// count so a guard drop between the check and the await isn't missed.
async fn drain(gate: &RepoGate) {
    loop {
        let notified = gate.drained.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if gate.state.lock().active_writes == 0 {
            return;
        }
        notified.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;
    use tokio::sync::oneshot;

    // A write is rejected with LockTimeout while an exclusive op holds the repo, and allowed again
    // once it releases.
    #[tokio::test]
    async fn test_exclusive_rejects_new_writers() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let (held_tx, held_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();

            let repo_excl = repo.clone();
            let handle = tokio::spawn(async move {
                with_repo_exclusive(&repo_excl, async move {
                    held_tx.send(()).expect("signal exclusive held");
                    release_rx.await.expect("await release");
                    Ok::<(), OxenError>(())
                })
                .await
            });

            held_rx.await.expect("exclusive should be held");
            assert!(
                matches!(acquire_write(&repo), Err(OxenError::LockTimeout(_))),
                "a write must be rejected while the exclusive lock is held"
            );

            release_tx.send(()).expect("release the exclusive op");
            handle.await.expect("join exclusive task")?;

            // Released: writes succeed again.
            let _guard = acquire_write(&repo)?;
            Ok(())
        })
        .await
    }

    // An exclusive acquire waits for an outstanding write guard to drop before it proceeds.
    #[tokio::test]
    async fn test_exclusive_waits_for_in_flight_writes_to_drain() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let guard = acquire_write(&repo)?;

            let repo_excl = repo.clone();
            let (done_tx, mut done_rx) = oneshot::channel();
            let handle = tokio::spawn(async move {
                with_repo_exclusive(&repo_excl, async { Ok::<(), OxenError>(()) })
                    .await
                    .expect("exclusive op");
                done_tx.send(()).expect("signal done");
            });

            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                done_rx.try_recv().is_err(),
                "exclusive proceeded before the in-flight write drained"
            );

            drop(guard);
            done_rx.await.expect("exclusive should proceed after drain");
            handle.await.expect("join exclusive task");
            Ok(())
        })
        .await
    }

    // The drain wait is bounded: a write that never drains yields LockTimeout.
    #[tokio::test]
    async fn test_exclusive_times_out_when_writes_never_drain() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let guard = acquire_write(&repo)?;
            let result =
                with_repo_exclusive_with_timeout(&repo, Duration::from_millis(100), async {
                    Ok::<(), OxenError>(())
                })
                .await;
            assert!(
                matches!(result, Err(OxenError::LockTimeout(_))),
                "an undrained write must time out the exclusive acquire"
            );
            drop(guard);

            // The timed-out acquire must clear the exclusive marker on its way out (via
            // ExclusiveMarker's Drop), leaving the repo writable again rather than wedged.
            assert!(
                acquire_write(&repo).is_ok(),
                "a drain timeout must leave the repo unwedged (exclusive marker cleared)"
            );
            Ok(())
        })
        .await
    }
}
