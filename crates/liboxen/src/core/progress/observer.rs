//! Report an operation's progress, as values of a type the operation chooses, to a caller that
//! shows it somewhere other than the terminal.
//!
//! An operation that reports progress takes an `Option<ProgressObserver<T>>`, with `T` its own
//! update type, and runs its work under `ProgressObserver::observe_while`, which hands the work a
//! `ProgressReporter` to send updates through.

use std::sync::Arc;
use tokio::sync::watch;

use crate::error::OxenError;
use crate::tasks;

/// Called with an operation's latest progress update, a `T`.
///
/// It runs on a blocking-pool thread of its own, never on the threads doing the operation, so it
/// may block. Updates sent between two calls, or before the first, replace one another rather than
/// queue, so each call gets the latest and a slow observer never holds the operation back. Updates
/// arrive in the order they were sent, and the first call may already show progress made.
#[derive(Clone)]
pub struct ProgressObserver<T>(Arc<dyn Fn(T) + Send + Sync>);

impl<T> ProgressObserver<T> {
    pub fn new(on_update: impl Fn(T) + Send + Sync + 'static) -> Self {
        Self(Arc::new(on_update))
    }
}

impl<T: Clone + Default + Send + Sync + 'static> ProgressObserver<T> {
    /// Run `operation`, calling `observer`, if there is one, on a blocking-pool thread with each
    /// update sent to the [`ProgressReporter`] `operation` is given. Returns only after the
    /// observer has returned from the last update, whether or not the operation succeeded, and
    /// gives the operation's error ahead of an observer's panic. Nothing `operation` starts may keep
    /// the reporter past its return, or this waits for it.
    pub(crate) async fn observe_while<R>(
        observer: Option<Self>,
        operation: impl AsyncFnOnce(ProgressReporter<T>) -> Result<R, OxenError>,
    ) -> Result<R, OxenError> {
        let Some(observer) = observer else {
            return operation(ProgressReporter::default()).await;
        };
        // The receiver starts with this value marked as seen, so the observer is never called
        // with it unless the operation sends it again.
        let (sender, mut receiver) = watch::channel(T::default());
        let on_update = observer.0;
        let observing = tasks::spawn_blocking(move || {
            // Waiting on the channel needs only a thread to park, not a runtime, and this thread
            // is the observer's alone.
            while futures::executor::block_on(receiver.changed()).is_ok() {
                let update = receiver.borrow_and_update().clone();
                on_update(update);
            }
        });
        // The operation's future, and with it every sender, is dropped once it completes, which
        // ends the observer's loop after the last update.
        let operated = operation(ProgressReporter(Some(sender))).await;
        let observed = observing.await.map_err(|cause| OxenError::JoinError {
            context: "progress observer panicked: ".to_string(),
            cause,
        });
        let value = operated?;
        observed?;
        Ok(value)
    }
}

/// Where an operation sends its progress: to the [`ProgressObserver`] that
/// [`ProgressObserver::observe_while`] runs it under, or nowhere when there is none, as the default
/// reporter does.
#[derive(Default)]
pub(crate) struct ProgressReporter<T>(Option<watch::Sender<T>>);

impl<T: Clone + PartialEq> ProgressReporter<T> {
    /// Change the latest update in place, waking the observer only if `change` left it different.
    /// `change` runs under the channel's lock, so changes from concurrent tasks apply one at a
    /// time, in the order the observer sees them. With no observer, `change` never runs.
    pub(crate) fn update(&self, change: impl FnOnce(&mut T)) {
        let Some(sender) = &self.0 else {
            return;
        };
        sender.send_if_modified(|update| {
            let before = update.clone();
            change(update);
            *update != before
        });
    }
}

#[cfg(test)]
impl<T> ProgressReporter<T> {
    /// A reporter that sends to `sender`, for a test that reads every update off the channel.
    pub(crate) fn sending_to(sender: watch::Sender<T>) -> Self {
        Self(Some(sender))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;

    #[test]
    fn test_update_wakes_the_observer_only_when_the_update_changes() {
        let (sender, mut receiver) = watch::channel(0_u64);
        let reporter = ProgressReporter::sending_to(sender);

        reporter.update(|count| *count = 0);
        assert!(
            !receiver.has_changed().unwrap(),
            "setting the value it already has"
        );
        reporter.update(|count| *count += 2);
        assert!(receiver.has_changed().unwrap());
        assert_eq!(*receiver.borrow_and_update(), 2);
    }

    #[test]
    fn test_update_never_runs_without_an_observer() {
        ProgressReporter::<u64>::default().update(|_| panic!("there is no observer to update"));
    }

    #[tokio::test]
    async fn test_observer_sees_the_last_update_before_observe_while_returns() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let observer = ProgressObserver::new({
            let seen = Arc::clone(&seen);
            move |update: u64| seen.lock().push(update)
        });

        let value = ProgressObserver::observe_while(Some(observer), async |reporter| {
            for _ in 0..3 {
                reporter.update(|count| *count += 1);
            }
            Ok("done")
        })
        .await
        .expect("neither the operation failed nor the observer panicked");

        assert_eq!(value, "done");
        assert_eq!(
            seen.lock().last().copied(),
            Some(3),
            "the last update, seen before `observe_while` returned"
        );
    }
}
