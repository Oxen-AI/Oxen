use indicatif::{ProgressBar, ProgressStyle};
use std::{
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::core::progress::observer::ProgressReporter;

/// How far a push or pull has got, as a [`ProgressObserver`] sees it. A transfer's counts never go
/// down from one update to the next.
///
/// [`ProgressObserver`]: crate::core::progress::observer::ProgressObserver
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SyncProgressUpdate {
    /// Files transferred so far.
    pub files: u64,
    /// Files the transfer moves in all, when that is known before it starts.
    pub total_files: Option<u64>,
    /// Bytes transferred so far.
    pub bytes: u64,
    /// Bytes the transfer moves in all, when that is known before it starts. It grows during a
    /// push as merkle tree nodes are added to the file bytes.
    pub total_bytes: Option<u64>,
}

pub enum SyncType {
    Push,
    Pull,
}

impl SyncType {
    pub fn as_str(&self) -> &str {
        match self {
            SyncType::Push => "push",
            SyncType::Pull => "pull",
        }
    }
}

pub struct SyncProgress {
    sync_type: SyncType,
    byte_counter: Arc<AtomicU64>,
    file_counter: Arc<AtomicU64>,
    progress_bar: ProgressBar,
    total_files: Option<u64>,
    total_bytes: Option<AtomicU64>,
    reporter: ProgressReporter<SyncProgressUpdate>,
}

impl SyncProgress {
    pub fn new(sync_type: SyncType) -> Self {
        let progress_bar = ProgressBar::new_spinner();
        progress_bar.set_style(ProgressStyle::default_spinner());
        progress_bar.enable_steady_tick(std::time::Duration::from_millis(100));

        SyncProgress {
            sync_type,
            byte_counter: Arc::new(AtomicU64::new(0)),
            file_counter: Arc::new(AtomicU64::new(0)),
            progress_bar,
            total_files: None,
            total_bytes: None,
            reporter: ProgressReporter::default(),
        }
    }

    pub fn new_with_totals(sync_type: SyncType, total_files: u64, total_bytes: u64) -> Self {
        let progress_bar = ProgressBar::new(total_bytes);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar}] {bytes}/{total_bytes}",
                )
                .unwrap()
                .progress_chars("🌾🐂➖"),
        );

        SyncProgress {
            sync_type,
            byte_counter: Arc::new(AtomicU64::new(0)),
            file_counter: Arc::new(AtomicU64::new(0)),
            progress_bar,
            total_files: Some(total_files),
            total_bytes: Some(AtomicU64::new(total_bytes)),
            reporter: ProgressReporter::default(),
        }
    }

    /// Send every change in the counts to `reporter`, starting with where they stand now.
    pub(crate) fn reporting_to(mut self, reporter: ProgressReporter<SyncProgressUpdate>) -> Self {
        self.reporter = reporter;
        self.send_update();
        self
    }

    fn send_update(&self) {
        // Reading the counts inside the update, under the channel's lock, keeps a receiver from
        // seeing this transfer's counts go down.
        self.reporter.update(|update| {
            *update = SyncProgressUpdate {
                files: self.get_num_files(),
                total_files: self.total_files,
                bytes: self.get_num_bytes(),
                total_bytes: self
                    .total_bytes
                    .as_ref()
                    .map(|total| total.load(Ordering::Relaxed)),
            };
        });
    }

    /// Extend the byte total, and the progress bar's length with it, by `delta`. Used when an
    /// additional pre-known byte count needs to be added to a bar that was already
    /// constructed with an initial total (e.g., the merkle-tree upload bytes layered
    /// on top of file-content upload bytes during a push). Uses `ProgressBar`'s
    /// interior mutability, so this works through `&Arc<SyncProgress>`.
    pub fn inc_total_bytes(&self, delta: u64) {
        if let Some(total_bytes) = &self.total_bytes {
            total_bytes.fetch_add(delta, Ordering::Relaxed);
        }
        self.progress_bar.inc_length(delta);
        self.send_update();
    }

    pub fn set_message(&self, message: impl Into<Cow<'static, str>>) {
        self.progress_bar.set_message(message);
    }

    pub fn update_message(&self) {
        let files = self.file_counter.load(Ordering::Relaxed);
        let bytes = self.byte_counter.load(Ordering::Relaxed);
        match (self.total_files, &self.total_bytes) {
            (Some(total_files), Some(_)) => {
                // let message = format!(
                //     "🐂 {} {} ({} / {} files, {} / {} bytes)",
                //     self.sync_type.as_str(),
                //     bytesize::ByteSize::b(bytes),
                //     files,
                //     total_files,
                //     bytesize::ByteSize::b(bytes),
                //     total_bytes
                // );
                // self.progress_bar.set_message(message);
                self.progress_bar.set_message(format!(
                    "🐂 {} ({}/{} files)",
                    self.sync_type.as_str(),
                    files,
                    total_files
                ));
                self.progress_bar.set_position(bytes);
            }
            _ => {
                let message = format!(
                    "🐂 {} ({} files {})",
                    self.sync_type.as_str(),
                    files,
                    bytesize::ByteSize::b(bytes)
                );
                self.progress_bar.set_message(message);
            }
        };
        self.send_update();
    }

    pub fn add_files(&self, files: u64) {
        self.file_counter.fetch_add(files, Ordering::Relaxed);
        self.update_message();
    }

    pub fn add_bytes(&self, bytes: u64) {
        self.byte_counter.fetch_add(bytes, Ordering::Relaxed);
        self.update_message();
    }

    pub fn get_num_files(&self) -> u64 {
        self.file_counter.load(Ordering::Relaxed)
    }

    pub fn get_num_bytes(&self) -> u64 {
        self.byte_counter.load(Ordering::Relaxed)
    }

    pub fn finish(&self) {
        self.progress_bar.finish_and_clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use tokio::sync::watch;

    /// The update `receiver` has not seen, `Some(None)` once every sender is gone, or `None` when
    /// nothing new was sent. Each update is sent before the call that made it returns, so `changed`
    /// is polled once rather than awaited: a missing update fails an assertion instead of hanging
    /// the test.
    fn next(
        receiver: &mut watch::Receiver<SyncProgressUpdate>,
    ) -> Option<Option<SyncProgressUpdate>> {
        let changed = receiver.changed().now_or_never()?;
        Some(changed.ok().map(|()| *receiver.borrow_and_update()))
    }

    #[test]
    fn test_progress_sends_each_change_in_the_counts() {
        let (sender, mut receiver) = watch::channel(SyncProgressUpdate::default());
        let progress = SyncProgress::new_with_totals(SyncType::Push, 2, 30)
            .reporting_to(ProgressReporter::sending_to(sender.clone()));

        let update = |files, bytes, total_bytes| SyncProgressUpdate {
            files,
            total_files: Some(2),
            bytes,
            total_bytes: Some(total_bytes),
        };
        assert_eq!(
            *receiver.borrow(),
            update(0, 0, 30),
            "the starting counts, sent as soon as the sender is set"
        );
        progress.add_bytes(10);
        assert_eq!(
            next(&mut receiver),
            Some(Some(update(0, 10, 30))),
            "the newest of the updates not yet seen"
        );
        progress.add_files(1);
        assert_eq!(next(&mut receiver), Some(Some(update(1, 10, 30))));
        progress.inc_total_bytes(5);
        assert_eq!(next(&mut receiver), Some(Some(update(1, 10, 35))));

        // Without totals there is nothing to report progress against.
        let spinner =
            SyncProgress::new(SyncType::Pull).reporting_to(ProgressReporter::sending_to(sender));
        spinner.add_bytes(7);
        assert_eq!(
            next(&mut receiver),
            Some(Some(SyncProgressUpdate {
                files: 0,
                total_files: None,
                bytes: 7,
                total_bytes: None,
            }))
        );

        drop((progress, spinner));
        assert_eq!(next(&mut receiver), Some(None), "every sender is gone");
    }
}
