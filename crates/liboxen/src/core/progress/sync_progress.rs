use indicatif::{ProgressBar, ProgressStyle};
use std::{
    borrow::Cow,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// How far a push or pull has got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Receives a [`SyncProgressUpdate`] each time a transfer's counts change, for a caller that shows
/// progress somewhere other than the terminal.
///
/// It is called from the tasks doing the transfer, many times a second and from several at once,
/// so it must return quickly, and two updates can arrive out of order.
#[derive(Clone)]
pub struct ProgressObserver(Arc<dyn Fn(SyncProgressUpdate) + Send + Sync>);

impl ProgressObserver {
    pub fn new(on_update: impl Fn(SyncProgressUpdate) + Send + Sync + 'static) -> Self {
        ProgressObserver(Arc::new(on_update))
    }
}

impl fmt::Debug for ProgressObserver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProgressObserver")
    }
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
    observer: Option<ProgressObserver>,
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
            observer: None,
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
            observer: None,
        }
    }

    /// Report every change in the counts to `observer`, starting with where they stand now.
    pub(crate) fn set_observer(&mut self, observer: ProgressObserver) {
        self.observer = Some(observer);
        self.notify_observer();
    }

    fn notify_observer(&self) {
        let Some(observer) = &self.observer else {
            return;
        };
        (observer.0)(SyncProgressUpdate {
            files: self.get_num_files(),
            total_files: self.total_files,
            bytes: self.get_num_bytes(),
            total_bytes: self
                .total_bytes
                .as_ref()
                .map(|total| total.load(Ordering::Relaxed)),
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
        self.notify_observer();
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
        self.notify_observer();
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
    use std::sync::Mutex;

    #[test]
    fn test_observer_is_told_each_change_in_the_counts() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut progress = SyncProgress::new_with_totals(SyncType::Push, 2, 30);
        progress.set_observer(ProgressObserver::new({
            let seen = Arc::clone(&seen);
            move |update| seen.lock().expect("no observer panicked").push(update)
        }));
        progress.add_bytes(10);
        progress.add_files(1);
        progress.inc_total_bytes(5);

        let update = |files, bytes, total_bytes| SyncProgressUpdate {
            files,
            total_files: Some(2),
            bytes,
            total_bytes: Some(total_bytes),
        };
        assert_eq!(
            *seen.lock().expect("no observer panicked"),
            [
                update(0, 0, 30),
                update(0, 10, 30),
                update(1, 10, 30),
                update(1, 10, 35),
            ],
            "the starting counts, then one update per change"
        );

        // Without totals there is nothing to report progress against.
        let mut spinner = SyncProgress::new(SyncType::Pull);
        spinner.set_observer(ProgressObserver::new({
            let seen = Arc::clone(&seen);
            move |update| seen.lock().expect("no observer panicked").push(update)
        }));
        spinner.add_bytes(7);
        let last = seen.lock().expect("no observer panicked").pop();
        assert_eq!(
            last,
            Some(SyncProgressUpdate {
                files: 0,
                total_files: None,
                bytes: 7,
                total_bytes: None,
            })
        );
    }
}
