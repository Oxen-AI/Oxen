use crate::core::progress::sync_progress::{SyncProgress, SyncType};
use std::borrow::Cow;

pub struct PullProgress {
    sync_progress: SyncProgress,
}

impl Default for PullProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl PullProgress {
    pub fn new() -> Self {
        PullProgress {
            sync_progress: SyncProgress::new(SyncType::Pull),
        }
    }

    pub fn new_with_totals(total_files: u64, total_bytes: u64) -> Self {
        PullProgress {
            sync_progress: SyncProgress::new_with_totals(SyncType::Pull, total_files, total_bytes),
        }
    }

    pub fn set_message(&self, message: impl Into<Cow<'static, str>>) {
        self.sync_progress.set_message(message);
    }

    pub fn update_message(&self) {
        self.sync_progress.update_message();
    }

    pub fn add_files(&self, files: u64) {
        self.sync_progress.add_files(files);
    }

    pub fn add_bytes(&self, bytes: u64) {
        self.sync_progress.add_bytes(bytes);
    }

    pub fn get_num_files(&self) -> u64 {
        self.sync_progress.get_num_files()
    }

    pub fn get_num_bytes(&self) -> u64 {
        self.sync_progress.get_num_bytes()
    }

    pub fn finish(&self) {
        self.sync_progress.finish();
    }
}
