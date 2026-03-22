use crate::core::progress::sync_progress::{SyncProgress, SyncType};
use std::borrow::Cow;
use std::ops::{Deref, DerefMut};

pub struct PushProgress {
    sync_progress: SyncProgress,
}

impl Default for PushProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl PushProgress {
    pub fn new() -> Self {
        PushProgress {
            sync_progress: SyncProgress::new(SyncType::Push),
        }
    }

    pub fn new_with_totals(total_files: u64, total_bytes: u64) -> Self {
        PushProgress {
            sync_progress: SyncProgress::new_with_totals(SyncType::Push, total_files, total_bytes),
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

impl Deref for PushProgress {
    type Target = SyncProgress;

    fn deref(&self) -> &Self::Target {
        &self.sync_progress
    }
}

impl DerefMut for PushProgress {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sync_progress
    }
}
