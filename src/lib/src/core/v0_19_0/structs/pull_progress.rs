use crate::core::v0_19_0::structs::sync_progress::{SyncProgress, SyncType};
use std::sync::Arc;

pub struct PullProgress {
    sync_progress: Arc<SyncProgress>,
}

impl PullProgress {
    pub fn new() -> Arc<Self> {
        Arc::new(PullProgress {
            sync_progress: SyncProgress::new(SyncType::Pull),
        })
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
