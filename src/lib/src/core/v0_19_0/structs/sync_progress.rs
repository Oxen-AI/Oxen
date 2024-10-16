use indicatif::{ProgressBar, ProgressStyle};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

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
    progress_bar: Arc<ProgressBar>,
}

impl SyncProgress {
    pub fn new(sync_type: SyncType) -> Arc<Self> {
        let progress_bar = Arc::new(ProgressBar::new_spinner());
        progress_bar.set_style(ProgressStyle::default_spinner());
        progress_bar.enable_steady_tick(std::time::Duration::from_millis(100));

        Arc::new(SyncProgress {
            sync_type,
            byte_counter: Arc::new(AtomicU64::new(0)),
            file_counter: Arc::new(AtomicU64::new(0)),
            progress_bar,
        })
    }

    pub fn update_message(&self) {
        let files = self.file_counter.load(Ordering::Relaxed);
        let bytes = self.byte_counter.load(Ordering::Relaxed);
        let message = format!(
            "🐂 {} {} ({} files)",
            self.sync_type.as_str(),
            bytesize::ByteSize::b(bytes),
            files
        );
        self.progress_bar.set_message(message);
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
