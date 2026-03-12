use indicatif::{ProgressBar, ProgressStyle};
use std::{
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
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
    progress_bar: ProgressBar,
    total_files: Option<u64>,
    total_bytes: Option<u64>,
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
            total_bytes: Some(total_bytes),
        }
    }

    pub fn set_totals(&mut self, total_files: u64, total_bytes: u64) {
        self.total_files = Some(total_files);
        self.total_bytes = Some(total_bytes);
    }

    pub fn set_message(&self, message: impl Into<Cow<'static, str>>) {
        self.progress_bar.set_message(message);
    }

    pub fn update_message(&self) {
        let files = self.file_counter.load(Ordering::Relaxed);
        let bytes = self.byte_counter.load(Ordering::Relaxed);
        match (self.total_files, self.total_bytes) {
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
