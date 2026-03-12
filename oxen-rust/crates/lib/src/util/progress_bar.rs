use std::sync::Arc;
use tokio::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};

pub enum ProgressBarType {
    Counter,
    Bytes,
    None,
}

pub fn spinner_with_msg(msg: impl AsRef<str>) -> ProgressBar {
    let spinner = ProgressBar::new_spinner();
    spinner.set_message(msg.as_ref().to_owned());
    spinner.set_style(ProgressStyle::default_spinner());
    spinner.enable_steady_tick(Duration::from_millis(100));
    spinner
}

pub fn oxen_progress_bar(size: u64, progress_type: ProgressBarType) -> Arc<ProgressBar> {
    let bar = Arc::new(ProgressBar::new(size));
    bar.set_style(
        ProgressStyle::default_bar()
            .template(progress_type_to_template(progress_type).as_str())
            .unwrap()
            .progress_chars("🌾🐂➖"),
    );
    bar
}

pub fn oxen_progress_bar_indeterminate(
    size: u64,
    progress_type: ProgressBarType,
) -> Arc<ProgressBar> {
    let bar = Arc::new(ProgressBar::new(size));
    bar.set_style(
        ProgressStyle::default_bar()
            .template(progress_type_to_template(progress_type).as_str())
            .unwrap()
            .progress_chars("🌾🐂➖"),
    );
    bar
}

pub fn oxen_progress_bar_with_msg(size: u64, msg: impl AsRef<str>) -> Arc<ProgressBar> {
    let bar = Arc::new(ProgressBar::new(size));
    bar.set_message(msg.as_ref().to_owned());
    bar.set_style(
        ProgressStyle::default_bar()
            .template(progress_type_to_template(ProgressBarType::Counter).as_str())
            .unwrap()
            .progress_chars("🌾🐂➖"),
    );
    bar
}

// Modify styling to oxen bar - necessary for bars which start out as spinners
pub fn oxify_bar(bar: Arc<ProgressBar>, progress_type: ProgressBarType) -> Arc<ProgressBar> {
    bar.set_style(
        ProgressStyle::default_bar()
            .template(progress_type_to_template(progress_type).as_str())
            .unwrap()
            .progress_chars("🌾🐂➖"),
    );
    bar
}

pub fn progress_type_to_template(progress_type: ProgressBarType) -> String {
    match progress_type {
        ProgressBarType::Counter => {
            "{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar}] {pos}/{len}".to_string()
        }
        ProgressBarType::Bytes => {
            "{spinner:.green} [{elapsed_precise}] [{wide_bar}] {bytes}/{total_bytes}".to_string()
        }
        ProgressBarType::None => "{spinner:.green} [{elapsed_precise}] [{wide_bar}]".to_string(),
    }
}
