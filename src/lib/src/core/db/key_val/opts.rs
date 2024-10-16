use rocksdb::{LogLevel, Options};

pub fn default() -> Options {
    let mut opts = Options::default();
    opts.set_log_level(LogLevel::Fatal);
    opts.create_if_missing(true);
    opts.set_max_log_file_size(0);
    opts.set_keep_log_file_num(1);
    opts.set_max_manifest_file_size(1);
    opts.set_max_file_opening_threads(num_cpus::get() as i32);
    opts.set_skip_stats_update_on_db_open(true);
    let max_open_files = std::env::var("MAX_OPEN_FILES")
        .map_or(128, |v| v.parse().expect("MAX_OPEN_FILES must be a number"));
    opts.set_max_open_files(max_open_files);

    opts
}
