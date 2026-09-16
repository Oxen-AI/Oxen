use rocksdb::{BlockBasedOptions, LogLevel, Options};

/// The block-based table format every oxen database is written in. Raising it makes new
/// databases unreadable to oxen releases built against an older RocksDB.
pub const TABLE_FORMAT_VERSION: i32 = 6;

pub fn default() -> Options {
    let mut opts = Options::default();
    set_table_format(&mut opts);
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

/// Writes tables in [`TABLE_FORMAT_VERSION`] for databases opened with `opts`.
pub fn set_table_format(opts: &mut Options) {
    let mut table_opts = BlockBasedOptions::default();
    table_opts.set_format_version(TABLE_FORMAT_VERSION);
    opts.set_block_based_table_factory(&table_opts);
}

#[cfg(test)]
mod tests {
    use rocksdb::{DBWithThreadMode, MultiThreaded};

    use super::*;
    use crate::error::OxenError;
    use crate::test;

    /// Trailing eight bytes of an SST footer, marking it a block-based table.
    const BLOCK_BASED_TABLE_MAGIC: u64 = 0x88e2_41b7_85f4_cff7;

    #[test]
    fn test_tables_are_written_in_the_pinned_format_version() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let db_path = dir.join("db");
            {
                let db: DBWithThreadMode<MultiThreaded> =
                    DBWithThreadMode::open(&default(), &db_path)?;
                for i in 0..1000 {
                    db.put(format!("key{i:05}"), format!("value{i}"))?;
                }
                db.flush()?;
            }

            let mut tables = 0;
            for entry in std::fs::read_dir(&db_path)? {
                let path = entry?.path();
                if path.extension().and_then(|e| e.to_str()) != Some("sst") {
                    continue;
                }

                let sst = std::fs::read(&path)?;
                let end = sst.len();
                let magic = u64::from_le_bytes(sst[end - 8..].try_into().unwrap());
                assert_eq!(magic, BLOCK_BASED_TABLE_MAGIC, "{path:?} is not an SST");
                let version = i32::from_le_bytes(sst[end - 12..end - 8].try_into().unwrap());
                assert_eq!(version, TABLE_FORMAT_VERSION, "{path:?}");
                tables += 1;
            }
            assert!(tables > 0, "flush wrote no tables under {db_path:?}");

            Ok(())
        })
    }
}
