//! # oxen db list
//!
//! Print out values from an oxen database
//!

use crate::core::v0_19_0::structs::StagedMerkleTreeNode;
use crate::error::OxenError;
use crate::util::progress_bar::spinner_with_msg;

use rocksdb::{IteratorMode, LogLevel, Options, DB};
use std::path::Path;
use std::str;

/// List the key -> value pairs in a database
pub fn list(path: impl AsRef<Path>, limit: Option<usize>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let mut opts = Options::default();
    opts.set_log_level(LogLevel::Fatal);

    let db = DB::open_for_read_only(&opts, dunce::simplified(path), false)?;
    let iter = db.iterator(IteratorMode::Start);
    let mut count = 0;
    for item in iter {
        if let Some(limit) = limit {
            if count >= limit {
                break;
            }
        }

        match item {
            Ok((key, value)) => {
                let key = if let Ok(key) = str::from_utf8(&key) {
                    key.to_string()
                } else {
                    // deserialize as u128
                    if key.len() == 16 {
                        let key: [u8; 16] = (*key).try_into().map_err(|_| {
                            OxenError::basic_str("Could not convert key to [u8; 16]")
                        })?;
                        let key = u128::from_le_bytes(key);
                        format!("{}", key)
                    } else {
                        return Err(OxenError::basic_str(
                            "Could not read iterate over db values",
                        ));
                    }
                };

                // try deserialize as ComputedEntryFields
                let val: Result<StagedMerkleTreeNode, rmp_serde::decode::Error> =
                    rmp_serde::from_slice(&value);
                match val {
                    Ok(val) => {
                        println!("{key}\t{val}");
                    }
                    Err(_) => {
                        if let Ok(val) = str::from_utf8(&value) {
                            println!("{key}\t{val}");
                        } else {
                            println!("{key}\t<binary data>");
                        }
                    }
                }
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
        count += 1;
    }

    println!("{} total entries", count);

    Ok(())
}

/// Count the values in a database
pub fn count(path: impl AsRef<Path>) -> Result<usize, OxenError> {
    let path = path.as_ref();
    let opts = Options::default();
    log::debug!("Opening db at {:?}", path);
    let db = DB::open_for_read_only(&opts, dunce::simplified(path), false)?;
    log::debug!("Opened db at {:?}", path);
    let iter = db.iterator(IteratorMode::Start);
    log::debug!("Iterating over db at {:?}", path);
    let progress = spinner_with_msg(format!("Counting db at {:?}", path));
    let mut count = 0;
    for _ in iter {
        count += 1;
        progress.inc(1);
        progress.set_message(format!("{} entries", count));
    }
    progress.finish_and_clear();
    Ok(count)
}

/// Get a value from a database
pub fn get(
    path: impl AsRef<Path>,
    key: impl AsRef<str>,
    dtype: Option<&str>,
) -> Result<String, OxenError> {
    let path = path.as_ref();
    let str_key = key.as_ref();
    let mut opts = Options::default();
    opts.set_log_level(LogLevel::Fatal);

    let key = if let Some(dtype) = dtype {
        if dtype == "u128" {
            let key = str_key.parse::<u128>()?;
            key.to_le_bytes().to_vec()
        } else {
            str_key.as_bytes().to_vec()
        }
    } else {
        str_key.as_bytes().to_vec()
    };

    log::debug!("Opening db at {:?}", path);
    let db = DB::open_for_read_only(&opts, dunce::simplified(path), false)?;
    log::debug!("Opened db at {:?}", path);

    if let Some(value) = db.get(key)? {
        log::debug!("Got value from db at {:?}", path);
        if let Ok(value) = str::from_utf8(&value) {
            Ok(value.to_string())
        } else {
            Ok(format!("<{} bytes>", value.len()))
        }
    } else {
        Err(OxenError::basic_str(format!("Key {} not found", str_key)))
    }
}
