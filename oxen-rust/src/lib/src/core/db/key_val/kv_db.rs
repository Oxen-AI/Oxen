use crate::error::OxenError;

use rocksdb::{DBWithThreadMode, IteratorMode, ThreadMode};
use std::str;

/// More efficient than get since it does not actual deserialize the value
pub fn has_key<T: ThreadMode, S: AsRef<str>>(db: &DBWithThreadMode<T>, key: S) -> bool {
    let key = key.as_ref();
    let bytes = key.as_bytes();
    match db.get_pinned(bytes) {
        Ok(Some(_value)) => true,
        Ok(None) => false,
        Err(err) => {
            log::error!("Error checking for entry: {err}");
            false
        }
    }
}

/// Count the number of entries in the db
pub fn count<T: ThreadMode>(db: &DBWithThreadMode<T>) -> Result<usize, OxenError> {
    let iter = db.iterator(IteratorMode::Start);
    Ok(iter.count())
}

/// Remove key from database
pub fn delete<T: ThreadMode, S: AsRef<str>>(
    db: &DBWithThreadMode<T>,
    key: S,
) -> Result<(), OxenError> {
    let key = key.as_ref();
    log::debug!("kv_db::delete {:?} from db: {:?}", key, db.path());

    db.delete(key)?;
    Ok(())
}

/// More efficient than `list` since it does not deserialize the values
pub fn list_keys<T: ThreadMode>(db: &DBWithThreadMode<T>) -> Result<Vec<String>, OxenError> {
    let iter = db.iterator(IteratorMode::Start);
    let mut keys: Vec<String> = vec![];
    for item in iter {
        match item {
            Ok((key, _value)) => {
                match str::from_utf8(&key) {
                    Ok(key) => {
                        // return full path
                        keys.push(String::from(key));
                    }
                    _ => {
                        log::error!("list_keys() Could not decode key {key:?}")
                    }
                }
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(keys)
}

/// Remove all values from the db
pub fn clear<T: ThreadMode>(db: &DBWithThreadMode<T>) -> Result<(), OxenError> {
    let iter = db.iterator(IteratorMode::Start);
    for item in iter {
        match item {
            Ok((key, _)) => {
                db.delete(key)?;
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(())
}
