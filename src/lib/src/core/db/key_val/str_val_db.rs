use crate::core::db::key_val::kv_db;
use crate::error::OxenError;

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded, ThreadMode};
use std::collections::HashMap;
use std::str;

/// More efficient than get since it does not actual deserialize the entry
pub fn has_key<T: ThreadMode, S: AsRef<str>>(db: &DBWithThreadMode<T>, key: S) -> bool {
    kv_db::has_key(db, key)
}

/// Remove all values from the db
pub fn clear<T: ThreadMode>(db: &DBWithThreadMode<T>) -> Result<(), OxenError> {
    kv_db::clear(db)
}

/// # Removes key from database
pub fn delete<T: ThreadMode, S: AsRef<str>>(
    db: &DBWithThreadMode<T>,
    key: S,
) -> Result<(), OxenError> {
    kv_db::delete(db, key)
}

/// More efficient than `list` since it does not deserialize the values
pub fn list_keys<T: ThreadMode>(db: &DBWithThreadMode<T>) -> Result<Vec<String>, OxenError> {
    kv_db::list_keys(db)
}

/// # Get the value from the key
pub fn get<T: ThreadMode, S: AsRef<str>, D>(
    db: &DBWithThreadMode<T>,
    key: S,
) -> Result<Option<D>, OxenError>
where
    D: bytevec::ByteDecodable,
{
    let key = key.as_ref();
    log::debug!("str_val_db::get({:?}) from db {:?}", key, db.path());

    let key_bytes = key.as_bytes();
    match db.get(key_bytes) {
        Ok(Some(value)) => {
            // found it
            if let Ok(entry) = D::decode::<u8>(&value) {
                Ok(Some(entry))
            } else {
                Err(OxenError::could_not_decode_value_for_key_error(key))
            }
        }
        Ok(None) => {
            // did not get val
            Ok(None)
        }
        Err(err) => {
            // error from the DB
            let err = format!(
                "Err could not fetch value {:?} from db: {} from db {:?}",
                key,
                err,
                db.path()
            );
            Err(OxenError::basic_str(err))
        }
    }
}

/// # Serializes the entry to json and writes to db
pub fn put<T: ThreadMode, S: AsRef<str>, D>(
    db: &DBWithThreadMode<T>,
    key: S,
    entry: &D,
) -> Result<(), OxenError>
where
    D: bytevec::ByteEncodable + std::fmt::Debug,
{
    let key = key.as_ref();

    log::debug!(
        "str_val_db::put {:?} -> {:?} db: {:?}",
        key,
        entry,
        db.path()
    );

    match entry.encode::<u8>() {
        Ok(val) => {
            db.put(key, val)?;
            Ok(())
        }
        Err(err) => {
            log::error!("Err: Could not encode value {}", err);
            Err(OxenError::basic_str("Could not encode value..."))
        }
    }
}

/// List Values
pub fn list_vals<T: ThreadMode, D>(db: &DBWithThreadMode<T>) -> Result<Vec<D>, OxenError>
where
    D: bytevec::ByteDecodable,
{
    let iter = db.iterator(IteratorMode::Start);
    let mut values: Vec<D> = vec![];
    for item in iter {
        match item {
            Ok((key, value)) => {
                // Full path given the dir it is in
                if let Ok(entry) = D::decode::<u8>(&value) {
                    values.push(entry);
                } else {
                    let key = str::from_utf8(&key).unwrap();
                    return Err(OxenError::could_not_decode_value_for_key_error(key));
                }
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(values)
}

/// # List keys and attached values
pub fn list<T>(db: &DBWithThreadMode<MultiThreaded>) -> Result<Vec<(String, T)>, OxenError>
where
    T: bytevec::ByteDecodable + std::fmt::Debug,
{
    let iter = db.iterator(IteratorMode::Start);
    let mut results: Vec<(String, T)> = vec![];
    for item in iter {
        match item {
            Ok((key, value)) => match (str::from_utf8(&key), T::decode::<u8>(&value)) {
                (Ok(key), Ok(value)) => {
                    let key = String::from(key);
                    results.push((key, value));
                }
                (Ok(key), _) => {
                    log::error!("str_val_db::list() Could not values for key {}.", key)
                }
                (_, Ok(val)) => {
                    log::error!("str_val_db::list() Could not key for value {:?}.", val)
                }
                _ => {
                    log::error!("str_val_db::list() Could not decoded keys and values.")
                }
            },
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(results)
}

/// # List keys and attached values
pub fn hash_map<T>(db: &DBWithThreadMode<MultiThreaded>) -> Result<HashMap<String, T>, OxenError>
where
    T: bytevec::ByteDecodable + std::fmt::Debug,
{
    let iter = db.iterator(IteratorMode::Start);
    let mut results: HashMap<String, T> = HashMap::new();
    for item in iter {
        match item {
            Ok((key, value)) => match (str::from_utf8(&key), T::decode::<u8>(&value)) {
                (Ok(key), Ok(value)) => {
                    let key = String::from(key);
                    results.insert(key, value);
                }
                (Ok(key), _) => {
                    log::error!("str_val_db::list() Could not values for key {}.", key)
                }
                (_, Ok(val)) => {
                    log::error!("str_val_db::list() Could not key for value {:?}.", val)
                }
                _ => {
                    log::error!("str_val_db::list() Could not decoded keys and values.")
                }
            },
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(results)
}
