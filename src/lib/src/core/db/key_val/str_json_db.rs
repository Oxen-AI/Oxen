use crate::core::db::key_val::kv_db;
use crate::error::OxenError;
use serde::{de, Serialize};

use rocksdb::{DBWithThreadMode, IteratorMode, ThreadMode};
use std::{collections::HashMap, str};

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
    D: de::DeserializeOwned,
{
    let key = key.as_ref();
    // log::debug!("str_json_db::get({:?}) from db {:?}", key, db.path());

    let bytes = key.as_bytes();
    match db.get(bytes) {
        Ok(Some(value)) => {
            // found it
            let str_val = str::from_utf8(&value)?;
            let entry = serde_json::from_str(str_val)?;
            // log::debug!("str_json_db::get({:?}) got entry {:?}", key, str_val);
            Ok(Some(entry))
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
    D: Serialize,
{
    let key = key.as_ref();
    let json_val = serde_json::to_string(entry)?;

    // log::debug!(
    //     "str_json_db::put {:?} -> {:?} -> db: {:?}",
    //     key,
    //     json_val,
    //     db.path()
    // );

    db.put(key, json_val.as_bytes())?;
    Ok(())
}

/// List Values
pub fn list_vals<T: ThreadMode, D>(db: &DBWithThreadMode<T>) -> Result<Vec<D>, OxenError>
where
    D: de::DeserializeOwned,
{
    let iter = db.iterator(IteratorMode::Start);
    let mut values: Vec<D> = vec![];
    for item in iter {
        match item {
            Ok((_, value)) => {
                let value = str::from_utf8(&value)?;
                // Full path given the dir it is in
                let entry: Result<D, serde_json::error::Error> = serde_json::from_str(value);
                match entry {
                    Ok(entry) => {
                        values.push(entry);
                    }
                    Err(err) => {
                        log::error!("Could not decode value: {}", err);
                    }
                }
            }
            Err(err) => {
                log::error!("error iterating over db values is: {}", err);
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(values)
}

/// # List keys and attached values
pub fn list<T: ThreadMode, D>(db: &DBWithThreadMode<T>) -> Result<Vec<(String, D)>, OxenError>
where
    D: de::DeserializeOwned,
{
    let iter = db.iterator(IteratorMode::Start);
    let mut results: Vec<(String, D)> = vec![];
    for item in iter {
        match item {
            Ok((key, value)) => match (str::from_utf8(&key), str::from_utf8(&value)) {
                (Ok(key), Ok(value)) => {
                    let key = String::from(key);
                    let entry: Result<D, serde_json::error::Error> = serde_json::from_str(value);
                    match entry {
                        Ok(entry) => {
                            results.push((key, entry));
                        }
                        Err(err) => {
                            log::warn!("str_json_db::list() Could not deserialize entry '{}' -> '{}'\n{:?}", key, value, err);
                        }
                    }
                }
                (Ok(key), _) => {
                    log::error!("str_json_db::list() Could not values for key {}.", key)
                }
                (_, Ok(val)) => {
                    log::error!("str_json_db::list() Could not key for value {}.", val)
                }
                _ => {
                    log::error!("str_json_db::list() Could not decoded keys and values.")
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

/// List keys and values in a hash map
pub fn hash_map<T: ThreadMode, D>(db: &DBWithThreadMode<T>) -> Result<HashMap<String, D>, OxenError>
where
    D: de::DeserializeOwned,
{
    let iter = db.iterator(IteratorMode::Start);
    let mut results: HashMap<String, D> = HashMap::new();
    for item in iter {
        // log::debug!("str_json_db::hash_map() got item {:?}", item);
        match item {
            Ok((key, value)) => match (str::from_utf8(&key), str::from_utf8(&value)) {
                (Ok(key), Ok(value)) => {
                    let key = String::from(key);
                    let entry: Result<D, serde_json::error::Error> = serde_json::from_str(value);
                    if let Ok(entry) = entry {
                        results.insert(key, entry);
                    }
                }
                (Ok(key), _) => {
                    log::error!("str_json_db::hash_map() Could not values for key {}.", key)
                }
                (_, Ok(val)) => {
                    log::error!("str_json_db::hash_map() Could not key for value {}.", val)
                }
                _ => {
                    log::error!("str_json_db::hash_map() Could not decoded keys and values.")
                }
            },
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    log::debug!("done iterating");
    Ok(results)
}
