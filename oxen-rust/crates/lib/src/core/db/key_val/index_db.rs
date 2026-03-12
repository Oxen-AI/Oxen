use crate::error::OxenError;

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};
use std::collections::HashMap;
use std::mem;
use std::str;

pub fn insert_indices<S: AsRef<str>>(
    db: &DBWithThreadMode<MultiThreaded>,
    key: S,
    indices: Vec<u32>,
) -> Result<(), OxenError> {
    let key = key.as_ref();

    // Could not use the bytevec library here when inserting a larger set of indices
    let byte_indices = u32_to_u8(indices);

    db.put(key, byte_indices)?;
    Ok(())
}

pub fn get_indices<S: AsRef<str>>(
    db: &DBWithThreadMode<MultiThreaded>,
    key: S,
) -> Result<Option<Vec<u32>>, OxenError> {
    let key = key.as_ref();
    let bytes = key.as_bytes();
    match db.get(bytes) {
        Ok(Some(raw_indices)) => {
            // found it
            let decoded = u32_from_u8(raw_indices);

            Ok(Some(decoded))
        }
        Ok(None) => {
            // did not get val
            Ok(None)
        }
        Err(err) => {
            // error from the DB
            let err = format!("Err could not fetch value {key:?} from db: {err:?}",);
            Err(OxenError::basic_str(err))
        }
    }
}

pub fn list_indices(
    db: &DBWithThreadMode<MultiThreaded>,
) -> Result<Vec<(String, Vec<u32>)>, OxenError> {
    let iter = db.iterator(IteratorMode::Start);
    let mut results: Vec<(String, Vec<u32>)> = vec![];
    for item in iter {
        match item {
            Ok((key, value)) => match str::from_utf8(&key) {
                Ok(key) => {
                    let key = String::from(key);
                    let mut buffer: Vec<u8> = vec![];
                    for v in &*value {
                        buffer.push(*v);
                    }
                    let value = u32_from_u8(buffer);
                    results.push((key, value));
                }
                _ => {
                    return Err(OxenError::basic_str("Could not read utf8 val..."));
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

pub fn hash_map_indices(
    db: &DBWithThreadMode<MultiThreaded>,
) -> Result<HashMap<String, Vec<u32>>, OxenError> {
    let iter = db.iterator(IteratorMode::Start);
    let mut results: HashMap<String, Vec<u32>> = HashMap::new();
    for item in iter {
        match item {
            Ok((key, value)) => match str::from_utf8(&key) {
                Ok(key) => {
                    let key = String::from(key);
                    let mut buffer: Vec<u8> = vec![];
                    for v in &*value {
                        buffer.push(*v);
                    }
                    let value = u32_from_u8(buffer);
                    results.insert(key, value);
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

pub fn u32_to_u8(mut indices: Vec<u32>) -> Vec<u8> {
    unsafe {
        let ratio = mem::size_of::<u32>() / mem::size_of::<u8>();

        let length = indices.len() * ratio;
        let capacity = indices.capacity() * ratio;
        let ptr = indices.as_mut_ptr() as *mut u8;

        // Don't run the destructor for vec32
        mem::forget(indices);

        // Construct new Vec
        Vec::from_raw_parts(ptr, length, capacity)
    }
}

pub fn u32_from_u8(v: Vec<u8>) -> Vec<u32> {
    let data = v.as_ptr();
    let len = v.len();
    let capacity = v.capacity();
    let element_size = mem::size_of::<u32>();

    // Make sure we have a proper amount of capacity (may be overkill)
    assert_eq!(capacity % element_size, 0);
    // Make sure we are going to read a full chunk of stuff
    assert_eq!(len % element_size, 0);

    unsafe {
        // Don't allow the current vector to be dropped
        // (which would invalidate the memory)
        mem::forget(v);

        Vec::from_raw_parts(
            data as *mut u32,
            len / element_size,
            capacity / element_size,
        )
    }
}
