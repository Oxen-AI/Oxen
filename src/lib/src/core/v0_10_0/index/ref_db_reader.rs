use crate::error::OxenError;
use rocksdb::{IteratorMode, DB};
use std::str;

pub struct RefDBReader {}

impl RefDBReader {
    pub fn has_branch(db: &DB, name: &str) -> bool {
        let bytes = name.as_bytes();
        match db.get(bytes) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(_) => false,
        }
    }

    pub fn get_commit_id_for_branch(
        db: &DB,
        branch_name: &str,
    ) -> Result<Option<String>, OxenError> {
        let bytes = branch_name.as_bytes();
        match db.get(bytes) {
            Ok(Some(value)) => Ok(Some(String::from(str::from_utf8(&value)?))),
            Ok(None) => Ok(None),
            Err(err) => {
                let err = format!("{err}");
                Err(OxenError::basic_str(err))
            }
        }
    }

    pub fn list_values(db: &DB) -> Result<Vec<(String, String)>, OxenError> {
        let mut values: Vec<(String, String)> = vec![];
        let iter = db.iterator(IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key, value)) => match (str::from_utf8(&key), str::from_utf8(&value)) {
                    (Ok(key_str), Ok(value)) => {
                        let ref_name = String::from(key_str);
                        let id = String::from(value);
                        values.push((ref_name.clone(), id.clone()));
                    }
                    _ => {
                        return Err(OxenError::basic_str("Error decoding utf8"));
                    }
                },
                Err(err) => {
                    let err = format!("Error reading db\nErr: {err}");
                    return Err(OxenError::basic_str(err));
                }
            }
        }
        Ok(values)
    }
}
