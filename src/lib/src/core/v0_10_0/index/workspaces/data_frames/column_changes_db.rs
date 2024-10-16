use rocksdb::DB;

use crate::{error::OxenError, view::data_frames::DataFrameColumnChange};

pub fn write_data_frame_column_change(
    data_frame_column_change: &DataFrameColumnChange,
    db: &DB,
) -> Result<(), OxenError> {
    save_data_frame_column_changes(db, data_frame_column_change)
}

pub fn save_data_frame_column_changes(
    db: &DB,
    data_frame_column_change: &DataFrameColumnChange,
) -> Result<(), OxenError> {
    let column_name = match &data_frame_column_change.column_after {
        Some(column_after) => column_after.column_name.clone(),
        None => data_frame_column_change
            .column_before
            .as_ref()
            .expect("column_before is None")
            .column_name
            .clone(),
    };

    let key = column_name;
    let val_json = serde_json::to_string(data_frame_column_change)?;

    db.put(key, val_json.as_bytes())?;

    log::debug!(
        "save_data_frame_column_changes() saved change: {:?}",
        data_frame_column_change
    );

    Ok(())
}

pub fn delete_data_frame_column_changes(db: &DB, column_name: &str) -> Result<(), OxenError> {
    db.delete(column_name)?;

    log::debug!(
        "delete_data_frame_column_changes() deleted change in: {:?}",
        column_name
    );

    Ok(())
}

pub fn get_all_data_frame_column_changes(db: &DB) -> Result<Vec<DataFrameColumnChange>, OxenError> {
    let mut changes = Vec::new();

    // Iterate from the start
    let iterator = db.iterator(rocksdb::IteratorMode::Start);

    for item in iterator {
        match item {
            Ok((_key, value)) => {
                let val_str = match std::str::from_utf8(&value) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                match serde_json::from_str::<DataFrameColumnChange>(val_str) {
                    Ok(change) => changes.push(change),
                    Err(_) => continue,
                }
            }
            Err(_) => continue,
        }
    }

    Ok(changes)
}

pub fn get_data_frame_column_change(
    db: &DB,
    name: &str,
) -> Result<Option<DataFrameColumnChange>, OxenError> {
    let val = db.get(name)?;

    match val {
        Some(val) => {
            let val_str = match std::str::from_utf8(&val) {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            match serde_json::from_str::<DataFrameColumnChange>(val_str) {
                Ok(change) => Ok(Some(change)),
                Err(_) => Ok(None),
            }
        }
        None => Ok(None),
    }
}
