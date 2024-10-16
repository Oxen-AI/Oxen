use rocksdb::DB;

use crate::{error::OxenError, view::data_frames::DataFrameRowChange};

pub fn write_data_frame_row_change(
    data_frame_row_change: &DataFrameRowChange,
    db: &DB,
) -> Result<(), OxenError> {
    save_data_frame_row_changes(db, data_frame_row_change)
}

pub fn save_data_frame_row_changes(
    db: &DB,
    data_frame_row_change: &DataFrameRowChange,
) -> Result<(), OxenError> {
    let key = &data_frame_row_change.row_id;
    let val_json = serde_json::to_string(data_frame_row_change)?;

    db.put(key, val_json.as_bytes())?;

    log::debug!(
        "save_data_frame_row_changes() row_id: {} saved change: {:?}",
        key,
        data_frame_row_change
    );

    Ok(())
}

pub fn delete_data_frame_row_changes(db: &DB, row_id: &str) -> Result<(), OxenError> {
    db.delete(row_id)?;

    log::debug!(
        "delete_data_frame_row_changes() deleted change in: {:?}",
        row_id
    );

    Ok(())
}

pub fn get_all_data_frame_row_changes(db: &DB) -> Result<Vec<DataFrameRowChange>, OxenError> {
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
                match serde_json::from_str::<DataFrameRowChange>(val_str) {
                    Ok(change) => changes.push(change),
                    Err(_) => continue,
                }
            }
            Err(_) => continue,
        }
    }

    Ok(changes)
}

pub fn get_data_frame_row_change(
    db: &DB,
    name: &str,
) -> Result<Option<DataFrameRowChange>, OxenError> {
    let val = db.get(name)?;

    match val {
        Some(val) => {
            let val_str = match std::str::from_utf8(&val) {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            match serde_json::from_str::<DataFrameRowChange>(val_str) {
                Ok(change) => Ok(Some(change)),
                Err(_) => Ok(None),
            }
        }
        None => Ok(None),
    }
}
