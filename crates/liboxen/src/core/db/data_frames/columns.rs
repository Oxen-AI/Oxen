use std::path::Path;

use duckdb::arrow::array::RecordBatch;
use polars::frame::DataFrame;
use rocksdb::DB;

use crate::constants::TABLE_NAME;
use crate::core::db::data_frames::workspace_df_db::schema_without_oxen_cols;
use crate::core::db::data_frames::{DataFrameError, changes_db, column_changes_db};
use crate::model::Schema;
use crate::model::data_frame::schema::DataType;
use crate::view::data_frames::columns::{ColumnToDelete, ColumnToUpdate, NewColumn};
use crate::view::data_frames::{ColumnChange, DataFrameColumnChange};

use super::df_db;

pub fn add_column(
    conn: &duckdb::Connection,
    new_column: &NewColumn,
) -> Result<DataFrame, DataFrameError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;

    if table_schema.has_column(&new_column.name) {
        return Err(DataFrameError::ColumnNameAlreadyExists(
            new_column.name.clone(),
        ));
    }

    let inserted_df = polar_insert_column(conn, TABLE_NAME, new_column)?;
    Ok(inserted_df)
}

pub fn delete_column(
    conn: &duckdb::Connection,
    column_to_delete: &ColumnToDelete,
) -> Result<DataFrame, DataFrameError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;

    if !table_schema.has_column(&column_to_delete.name) {
        return Err(DataFrameError::ColumnNameNotFound(
            column_to_delete.name.clone(),
        ));
    }

    let inserted_df = polar_delete_column(conn, TABLE_NAME, column_to_delete)?;
    Ok(inserted_df)
}

pub fn update_column(
    conn: &duckdb::Connection,
    column_to_update: &ColumnToUpdate,
    table_schema: &Schema,
) -> Result<DataFrame, DataFrameError> {
    if !table_schema.has_column(&column_to_update.name) {
        return Err(DataFrameError::ColumnNameNotFound(
            column_to_update.name.clone(),
        ));
    }

    let inserted_df = polar_update_column(conn, TABLE_NAME, column_to_update)?;
    Ok(inserted_df)
}

pub fn record_column_change(
    column_changes_path: &Path,
    operation: String,
    column_before: Option<ColumnChange>,
    column_after: Option<ColumnChange>,
) -> Result<(), DataFrameError> {
    let handle = changes_db::get_changes_db(column_changes_path)?;

    // One write guard across the lookup-revert-write compound. Must not cross `.await`.
    let db = handle.write();

    if operation == "deleted"
        && let Some(column) = &column_before
        && let Some(previous_change) =
            column_changes_db::get_data_frame_column_change(&db, &column.column_name)?
        && previous_change.operation == "added"
    {
        // If we're deleting a previously added column, just remove the change
        return revert_column_changes(&db, &column.column_name);
    }

    let change = DataFrameColumnChange {
        operation,
        column_before: column_before.clone(),
        column_after: column_after.clone(),
    };

    let _ = maybe_revert_column_changes(&db, column_before);
    let _ = maybe_revert_column_changes(&db, column_after);

    column_changes_db::write_data_frame_column_change(&change, &db)
}

pub fn maybe_revert_column_changes(
    db: &DB,
    column: Option<ColumnChange>,
) -> Result<(), DataFrameError> {
    if let Some(column) = column {
        column_changes_db::get_data_frame_column_change(db, &column.column_name).and_then(
            |change_opt| match change_opt {
                Some(_) => revert_column_changes(db, &column.column_name.to_owned()),
                None => Ok(()),
            },
        )
    } else {
        Ok(())
    }
}

pub fn revert_column_changes(db: &DB, column_name: &str) -> Result<(), DataFrameError> {
    column_changes_db::delete_data_frame_column_changes(db, column_name)
}

pub fn polar_insert_column(
    conn: &duckdb::Connection,
    table_name: &str,
    new_column: &NewColumn,
) -> Result<DataFrame, DataFrameError> {
    let data_type = DataType::from_string(&new_column.data_type).to_sql();
    let sql = format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        table_name, new_column.name, data_type
    );
    conn.execute(&sql, [])?;

    let sql_query = format!("SELECT * FROM {table_name}");
    let result_set: Vec<RecordBatch> = conn.prepare(&sql_query)?.query_arrow([])?.collect();

    df_db::record_batches_to_polars_df(result_set)
}

pub fn polar_delete_column(
    conn: &duckdb::Connection,
    table_name: &str,
    column_to_delete: &ColumnToDelete,
) -> Result<DataFrame, DataFrameError> {
    // Corrected to DROP COLUMN instead of ADD COLUMN
    let sql = format!(
        "ALTER TABLE {} DROP COLUMN {}",
        table_name, column_to_delete.name
    );
    conn.execute(&sql, [])?;

    let sql_query = format!("SELECT * FROM {table_name}");
    let result_set: Vec<RecordBatch> = conn.prepare(&sql_query)?.query_arrow([])?.collect();

    df_db::record_batches_to_polars_df(result_set)
}

pub fn polar_update_column(
    conn: &duckdb::Connection,
    table_name: &str,
    column_to_update: &ColumnToUpdate,
) -> Result<DataFrame, DataFrameError> {
    let sql_commands = {
        let mut sql_commands = Vec::new();

        if let Some(ref new_data_type) = column_to_update.new_data_type {
            let data_type = DataType::from_string(new_data_type).to_sql();

            let update_type_sql = format!(
                "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                table_name, column_to_update.name, data_type
            );
            sql_commands.push(update_type_sql);
        }

        if let Some(ref new_name) = column_to_update.new_name {
            let rename_sql = format!(
                "ALTER TABLE {} RENAME COLUMN {} TO {}",
                table_name, column_to_update.name, new_name
            );
            sql_commands.push(rename_sql);
        }
        sql_commands
    };

    for sql in sql_commands {
        conn.execute(&sql, [])?;
    }

    let sql_query = format!("SELECT * FROM {table_name}");
    let result_set: Vec<RecordBatch> = conn.prepare(&sql_query)?.query_arrow([])?.collect();

    df_db::record_batches_to_polars_df(result_set)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OxenError;
    use crate::test;

    /// Concurrent `record_column_change` calls on the same column serialize: the final state
    /// matches exactly one writer's complete record (or is empty after an added-then-deleted
    /// collapse), with no mixed fields.
    #[test]
    fn test_concurrent_record_column_change_same_column_serializes() -> Result<(), OxenError> {
        const NUM_THREADS: usize = 16;
        const COLUMN: &str = "shared-col";

        test::run_empty_dir_test(|data_dir| {
            let column_changes_path = data_dir.join("column_changes");
            let _bootstrap = changes_db::get_changes_db(&column_changes_path)?;

            std::thread::scope(|scope| {
                let workers: Vec<_> = (0..NUM_THREADS)
                    .map(|i| {
                        let column_changes_path = column_changes_path.clone();
                        scope.spawn(move || -> Result<(), DataFrameError> {
                            // Mix add/delete to exercise the early-return
                            // (added-then-deleted collapse) branch under
                            // contention alongside the normal write path.
                            if i % 2 == 0 {
                                record_column_change(
                                    &column_changes_path,
                                    "added".to_string(),
                                    None,
                                    Some(ColumnChange {
                                        column_name: COLUMN.to_string(),
                                        column_data_type: Some(format!("type-{i}")),
                                    }),
                                )
                            } else {
                                record_column_change(
                                    &column_changes_path,
                                    "deleted".to_string(),
                                    Some(ColumnChange {
                                        column_name: COLUMN.to_string(),
                                        column_data_type: Some(format!("type-{i}")),
                                    }),
                                    None,
                                )
                            }
                        })
                    })
                    .collect();
                for w in workers {
                    w.join()
                        .expect("worker panicked")
                        .expect("record_column_change must not race itself");
                }
            });

            // After all threads finish, the stored entry (if any) must be a
            // self-consistent record from a single writer — never a mix of
            // fields from different writers.
            let handle = changes_db::get_changes_db(&column_changes_path)?;
            let stored = column_changes_db::get_data_frame_column_change(&handle.read(), COLUMN)?;
            if let Some(change) = stored {
                match change.operation.as_str() {
                    "added" => {
                        assert!(
                            change.column_before.is_none(),
                            "added record must have no column_before, got {:?}",
                            change.column_before,
                        );
                        let after = change.column_after.expect("added has column_after");
                        assert_eq!(after.column_name, COLUMN);
                        let dt = after.column_data_type.expect("added has data type");
                        assert!(
                            (0..NUM_THREADS)
                                .step_by(2)
                                .any(|i| dt == format!("type-{i}")),
                            "stored type must match an add-writer, got {dt:?}",
                        );
                    }
                    "deleted" => {
                        assert!(
                            change.column_after.is_none(),
                            "deleted record must have no column_after, got {:?}",
                            change.column_after,
                        );
                        let before = change.column_before.expect("deleted has column_before");
                        assert_eq!(before.column_name, COLUMN);
                        let dt = before.column_data_type.expect("deleted has data type");
                        assert!(
                            (1..NUM_THREADS)
                                .step_by(2)
                                .any(|i| dt == format!("type-{i}")),
                            "stored type must match a delete-writer, got {dt:?}",
                        );
                    }
                    other => panic!("unexpected operation: {other:?}"),
                }
            }
            drop(handle);

            changes_db::remove_from_cache(&column_changes_path)?;
            Ok(())
        })
    }
}
