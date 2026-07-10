use std::collections::HashMap;
use std::path::Path;

use duckdb::ToSql;
use duckdb::arrow::array::RecordBatch;
use polars::frame::DataFrame;
use rocksdb::DB;
use serde_json::Value;
use sql::Select;
// use sql::Select;
use sql_query_builder as sql;

use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, OXEN_COLS, OXEN_ID_COL};

use crate::constants::TABLE_NAME;
use crate::core::db::data_frames::workspace_df_db::schema_without_oxen_cols;
use crate::core::db::data_frames::{DataFrameError, changes_db, row_changes_db};
use crate::core::df::tabular;
use crate::model::data_frame::schema::DataType;
use crate::model::staged_row_status::StagedRowStatus;
use crate::view::data_frames::DataFrameRowChange;
use polars::prelude::*; // or use polars::lazy::*; if you're working in a lazy context

use super::df_db;

pub fn append_row(conn: &duckdb::Connection, df: &DataFrame) -> Result<DataFrame, DataFrameError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    let df_schema = df.schema();

    let df_names: Vec<String> = df_schema.iter_names().map(|s| s.to_string()).collect();
    if !table_schema.has_field_names(&df_names) {
        return Err(DataFrameError::IncompatibleSchemas {
            table_schema,
            df_cols: df_names,
        });
    }

    let added_column = Column::Series(
        Series::new(
            PlSmallStr::from_str(DIFF_STATUS_COL),
            vec![StagedRowStatus::Added.to_string(); df.height()],
        )
        .into(),
    );
    let df = df.hstack(&[added_column])?;

    // Handle initialization for completely null {} create objects coming over from the hub
    let df = if df.height() == 0 {
        let added_column = Column::Series(
            Series::new(
                PlSmallStr::from_str(DIFF_STATUS_COL),
                vec![StagedRowStatus::Added.to_string()],
            )
            .into(),
        );
        DataFrame::new(vec![added_column])?
    } else {
        df
    };

    let inserted_df = insert_polars_df(conn, TABLE_NAME, &df)?;

    Ok(inserted_df)

    // Proceed with appending `new_df` to the database
}

pub fn modify_row(
    conn: &duckdb::Connection,
    df: &mut DataFrame,
    uuid: &str,
) -> Result<DataFrame, DataFrameError> {
    if df.height() != 1 {
        return Err(DataFrameError::ModifyOnly1Row);
    }

    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;

    // Filter it down to exclude any of the OXEN_COLS, we don't want to modify these but hub sends them over
    let schema = df.schema();
    let df_col_names: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
    let df_cols: Vec<String> = df_col_names
        .clone()
        .into_iter()
        .filter(|col| !OXEN_COLS.contains(&col.as_str()))
        .collect();
    let df = df.select(&df_cols)?;
    if !table_schema.has_field_names(&df_cols) {
        log::error!("modify_row incompatible_schemas {table_schema:?}\n{df_cols:?}");
        return Err(DataFrameError::IncompatibleSchemas {
            table_schema,
            df_cols,
        });
    }

    // get existing hash and status from db
    let select_hash = Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!("\"{OXEN_ID_COL}\" = '{uuid}'"));
    let maybe_db_data = df_db::select(conn, &select_hash, None)?;

    let mut new_row = maybe_db_data.clone().to_owned();

    for col in df.get_columns() {
        // Replace that column - copy the entire Series to preserve complex types (lists, structs, etc.)
        let col_name = col.name();
        let col_series = df.column(col_name)?;
        if let Some(col_idx) = new_row.get_column_index(col_name) {
            new_row.replace_column(col_idx, col_series.clone())?;
        } else {
            new_row.with_column(col_series.clone())?;
        }
    }

    // TODO could use a struct to return these more safely
    let (insert_hash, updated_status) =
        get_hash_and_status_for_modification(conn, &maybe_db_data, &new_row)?;

    // Update with latest values pre insert
    // TODO: Find a better way to do this than overwriting the entire column here.
    new_row.with_column(Series::new(
        PlSmallStr::from_str(DIFF_STATUS_COL),
        vec![updated_status],
    ))?;
    new_row.with_column(Series::new(
        PlSmallStr::from_str(DIFF_HASH_COL),
        vec![insert_hash],
    ))?;
    let result = df_db::modify_row_with_polars_df(conn, TABLE_NAME, uuid, &new_row)?;
    if result.height() == 0 {
        return Err(DataFrameError::MissingDataFrame(uuid.to_string()));
    }
    Ok(result)
}

pub fn modify_rows(
    conn: &duckdb::Connection,
    row_map: HashMap<String, DataFrame>,
) -> Result<DataFrame, DataFrameError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;

    let mut update_map: HashMap<String, DataFrame> = HashMap::new();

    // Filter it down to exclude any of the OXEN_COLS, we don't want to modify these but hub sends them over
    for (row_id, df) in row_map.iter() {
        if df.height() != 1 {
            return Err(DataFrameError::ModifyOnly1Row);
        }
        let schema = df.schema();
        let df_col_names: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
        let df_cols: Vec<String> = df_col_names
            .clone()
            .into_iter()
            .filter(|col| !OXEN_COLS.contains(&col.as_str()))
            .collect();
        let df = df.select(&df_cols)?;
        if !table_schema.has_field_names(&df_cols) {
            log::error!("modify_row incompatible_schemas {table_schema:?}\n{df_cols:?}");
            return Err(DataFrameError::IncompatibleSchemas {
                table_schema,
                df_cols,
            });
        }

        // get existing hash and status from db
        let select_hash = Select::new()
            .select("*")
            .from(TABLE_NAME)
            .where_clause(&format!("\"{OXEN_ID_COL}\" = '{row_id}'"));
        let maybe_db_data = df_db::select(conn, &select_hash, None)?;

        let mut new_row = maybe_db_data.clone().to_owned();
        for col in df.get_columns() {
            // Replace that column - copy the entire Series to preserve complex types (lists, structs, etc.)
            let col_name = col.name();
            let col_series = df.column(col_name)?;
            if let Some(col_idx) = new_row.get_column_index(col_name) {
                new_row.replace_column(col_idx, col_series.clone())?;
            } else {
                new_row.with_column(col_series.clone())?;
            }
        }

        // TODO could use a struct to return these more safely
        let (insert_hash, updated_status) =
            get_hash_and_status_for_modification(conn, &maybe_db_data, &new_row)?;

        // TODO: Find a better way to do this than overwriting the entire column here.
        new_row.with_column(Series::new(
            PlSmallStr::from_str(DIFF_STATUS_COL),
            vec![updated_status],
        ))?;
        new_row.with_column(Series::new(
            PlSmallStr::from_str(DIFF_HASH_COL),
            vec![insert_hash],
        ))?;
        update_map.insert(row_id.to_owned(), new_row);
    }

    let result = df_db::modify_rows_with_polars_df(conn, TABLE_NAME, &update_map)?;
    if result.height() == 0 {
        return Err(DataFrameError::MissingDataFrame("".to_string()));
    }

    if result.height() != update_map.len() {
        return Err(DataFrameError::UnexpectedModifications {
            expected: update_map.len(),
            actual: result.height(),
        });
    }

    Ok(result)
}

pub fn delete_row(conn: &duckdb::Connection, uuid: &str) -> Result<DataFrame, DataFrameError> {
    let select_stmt = sql::Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!("{OXEN_ID_COL} = '{uuid}'"));

    let row_to_delete = df_db::select(conn, &select_stmt, None)?;
    if row_to_delete.height() == 0 {
        return Err(DataFrameError::MissingDataFrame(uuid.to_string()));
    }

    // If it's newly added, delete it. Otherwise, set it to removed
    let status = row_to_delete.column(DIFF_STATUS_COL)?.get(0)?;
    let status_str = status.get_str();

    let status = match status_str {
        Some(status) => status,
        None => return Err(DataFrameError::DiffStatusColNotStr),
    };
    log::debug!("status is: {status}");

    // Rows that weren't in previous commits are just removed from the staging df, rows in previous commits are tombstoned as "Removed"
    if status == StagedRowStatus::Added.to_string() {
        log::debug!("staged_df_db::delete_row() deleting row");
        let stmt = sql::Delete::new()
            .delete_from(TABLE_NAME)
            .where_clause(&format!("{OXEN_ID_COL} = '{uuid}'"));
        log::debug!("staged_df_db::delete_row() sql: {stmt:?}");
        conn.execute(&stmt.to_string(), [])?;
    } else {
        log::debug!("staged_df_db::delete_row() updating row to indicate deletion");
        let stmt = sql::Update::new()
            .update(TABLE_NAME)
            .set(&format!(
                "\"{}\" = '{}'",
                DIFF_STATUS_COL,
                StagedRowStatus::Removed
            ))
            .where_clause(&format!("{OXEN_ID_COL} = '{uuid}'"));
        log::debug!("staged_df_db::delete_row() sql: {stmt:?}");
        conn.execute(&stmt.to_string(), [])?;
    };

    Ok(row_to_delete)
}

fn get_hash_and_status_for_modification(
    conn: &duckdb::Connection,
    old_row: &DataFrame,
    new_row: &DataFrame,
) -> Result<(String, String), DataFrameError> {
    let schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    let col_names = schema.fields_names();
    let old_status = old_row.column(DIFF_STATUS_COL)?.get(0)?;
    let old_status = old_status
        .get_str()
        .ok_or_else(|| DataFrameError::DiffStatusColNotStr)?;

    let old_hash = old_row.column(DIFF_HASH_COL)?.get(0)?;

    let new_hash_df = tabular::df_hash_rows_on_cols(new_row.clone(), &col_names, "_temp_hash")?;
    let new_hash = new_hash_df.column("_temp_hash")?.get(0)?;
    let new_hash = new_hash
        .get_str()
        .ok_or_else(|| DataFrameError::DiffHashColNotStr)?;

    // We need to calculate the original hash for the row
    // Use a temp hash column to avoid collision with the column that's already there.
    let insert_hash = if old_hash.is_null() {
        let original_data_hash =
            tabular::df_hash_rows_on_cols(old_row.clone(), &col_names, "_temp_hash")?;
        let original_data_hash = original_data_hash.column("_temp_hash")?.get(0)?;
        original_data_hash
            .get_str()
            .ok_or_else(|| DataFrameError::DiffHashColNotStr)?
            .to_owned()
    } else {
        old_hash
            .get_str()
            .ok_or_else(|| DataFrameError::DiffHashColNotStr)?
            .to_owned()
    };

    // Anything previously added must stay added regardless of any further modifications.
    // Modifying back to original state changes it to unchanged
    // If we have no prior hash info on the original hash state, it is now modified (this is the first modification)
    let new_status = if old_status == StagedRowStatus::Added.to_string() {
        StagedRowStatus::Added.to_string()
    } else if old_status == StagedRowStatus::Removed.to_string() {
        if insert_hash == new_hash {
            StagedRowStatus::Unchanged.to_string()
        } else {
            StagedRowStatus::Modified.to_string()
        }
    } else if old_hash.is_null() {
        StagedRowStatus::Modified.to_string()
    } else if new_hash == insert_hash {
        StagedRowStatus::Unchanged.to_string()
    } else {
        StagedRowStatus::Modified.to_string()
    };

    Ok((insert_hash.to_string(), new_status))
}

/// Insert a row from a polars dataframe into a duckdb table.
pub fn insert_polars_df(
    conn: &duckdb::Connection,
    table_name: &str,
    df: &DataFrame,
) -> Result<DataFrame, DataFrameError> {
    let schema = df.schema();
    let field_names: Vec<&str> = schema.iter_names().map(|s| s.as_str()).collect();
    let column_names: Vec<String> = field_names
        .iter()
        .map(|name| format!("\"{name}\""))
        .collect();

    let column_sql_types = column_sql_types_by_name(conn, table_name)?;
    let placeholders: String = field_names
        .iter()
        .map(|name| placeholder_for_column(&column_sql_types, name))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
        table_name,
        column_names.join(", "),
        placeholders,
    );

    let mut stmt = conn.prepare(&sql)?;

    // TODO: is there a way to bulk insert this?
    let mut result_df = DataFrame::default();
    for idx in 0..df.height() {
        let row = df.get(idx).unwrap();
        let boxed_values: Vec<Box<dyn ToSql>> = row
            .iter()
            .map(|v| tabular::value_to_tosql(v.to_owned()))
            .collect();

        let params: Vec<&dyn ToSql> = boxed_values
            .iter()
            .map(|boxed_value| &**boxed_value as &dyn ToSql)
            .collect();

        // Convert to Vec<&RecordBatch>
        let result_set: Vec<RecordBatch> = stmt.query_arrow(params.as_slice())?.collect();

        let df = df_db::record_batches_to_polars_df(result_set)?;
        result_df = if df.height() == 0 || result_df.height() == 0 {
            df
        } else {
            result_df.vstack(&df).unwrap()
        };
    }

    Ok(result_df)
}

pub fn record_row_change(
    row_changes_path: &Path,
    row_id: String,
    operation: String,
    value: Value,
    new_value: Option<Value>,
) -> Result<(), DataFrameError> {
    let change = DataFrameRowChange {
        row_id: row_id.to_owned(),
        operation,
        value,
        new_value,
    };

    let handle = changes_db::get_changes_db(row_changes_path)?;

    // One write guard across the revert-then-put compound. Must not cross `.await`.
    let db = handle.write();
    maybe_revert_row_changes(&db, row_id.to_owned())?;
    row_changes_db::write_data_frame_row_change(&change, &db)
}

pub fn maybe_revert_row_changes(db: &DB, row_id: String) -> Result<(), DataFrameError> {
    match row_changes_db::get_data_frame_row_change(db, &row_id) {
        Ok(None) => revert_row_changes(db, row_id),
        Ok(Some(_)) => Ok(()),
        Err(e) => Err(e),
    }
}

pub fn revert_row_changes(db: &DB, row_id: String) -> Result<(), DataFrameError> {
    row_changes_db::delete_data_frame_row_changes(db, &row_id)
}

/// Build a column-name → SQL type map for the given DuckDB table.
///
/// Used to wrap List/Struct/Embedding placeholders in `CAST(? AS <sql_type>)` so that
/// JSON-encoded payloads bind unambiguously to typed list columns.
pub(crate) fn column_sql_types_by_name(
    conn: &duckdb::Connection,
    table_name: &str,
) -> Result<HashMap<String, String>, DataFrameError> {
    let schema = df_db::get_schema(conn, table_name)?;
    let mut by_name = HashMap::with_capacity(schema.fields.len());
    for field in schema.fields {
        let sql_type = DataType::from_string(&field.dtype).to_sql();
        by_name.insert(field.name, sql_type);
    }
    Ok(by_name)
}

/// Returns `CAST(? AS <sql_type>)` for List/Struct/Embedding columns and a bare `?` otherwise.
pub(crate) fn placeholder_for_column(
    column_sql_types: &HashMap<String, String>,
    column_name: &str,
) -> String {
    match column_sql_types.get(column_name) {
        Some(sql_type) if needs_explicit_cast(sql_type) => format!("CAST(? AS {sql_type})"),
        _ => "?".to_string(),
    }
}

fn needs_explicit_cast(sql_type: &str) -> bool {
    // List columns end with `[]` (e.g. INTEGER[], VARCHAR[]); fixed-size arrays / embeddings end with `[N]`.
    // Structs are stored as JSON (which already accepts string binds), but cast for symmetry / clarity.
    sql_type.ends_with(']') || sql_type == "JSON"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OxenError;
    use crate::test;

    /// Concurrent `record_row_change` calls for the same `row_id` serialize: the final stored value
    /// matches exactly one writer, with no torn state.
    #[test]
    fn test_concurrent_record_row_change_same_row_id_serializes() -> Result<(), OxenError> {
        const NUM_THREADS: usize = 16;

        test::run_empty_dir_test(|data_dir| {
            let row_changes_path = data_dir.join("row_changes");
            // Pre-open so all worker threads share the same cached handle.
            let _bootstrap = changes_db::get_changes_db(&row_changes_path)?;

            std::thread::scope(|scope| {
                let workers: Vec<_> = (0..NUM_THREADS)
                    .map(|i| {
                        let row_changes_path = row_changes_path.clone();
                        scope.spawn(move || -> Result<(), OxenError> {
                            record_row_change(
                                &row_changes_path,
                                "shared-row".to_string(),
                                "modified".to_string(),
                                Value::String(format!("value-{i}")),
                                None,
                            )?;
                            Ok(())
                        })
                    })
                    .collect();
                for w in workers {
                    w.join()
                        .expect("worker panicked")
                        .expect("record_row_change must not race itself");
                }
            });

            // Exactly one writer's change is stored (last-writer-wins on the
            // atomic compound); no torn intermediate is visible.
            let handle = changes_db::get_changes_db(&row_changes_path)?;
            let stored = row_changes_db::get_data_frame_row_change(&handle.read(), "shared-row")?
                .expect("a writer's change is present");
            let value = match stored.value {
                Value::String(s) => s,
                other => panic!("unexpected stored value: {other:?}"),
            };
            assert!(
                (0..NUM_THREADS).any(|i| value == format!("value-{i}")),
                "stored value should match one of the writers, got {value:?}",
            );
            drop(handle);

            changes_db::remove_from_cache(&row_changes_path)?;
            Ok(())
        })
    }
}
