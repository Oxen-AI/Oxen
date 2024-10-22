use std::path::Path;

use duckdb::arrow::array::RecordBatch;
use duckdb::ToSql;
use polars::frame::DataFrame;
use rocksdb::DB;
use serde_json::Value;
use sql::Select;
// use sql::Select;
use sql_query_builder as sql;

use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, OXEN_COLS, OXEN_ID_COL};

use crate::core::db;
use crate::core::db::data_frames::workspace_df_db::{
    full_staged_table_schema, schema_without_oxen_cols,
};
use crate::core::df::tabular;
use crate::core::v0_10_0::index::workspaces::data_frames::row_changes_db;
use crate::model::data_frame::schema::Schema;
use crate::model::staged_row_status::StagedRowStatus;
use crate::view::data_frames::DataFrameRowChange;
use crate::{constants::TABLE_NAME, error::OxenError};
use polars::prelude::*; // or use polars::lazy::*; if you're working in a lazy context

use super::df_db;

pub fn append_row(conn: &duckdb::Connection, df: &DataFrame) -> Result<DataFrame, OxenError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    let df_schema = df.schema();

    let df_names: Vec<String> = df_schema.iter_names().map(|s| s.to_string()).collect();
    if !table_schema.has_field_names(&df_names) {
        return Err(OxenError::incompatible_schemas(table_schema.clone()));
    }

    let added_column = Column::Series(Series::new(
        PlSmallStr::from_str(DIFF_STATUS_COL),
        vec![StagedRowStatus::Added.to_string(); df.height()],
    ));
    let df = df.hstack(&[added_column])?;

    // Handle initialization for completely null {} create objects coming over from the hub
    let df = if df.height() == 0 {
        let added_column = Column::Series(Series::new(
            PlSmallStr::from_str(DIFF_STATUS_COL),
            vec![StagedRowStatus::Added.to_string()],
        ));
        DataFrame::new(vec![added_column])?
    } else {
        df
    };

    let schema = full_staged_table_schema(conn)?;
    let inserted_df = insert_polars_df(conn, TABLE_NAME, &df, &schema)?;

    log::debug!("staged_df_db::append_row() inserted_df: {:?}", inserted_df);

    Ok(inserted_df)

    // Proceed with appending `new_df` to the database
}

pub fn modify_row(
    conn: &duckdb::Connection,
    df: &mut DataFrame,
    uuid: &str,
) -> Result<DataFrame, OxenError> {
    if df.height() != 1 {
        return Err(OxenError::basic_str("Modify row requires exactly one row"));
    }

    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    let out_schema = full_staged_table_schema(conn)?;

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
        log::error!(
            "modify_row incompatible_schemas {:?}\n{:?}",
            table_schema,
            df_cols
        );
        return Err(OxenError::incompatible_schemas(table_schema));
    }

    // get existing hash and status from db
    let select_hash = Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!("\"{}\" = '{}'", OXEN_ID_COL, uuid));
    let maybe_db_data = df_db::select(conn, &select_hash, true, None, None)?;

    let mut new_row = maybe_db_data.clone().to_owned();
    for col in df.get_columns() {
        // Replace that column in the existing df if it exists
        let col_name = col.name();
        let new_val = df.column(col_name)?.get(0)?;
        new_row.with_column(Series::new(PlSmallStr::from_str(col_name), vec![new_val]))?;
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
    let result = df_db::modify_row_with_polars_df(conn, TABLE_NAME, uuid, &new_row, &out_schema)?;
    if result.height() == 0 {
        return Err(OxenError::resource_not_found(uuid));
    }
    Ok(result)
}

pub fn delete_row(conn: &duckdb::Connection, uuid: &str) -> Result<DataFrame, OxenError> {
    let select_stmt = sql::Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!("{} = '{}'", OXEN_ID_COL, uuid));

    let row_to_delete = df_db::select(conn, &select_stmt, true, None, None)?;

    if row_to_delete.height() == 0 {
        return Err(OxenError::resource_not_found(uuid));
    }

    // If it's newly added, delete it. Otherwise, set it to removed
    let status = row_to_delete.column(DIFF_STATUS_COL)?.get(0)?;
    let status_str = status.get_str();

    let status = match status_str {
        Some(status) => status,
        None => return Err(OxenError::basic_str("Diff status column is not a string")),
    };
    log::debug!("status is: {}", status);

    // Rows that weren't in previous commits are just removed from the staging df, rows in previous commits are tombstoned as "Removed"
    if status == StagedRowStatus::Added.to_string() {
        log::debug!("staged_df_db::delete_row() deleting row");
        let stmt = sql::Delete::new()
            .delete_from(TABLE_NAME)
            .where_clause(&format!("{} = '{}'", OXEN_ID_COL, uuid));
        log::debug!("staged_df_db::delete_row() sql: {:?}", stmt);
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
            .where_clause(&format!("{} = '{}'", OXEN_ID_COL, uuid));
        log::debug!("staged_df_db::delete_row() sql: {:?}", stmt);
        conn.execute(&stmt.to_string(), [])?;
    };

    Ok(row_to_delete)
}

fn get_hash_and_status_for_modification(
    conn: &duckdb::Connection,
    old_row: &DataFrame,
    new_row: &DataFrame,
) -> Result<(String, String), OxenError> {
    let schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    let col_names = schema.fields_names();
    let old_status = old_row.column(DIFF_STATUS_COL)?.get(0)?;
    let old_status = old_status
        .get_str()
        .ok_or_else(|| OxenError::basic_str("Diff status column is not a string"))?;

    let old_hash = old_row.column(DIFF_HASH_COL)?.get(0)?;

    let new_hash_df = tabular::df_hash_rows_on_cols(new_row.clone(), &col_names, "_temp_hash")?;
    let new_hash = new_hash_df.column("_temp_hash")?.get(0)?;
    let new_hash = new_hash
        .get_str()
        .ok_or_else(|| OxenError::basic_str("Diff hash column is not a string"))?;

    // We need to calculate the original hash for the row
    // Use a temp hash column to avoid collision with the column that's already there.
    let insert_hash = if old_hash.is_null() {
        let original_data_hash =
            tabular::df_hash_rows_on_cols(old_row.clone(), &col_names, "_temp_hash")?;
        let original_data_hash = original_data_hash.column("_temp_hash")?.get(0)?;
        original_data_hash
            .get_str()
            .ok_or_else(|| OxenError::basic_str("Diff hash column is not a string"))?
            .to_owned()
    } else {
        old_hash
            .get_str()
            .ok_or_else(|| OxenError::basic_str("Diff hash column is not a string"))?
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
    table_name: impl AsRef<str>,
    df: &DataFrame,
    out_schema: &Schema,
) -> Result<DataFrame, OxenError> {
    let table_name = table_name.as_ref();

    let schema = df.schema();
    let column_names: Vec<String> = schema
        .iter_fields()
        .map(|f| format!("\"{}\"", f.name()))
        .collect();

    let placeholders: String = column_names
        .iter()
        .map(|_| "?".to_string())
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

        let df = df_db::record_batches_to_polars_df_explicit_nulls(result_set, out_schema)?;

        result_df = if df.height() == 0 {
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
) -> Result<(), OxenError> {
    let change = DataFrameRowChange {
        row_id: row_id.to_owned(),
        operation,
        value,
        new_value,
    };

    let opts = db::key_val::opts::default();
    let db = DB::open(&opts, dunce::simplified(row_changes_path))?;

    maybe_revert_row_changes(&db, row_id.to_owned())?;

    row_changes_db::write_data_frame_row_change(&change, &db)
}

pub fn maybe_revert_row_changes(db: &DB, row_id: String) -> Result<(), OxenError> {
    match row_changes_db::get_data_frame_row_change(db, &row_id) {
        Ok(None) => revert_row_changes(db, row_id),
        Ok(Some(_)) => Ok(()),
        Err(e) => Err(e),
    }
}

pub fn revert_row_changes(db: &DB, row_id: String) -> Result<(), OxenError> {
    row_changes_db::delete_data_frame_row_changes(db, &row_id)
}
