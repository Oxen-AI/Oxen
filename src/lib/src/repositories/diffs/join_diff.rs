use std::collections::{HashMap, HashSet};

use crate::constants::DIFF_STATUS_COL;
use crate::error::OxenError;
use crate::model::data_frame::schema::Field;
use crate::model::diff::tabular_diff::{
    TabularDiffDupes, TabularDiffMods, TabularDiffParameters, TabularDiffSchemas,
    TabularDiffSummary, TabularSchemaDiff,
};
use crate::model::diff::{AddRemoveModifyCounts, DiffResult, TabularDiff};
use crate::model::Schema;
use crate::view::compare::{
    TabularCompareFieldBody, TabularCompareFields, TabularCompareTargetBody,
};

use polars::chunked_array::ops::SortMultipleOptions;
use polars::datatypes::AnyValue;
use polars::lazy::dsl::coalesce;
use polars::lazy::dsl::{all, as_struct, col, GetOutput};
use polars::lazy::frame::IntoLazy;
use polars::prelude::ChunkCompareEq;
use polars::prelude::PlSmallStr;
use polars::prelude::SchemaExt;
use polars::prelude::{Column, NamedFrom};
use polars::prelude::{DataFrame, DataFrameJoinOps};
use polars::series::Series;

use super::{tabular, SchemaDiff};

const TARGETS_HASH_COL: &str = "_targets_hash";
const KEYS_HASH_COL: &str = "_keys_hash";

const DIFF_STATUS_ADDED: &str = "added";
const DIFF_STATUS_REMOVED: &str = "removed";
const DIFF_STATUS_MODIFIED: &str = "modified";
const DIFF_STATUS_UNCHANGED: &str = "unchanged";

pub fn diff(
    df_1: &DataFrame,
    df_2: &DataFrame,
    schema_diff: SchemaDiff,
    keys: &[impl AsRef<str>],
    targets: &[impl AsRef<str>],
    display: &[impl AsRef<str>],
) -> Result<DiffResult, OxenError> {
    if !targets.is_empty() && keys.is_empty() {
        let targets = targets.iter().map(|k| k.as_ref()).collect::<Vec<&str>>();
        return Err(OxenError::basic_str(
            format!("Must specify at least one key column if specifying target columns. Targets: {targets:?}"),
        ));
    }

    let keys: Vec<&str> = keys.iter().map(|k| k.as_ref()).collect();
    let targets: Vec<&str> = targets.iter().map(|k| k.as_ref()).collect();
    let display: Vec<&str> = display.iter().map(|k| k.as_ref()).collect();

    let output_columns = get_output_columns(
        &Schema::from_polars(&df_2.schema()),
        keys.clone(),
        targets.clone(),
        display.clone(),
        schema_diff.clone(),
    );

    log::debug!("df_1 is {:?}", df_1);
    log::debug!("df_2 is {:?}", df_2);
    log::debug!("keys are {:?}", keys);
    log::debug!("targets are {:?}", targets);
    log::debug!("display are {:?}", display);
    log::debug!("output_columns are {:?}", output_columns);

    let joined_df = join_hashed_dfs(
        df_1,
        df_2,
        keys.clone(),
        targets.clone(),
        schema_diff.clone(),
    )?;

    log::debug!("joined_df is {:?}", joined_df);

    let joined_df = add_diff_status_column(joined_df, keys.clone(), targets.clone())?;
    log::debug!("joined_df after add_diff_status_column is {:?}", joined_df);
    let unchanged_vec = vec![DIFF_STATUS_UNCHANGED; joined_df.height()];
    let unchanged_series = Column::Series(Series::new(
        PlSmallStr::from_str(DIFF_STATUS_UNCHANGED),
        unchanged_vec,
    ));
    let mut joined_df = joined_df.filter(
        &joined_df
            .column(DIFF_STATUS_COL)?
            .not_equal(&unchanged_series)?,
    )?;
    log::debug!("joined_df after filter is {:?}", joined_df);
    // Once we've joined and calculated group membership based on .left and .right nullity, coalesce keys
    for key in keys.clone() {
        joined_df = joined_df
            .lazy()
            .with_columns([coalesce(&[
                col(format!("{}.right", key)),
                col(format!("{}.left", key)),
            ])
            .alias(key)])
            .collect()?;
    }
    log::debug!("joined_df after coalesce is {:?}", joined_df);
    let modifications = calculate_compare_mods(&joined_df)?;

    // Sort by all keys with primitive dtypes
    let joined_df = sort_df_on_keys(joined_df, keys.clone())?;

    let _result_fields =
        prepare_response_fields(&schema_diff, keys.clone(), targets.clone(), display.clone());

    let schema_diff = build_compare_schema_diff(schema_diff, df_1, df_2)?;

    let dupes = TabularDiffDupes {
        left: tabular::n_duped_rows(df_1, &[KEYS_HASH_COL])?,
        right: tabular::n_duped_rows(df_2, &[KEYS_HASH_COL])?,
    };

    let schemas = TabularDiffSchemas {
        left: Schema::from_polars(&df_1.schema()),
        right: Schema::from_polars(&df_2.schema()),
        diff: Schema::from_polars(&joined_df.schema()),
    };

    let diff = TabularDiff {
        summary: TabularDiffSummary {
            modifications: TabularDiffMods {
                row_counts: modifications,
                col_changes: schema_diff,
            },
            schemas,
            dupes,
        },
        contents: joined_df.select(output_columns)?,
        parameters: TabularDiffParameters {
            keys: keys.iter().map(|s| s.to_string()).collect(),
            targets: targets.iter().map(|s| s.to_string()).collect(),
            display: display.iter().map(|s| s.to_string()).collect(),
        },
    };
    Ok(DiffResult::Tabular(diff))
}

fn sort_df_on_keys(df: DataFrame, keys: Vec<&str>) -> Result<DataFrame, OxenError> {
    let mut sort_cols = vec![];
    for key in keys.iter() {
        if let Ok(col) = df.column(key) {
            if col.dtype().is_primitive() {
                sort_cols.push(*key);
            }
        }
    }

    if !sort_cols.is_empty() {
        return Ok(df.sort(
            sort_cols,
            SortMultipleOptions::new().with_order_descending(false),
        )?);
    }
    Ok(df)
}

fn build_compare_schema_diff(
    schema_diff: SchemaDiff,
    df_1: &DataFrame,
    df_2: &DataFrame,
) -> Result<TabularSchemaDiff, OxenError> {
    let added_cols = schema_diff
        .added_cols
        .iter()
        .map(|col| {
            let dtype = df_2.column(col)?;
            Ok(Field {
                name: col.clone(),
                dtype: dtype.dtype().to_string(),
                metadata: None,
                changes: None,
            })
        })
        .collect::<Result<Vec<Field>, OxenError>>()?;

    let removed_cols = schema_diff
        .removed_cols
        .iter()
        .map(|col| {
            let dtype = df_1.column(col)?;
            Ok(Field {
                name: col.clone(),
                dtype: dtype.dtype().to_string(),
                metadata: None,
                changes: None,
            })
        })
        .collect::<Result<Vec<Field>, OxenError>>()?;

    Ok(TabularSchemaDiff {
        added: added_cols,
        removed: removed_cols,
    })
}

fn get_output_columns(
    df2_schema: &Schema, // For consistent column ordering
    keys: Vec<&str>,
    targets: Vec<&str>,
    display: Vec<&str>,
    schema_diff: SchemaDiff,
) -> Vec<String> {
    // Keys in df2 order. Targets in df2 order, then any additional. Then display in df2, then any additional.
    let df2_cols_set: HashSet<&str> = df2_schema.fields.iter().map(|f| f.name.as_str()).collect();

    // Get the column index of each column in df2schema
    let mut col_indices: HashMap<&str, usize> = HashMap::new();
    for (i, col) in df2_schema.fields.iter().enumerate() {
        col_indices.insert(col.name.as_str(), i);
    }

    let mut out_columns = vec![];

    let ordered_keys = order_columns_by_schema(keys, &df2_cols_set, &col_indices);
    let ordered_targets = order_columns_by_schema(targets, &df2_cols_set, &col_indices);
    let ordered_display = order_columns_by_schema(display, &df2_cols_set, &col_indices);

    for key in ordered_keys.iter() {
        out_columns.push(key.to_string());
    }

    for target in ordered_targets.iter() {
        if schema_diff.added_cols.contains(&target.to_string()) {
            out_columns.push(format!("{}.right", target));
        } else if schema_diff.removed_cols.contains(&target.to_string()) {
            out_columns.push(format!("{}.left", target));
        } else {
            out_columns.push(format!("{}.left", target));
            out_columns.push(format!("{}.right", target))
        };
    }

    for col in ordered_display.iter() {
        if col.ends_with(".left") {
            let stripped = col.trim_end_matches(".left");
            if schema_diff.removed_cols.contains(&stripped.to_string())
                || schema_diff.unchanged_cols.contains(&stripped.to_string())
            {
                out_columns.push(col.to_string());
            }
        }
        if col.ends_with(".right") {
            let stripped = col.trim_end_matches(".right");
            if schema_diff.added_cols.contains(&stripped.to_string())
                || schema_diff.unchanged_cols.contains(&stripped.to_string())
            {
                out_columns.push(col.to_string());
            }
        }
    }

    out_columns.push(DIFF_STATUS_COL.to_string());
    out_columns
}

fn order_columns_by_schema<'a>(
    columns: Vec<&'a str>,
    df2_cols_set: &HashSet<&'a str>,
    col_indices: &HashMap<&'a str, usize>,
) -> Vec<&'a str> {
    let mut ordered_columns: Vec<&'a str> = columns
        .iter()
        .filter(|col| df2_cols_set.contains(*col))
        .cloned()
        .collect();

    ordered_columns.sort_by_key(|col| *col_indices.get(col).unwrap_or(&usize::MAX));

    ordered_columns
}

fn join_hashed_dfs(
    left_df: &DataFrame,
    right_df: &DataFrame,
    keys: Vec<&str>,
    targets: Vec<&str>,
    schema_diff: SchemaDiff,
) -> Result<DataFrame, OxenError> {
    log::debug!("left_df: {:?}", left_df);
    log::debug!("right_df: {:?}", right_df);

    let mut joined_df = left_df.full_join(right_df, [KEYS_HASH_COL], [KEYS_HASH_COL])?;
    log::debug!("joined_df: {:?}", joined_df);

    let mut cols_to_rename = targets.clone();
    for key in keys.iter() {
        cols_to_rename.push(key);
    }
    // TODO: maybe set logic?
    for col in schema_diff.unchanged_cols.iter() {
        if !cols_to_rename.contains(&col.as_str()) {
            cols_to_rename.push(col);
        }
    }

    if !targets.is_empty() {
        cols_to_rename.push(TARGETS_HASH_COL);
    }

    for col in schema_diff.added_cols.iter() {
        if joined_df.schema().contains(col) {
            joined_df.rename(col, PlSmallStr::from_str(&format!("{}.right", col)))?;
        }
    }

    for col in schema_diff.removed_cols.iter() {
        if joined_df.schema().contains(col) {
            joined_df.rename(col, PlSmallStr::from_str(&format!("{}.left", col)))?;
        }
    }

    for target in cols_to_rename.iter() {
        log::debug!("trying to rename col: {}", target);
        let left_before = target.to_string();
        let left_after = format!("{}.left", target);
        let right_before = format!("{}_right", target);
        let right_after = format!("{}.right", target);
        // Rename conditionally for asymetric targets
        if joined_df.schema().contains(&left_before) {
            joined_df.rename(&left_before, PlSmallStr::from_str(&left_after))?;
        }
        if joined_df.schema().contains(&right_before) {
            joined_df.rename(&right_before, PlSmallStr::from_str(&right_after))?;
        }
    }

    Ok(joined_df)
}

fn add_diff_status_column(
    joined_df: DataFrame,
    keys: Vec<&str>,
    targets: Vec<&str>,
) -> Result<DataFrame, OxenError> {
    // Columns required for determining group membership in the closure
    let col_names = [
        format!("{}.left", keys[0]),
        format!("{}.right", keys[0]),
        format!("{}.left", TARGETS_HASH_COL),
        format!("{}.right", TARGETS_HASH_COL),
    ];

    let mut field_names = vec![];
    for col_name in &col_names {
        if joined_df
            .schema()
            .iter_fields()
            .any(|field| field.name() == col_name)
        {
            field_names.push(col(col_name));
        }
    }

    // For pulling into the closure
    let has_targets = !targets.is_empty();
    let joined_df = joined_df
        .lazy()
        .select([
            all(),
            as_struct(field_names)
                .apply(
                    move |s| {
                        let ca = s.struct_()?;
                        let s_a = &ca.fields_as_series();

                        let num_rows = s_a[0].len();
                        let num_columns = s_a.len();
                        let mut results = vec![];
                        for i in 0..num_rows {
                            // log::debug!("row: {:?}", i);
                            let mut row = vec![];
                            for j_elem in s_a.iter().take(num_columns) {
                                let elem = j_elem.get(i).unwrap();
                                // log::debug!("  elem: {:?}", elem);
                                row.push(elem);
                            }
                            let key_left = row.first();
                            let key_right = row.get(1);
                            let target_hash_left = row.get(2);
                            let target_hash_right = row.get(3);

                            results.push(test_function(
                                key_left,
                                key_right,
                                target_hash_left,
                                target_hash_right,
                                has_targets,
                            ));
                        }
                        Ok(Some(Column::Series(Series::new(
                            PlSmallStr::from_str(""),
                            results,
                        ))))
                    },
                    GetOutput::from_type(polars::prelude::DataType::String),
                )
                .alias(DIFF_STATUS_COL),
        ])
        .collect()?;

    Ok(joined_df)
}

fn calculate_compare_mods(joined_df: &DataFrame) -> Result<AddRemoveModifyCounts, OxenError> {
    // TODO: for reasons which are unclear to me it is ridiculously unclear how
    // to use the polars DSL to get the added, removed, and modified rows as a scalary without
    // filtering down into sub-dataframes or cloning them. This is a workaround for now.

    let mut added_rows = 0;
    let mut removed_rows = 0;
    let mut modified_rows = 0;

    for row in joined_df.column(DIFF_STATUS_COL)?.str()?.into_iter() {
        match row {
            Some("added") => added_rows += 1,
            Some("removed") => removed_rows += 1,
            Some("modified") => modified_rows += 1,
            _ => (),
        }
    }
    Ok(AddRemoveModifyCounts {
        added: added_rows,
        removed: removed_rows,
        modified: modified_rows,
    })
}

fn test_function(
    key_left: Option<&AnyValue>,
    key_right: Option<&AnyValue>,
    target_hash_left: Option<&AnyValue>,
    target_hash_right: Option<&AnyValue>,
    has_targets: bool,
) -> String {
    // TODO better error handling
    if let Some(AnyValue::Null) = key_left {
        return DIFF_STATUS_ADDED.to_string();
    }

    if let Some(AnyValue::Null) = key_right {
        return DIFF_STATUS_REMOVED.to_string();
    }

    if !has_targets {
        return DIFF_STATUS_UNCHANGED.to_string();
    }
    if let Some(target_hash_left) = target_hash_left {
        if let Some(target_hash_right) = target_hash_right {
            if target_hash_left != target_hash_right {
                return DIFF_STATUS_MODIFIED.to_string();
            }
        }
    }
    DIFF_STATUS_UNCHANGED.to_string()
}

fn prepare_response_fields(
    schema_diff: &SchemaDiff,
    keys: Vec<&str>,
    targets: Vec<&str>,
    display: Vec<&str>,
) -> TabularCompareFields {
    let res_keys = keys
        .iter()
        .map(|key| TabularCompareFieldBody {
            left: key.to_string(),
            right: key.to_string(),
            alias_as: None,
            compare_method: None,
        })
        .collect::<Vec<TabularCompareFieldBody>>();

    let mut res_targets: Vec<TabularCompareTargetBody> = vec![];

    for target in targets.iter() {
        if schema_diff.added_cols.contains(&target.to_string()) {
            res_targets.push(TabularCompareTargetBody {
                left: None,
                right: Some(target.to_string()),
                compare_method: None,
            });
        } else if schema_diff.removed_cols.contains(&target.to_string()) {
            res_targets.push(TabularCompareTargetBody {
                left: Some(target.to_string()),
                right: None,
                compare_method: None,
            });
        } else {
            res_targets.push(TabularCompareTargetBody {
                left: Some(target.to_string()),
                right: Some(target.to_string()),
                compare_method: None,
            });
        }
    }

    let mut res_display: Vec<TabularCompareTargetBody> = vec![];
    for disp in display.iter() {
        if schema_diff.added_cols.contains(&disp.to_string()) {
            res_display.push(TabularCompareTargetBody {
                left: None,
                right: Some(disp.to_string()),
                compare_method: None,
            });
        } else if schema_diff.removed_cols.contains(&disp.to_string()) {
            res_display.push(TabularCompareTargetBody {
                left: Some(disp.to_string()),
                right: None,
                compare_method: None,
            });
        } else {
            res_display.push(TabularCompareTargetBody {
                left: Some(disp.to_string()),
                right: Some(disp.to_string()),
                compare_method: None,
            });
        }
    }

    TabularCompareFields {
        keys: res_keys,
        targets: res_targets,
        display: res_display,
    }
}
