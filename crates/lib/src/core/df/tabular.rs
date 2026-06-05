use duckdb::ToSql;
use polars::io::cloud::CloudOptions;
use polars::prelude::*;
use serde_json::json;
use std::collections::HashSet;
use std::fs::File;
use std::num::NonZeroUsize;
use tokio::task;

use crate::constants;
use crate::constants::EXCLUDE_OXEN_COLS;
use crate::core::df::filter::DFLogicalOp;
use crate::core::df::pretty_print;
use crate::core::df::sql;
use crate::error::OxenError;
use crate::io::chunk_reader::ChunkReader;
use crate::model::DataFrameSize;
use crate::model::LocalRepository;
use crate::model::data_frame::schema::DataType;
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::opts::{CountLinesOpts, DFOpts, PaginateOpts};
use crate::repositories;
use crate::storage::{VersionLocation, VersionStore};
use crate::util::fs;
use crate::util::hasher;
use std::sync::Arc;

use comfy_table::Table;
use indicatif::ProgressBar;
use serde_json::Value;
use std::ffi::OsStr;
use std::io::Cursor;
use std::path::Path;

use super::filter::{DFFilterExp, DFFilterOp, DFFilterVal};

const READ_ERROR: &str = "Could not read tabular data from path";

fn base_lazy_csv_reader(
    path: impl AsRef<Path>,
    delimiter: u8,
    quote_char: Option<u8>,
) -> LazyCsvReader {
    let path = path.as_ref();
    let reader = LazyCsvReader::new(path);
    reader
        .with_infer_schema_length(Some(10000))
        .with_ignore_errors(true)
        .with_has_header(true)
        .with_truncate_ragged_lines(true)
        .with_separator(delimiter)
        .with_eol_char(b'\n')
        .with_quote_char(quote_char)
        .with_rechunk(true)
        .with_encoding(CsvEncoding::LossyUtf8)
}

pub fn new_df() -> DataFrame {
    DataFrame::empty()
}

fn read_df_csv(
    path: impl AsRef<Path>,
    delimiter: u8,
    quote_char: Option<u8>,
) -> Result<LazyFrame, OxenError> {
    let reader = base_lazy_csv_reader(path.as_ref(), delimiter, quote_char);
    reader
        .finish()
        .map_err(|_| OxenError::basic_str(format!("{}: {:?}", READ_ERROR, path.as_ref())))
}

fn read_df_jsonl(path: impl AsRef<Path>) -> Result<LazyFrame, OxenError> {
    let path = path
        .as_ref()
        .to_str()
        .ok_or_else(|| OxenError::basic_str("Could not convert path to string"))?;
    LazyJsonLineReader::new(path)
        .with_infer_schema_length(Some(NonZeroUsize::new(10000).unwrap()))
        .finish()
        .map_err(|_| OxenError::basic_str(format!("{READ_ERROR}: {path:?}")))
}

fn scan_df_json(path: impl AsRef<Path>) -> Result<LazyFrame, OxenError> {
    // cannot lazy read json array
    let df = read_df_json(path)?;
    Ok(df)
}

pub fn read_df_json(path: impl AsRef<Path>) -> Result<LazyFrame, OxenError> {
    let path = path.as_ref();
    let error_str = format!("Could not read json data from path {path:?}");
    let file = File::open(path)?;
    let df = JsonReader::new(file)
        .infer_schema_len(Some(NonZeroUsize::new(10000).unwrap()))
        .finish()
        .map_err(|e| OxenError::basic_str(format!("{error_str}: {e}")))?;
    Ok(df.lazy())
}

pub fn read_df_parquet(path: impl AsRef<Path>) -> Result<LazyFrame, OxenError> {
    let args = ScanArgsParquet {
        n_rows: None,
        ..Default::default()
    };
    // log::debug!(
    //     "scan_df_parquet_n_rows path: {:?} n_rows: {:?}",
    //     path.as_ref(),
    //     args.n_rows
    // )
    LazyFrame::scan_parquet(&path, args).map_err(|_| {
        OxenError::basic_str(format!(
            "Error scanning parquet file {}: {:?}",
            READ_ERROR,
            path.as_ref()
        ))
    })
}

fn read_df_arrow(path: impl AsRef<Path>) -> Result<LazyFrame, OxenError> {
    LazyFrame::scan_ipc(&path, ScanArgsIpc::default())
        .map_err(|_| OxenError::basic_str(format!("{}: {:?}", READ_ERROR, path.as_ref())))
}

pub fn take(df: LazyFrame, indices: Vec<u32>) -> Result<DataFrame, OxenError> {
    let idx = IdxCa::new(PlSmallStr::from_str("idx"), &indices);
    let collected = df
        .collect()
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    // log::debug!("take indices {:?}", indices);
    // log::debug!("from df {:?}", collected);
    collected
        .take(&idx)
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))
}

pub fn scan_df_csv(
    path: impl AsRef<Path>,
    delimiter: u8,
    quote_char: Option<u8>,
    total_rows: usize,
) -> Result<LazyFrame, OxenError> {
    let reader = base_lazy_csv_reader(path.as_ref(), delimiter, quote_char);
    reader
        .with_n_rows(Some(total_rows))
        .finish()
        .map_err(|_| OxenError::basic_str(format!("{}: {:?}", READ_ERROR, path.as_ref())))
}

pub fn scan_df_jsonl(path: impl AsRef<Path>, total_rows: usize) -> Result<LazyFrame, OxenError> {
    let path = path
        .as_ref()
        .to_str()
        .ok_or_else(|| OxenError::basic_str("Could not convert path to string"))?;
    LazyJsonLineReader::new(path)
        .with_infer_schema_length(Some(NonZeroUsize::new(10000).unwrap()))
        .with_n_rows(Some(total_rows))
        .finish()
        .map_err(|_| OxenError::basic_str(format!("{READ_ERROR}: {path:?}")))
}

pub fn scan_df_parquet(path: impl AsRef<Path>, total_rows: usize) -> Result<LazyFrame, OxenError> {
    let args = ScanArgsParquet {
        n_rows: Some(total_rows),
        ..Default::default()
    };
    // log::debug!(
    //     "scan_df_parquet_n_rows path: {:?} n_rows: {:?}",
    //     path.as_ref(),
    //     args.n_rows
    // );
    LazyFrame::scan_parquet(&path, args).map_err(|_| {
        OxenError::basic_str(format!(
            "Error scanning parquet file {}: {:?}",
            READ_ERROR,
            path.as_ref()
        ))
    })
}

pub fn scan_df_arrow(path: impl AsRef<Path>, total_rows: usize) -> Result<LazyFrame, OxenError> {
    let args = ScanArgsIpc {
        n_rows: Some(total_rows),
        ..Default::default()
    };

    LazyFrame::scan_ipc(&path, args)
        .map_err(|_| OxenError::basic_str(format!("{}: {:?}", READ_ERROR, path.as_ref())))
}

pub async fn add_col_lazy(
    df: LazyFrame,
    name: &str,
    val: &str,
    dtype: &str,
    at: Option<usize>,
) -> Result<LazyFrame, OxenError> {
    let mut df = match task::spawn_blocking(move || -> Result<DataFrame, OxenError> {
        df.collect()
            .map_err(|e| OxenError::basic_str(format!("{e:?}")))
    })
    .await?
    {
        Ok(df) => df,
        Err(e) => return Err(OxenError::basic_str(format!("{e:?}"))),
    };

    let dtype = DataType::from_string(dtype).to_polars();

    let column = Series::new_empty(PlSmallStr::from_str(name), &dtype);
    let column = column
        .extend_constant(val_from_str_and_dtype(val, &dtype), df.height())
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    if let Some(at) = at {
        df.insert_column(at, column)
            .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    } else {
        df.with_column(column)
            .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    }
    let df = df.lazy();
    Ok(df)
}

pub fn add_col(
    mut df: DataFrame,
    name: &str,
    val: &str,
    dtype: &str,
) -> Result<DataFrame, OxenError> {
    let dtype = DataType::from_string(dtype).to_polars();

    let column = Series::new_empty(PlSmallStr::from_str(name), &dtype);
    let column = column
        .extend_constant(val_from_str_and_dtype(val, &dtype), df.height())
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    df.with_column(column)
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    Ok(df)
}

pub async fn add_row(df: LazyFrame, data: String) -> Result<LazyFrame, OxenError> {
    let df = match task::spawn_blocking(move || -> Result<DataFrame, OxenError> {
        df.collect()
            .map_err(|e| OxenError::basic_str(format!("{e:?}")))
    })
    .await?
    {
        Ok(df) => df,
        Err(e) => return Err(OxenError::basic_str(format!("{e:?}"))),
    };

    let new_row = row_from_str_and_schema(data, df.schema())?;
    log::debug!("add_row og df: {df:?}");
    log::debug!("add_row new_row: {new_row:?}");
    let df = df
        .vstack(&new_row)
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?
        .lazy();
    Ok(df)
}

pub fn n_duped_rows(df: &DataFrame, cols: &[&str]) -> Result<u64, OxenError> {
    let cols = cols
        .iter()
        .map(|c| PlSmallStr::from_str(c))
        .collect::<Vec<PlSmallStr>>();
    let dupe_mask = df.select(cols)?.is_duplicated()?;
    let n_dupes = dupe_mask.sum().unwrap() as u64; // Can unwrap - sum implemented for boolean
    Ok(n_dupes)
}

pub fn row_from_str_and_schema(
    data: impl AsRef<str>,
    schema: &SchemaRef,
) -> Result<DataFrame, OxenError> {
    if serde_json::from_str::<Value>(data.as_ref()).is_ok() {
        return parse_str_to_df(data);
    }

    let values: Vec<&str> = data.as_ref().split(',').collect();

    if values.len() != schema.len() {
        return Err(OxenError::basic_str(format!(
            "Error: Added row must have same number of columns as df\nRow columns: {}\ndf columns: {}",
            values.len(),
            schema.len()
        )));
    }

    let mut vec: Vec<Column> = Vec::new();

    for ((name, dtype), value) in schema.iter_names_and_dtypes().zip(values) {
        let typed_val = val_from_str_and_dtype(value, dtype);
        match Series::from_any_values_and_dtype(name.clone(), &[typed_val], dtype, false) {
            Ok(series) => {
                vec.push(Column::Series(series.into()));
            }
            Err(err) => {
                return Err(OxenError::basic_str(format!("Error parsing json: {err}")));
            }
        }
    }

    let df = DataFrame::new(vec)?;

    Ok(df)
}

pub fn parse_str_to_df(data: impl AsRef<str>) -> Result<DataFrame, OxenError> {
    let data = data.as_ref();

    if data == "{}" {
        return Ok(DataFrame::default());
    }

    let cursor = Cursor::new(data.as_bytes());

    let reader = JsonLineReader::new(cursor);

    match reader.finish() {
        Ok(df) => Ok(df),
        Err(err) => Err(OxenError::basic_str(format!("Error parsing json: {err}"))),
    }
}

pub fn parse_json_to_df(data: &serde_json::Value) -> Result<DataFrame, OxenError> {
    let data = serde_json::to_string(data)?;
    parse_str_to_df(data)
}

fn val_from_str_and_dtype<'a>(s: &'a str, dtype: &polars::prelude::DataType) -> AnyValue<'a> {
    match dtype {
        polars::prelude::DataType::Boolean => {
            AnyValue::Boolean(s.parse::<bool>().expect("val must be bool"))
        }
        polars::prelude::DataType::UInt8 => AnyValue::UInt8(s.parse::<u8>().expect("must be u8")),
        polars::prelude::DataType::UInt16 => {
            AnyValue::UInt16(s.parse::<u16>().expect("must be u16"))
        }
        polars::prelude::DataType::UInt32 => {
            AnyValue::UInt32(s.parse::<u32>().expect("must be u32"))
        }
        polars::prelude::DataType::UInt64 => {
            AnyValue::UInt64(s.parse::<u64>().expect("must be u64"))
        }
        polars::prelude::DataType::Int8 => AnyValue::Int8(s.parse::<i8>().expect("must be i8")),
        polars::prelude::DataType::Int16 => AnyValue::Int16(s.parse::<i16>().expect("must be i16")),
        polars::prelude::DataType::Int32 => AnyValue::Int32(s.parse::<i32>().expect("must be i32")),
        polars::prelude::DataType::Int64 => AnyValue::Int64(s.parse::<i64>().expect("must be i64")),
        polars::prelude::DataType::Float32 => {
            AnyValue::Float32(s.parse::<f32>().expect("must be f32"))
        }
        polars::prelude::DataType::Float64 => {
            AnyValue::Float64(s.parse::<f64>().expect("must be f64"))
        }
        polars::prelude::DataType::String => AnyValue::String(s),
        polars::prelude::DataType::Null => AnyValue::Null,
        _ => panic!("Currently do not support data type {dtype}"),
    }
}

fn val_from_df_and_filter<'a>(
    df: &mut LazyFrame,
    filter: &'a DFFilterVal,
) -> Result<AnyValue<'a>, OxenError> {
    // Resolving the schema collects metadata; for a cloud-backed frame this is real S3 IO, so
    // propagate the error rather than panicking on a transient failure.
    let schema = df.collect_schema()?;
    if let Some(value) = schema.iter_fields().find(|f| f.name == filter.field) {
        Ok(val_from_str_and_dtype(&filter.value, value.dtype()))
    } else {
        log::error!("Unknown field {:?}", filter.field);
        Ok(AnyValue::Null)
    }
}

fn lit_from_any(value: &AnyValue) -> Expr {
    match value {
        AnyValue::Boolean(val) => lit(*val),
        AnyValue::Float64(val) => lit(*val),
        AnyValue::Float32(val) => lit(*val),
        AnyValue::Int64(val) => lit(*val),
        AnyValue::Int32(val) => lit(*val),
        AnyValue::String(val) => lit(*val),
        AnyValue::StringOwned(val) => lit(val.to_string()),
        val => panic!("Unknown data type for [{val}] to create literal"),
    }
}

fn filter_from_val(df: &mut LazyFrame, filter: &DFFilterVal) -> Result<Expr, OxenError> {
    let val = val_from_df_and_filter(df, filter)?;
    let val = lit_from_any(&val);
    Ok(match filter.op {
        DFFilterOp::EQ => col(&filter.field).eq(val),
        DFFilterOp::GT => col(&filter.field).gt(val),
        DFFilterOp::LT => col(&filter.field).lt(val),
        DFFilterOp::GTE => col(&filter.field).gt_eq(val),
        DFFilterOp::LTE => col(&filter.field).lt_eq(val),
        DFFilterOp::NEQ => col(&filter.field).neq(val),
    })
}

fn filter_df(mut df: LazyFrame, filter: &DFFilterExp) -> Result<LazyFrame, OxenError> {
    log::debug!("Got filter: {filter:?}");
    if filter.vals.is_empty() {
        return Ok(df);
    }

    let mut vals = filter.vals.iter();
    let mut expr: Expr = filter_from_val(&mut df, vals.next().unwrap())?;
    for op in &filter.logical_ops {
        let chain_expr: Expr = filter_from_val(&mut df, vals.next().unwrap())?;

        match op {
            DFLogicalOp::AND => expr = expr.and(chain_expr),
            DFLogicalOp::OR => expr = expr.or(chain_expr),
        }
    }

    Ok(df.filter(expr))
}

fn unique_df(df: LazyFrame, columns: Vec<String>) -> Result<LazyFrame, OxenError> {
    log::debug!("Got unique: {columns:?}");
    Ok(df.unique(Some(columns), UniqueKeepStrategy::First))
}

fn unique_count_df(df: LazyFrame, columns: Vec<String>) -> Result<LazyFrame, OxenError> {
    log::debug!("Got unique_count: {columns:?}");
    let group_by_cols: Vec<Expr> = columns.iter().map(col).collect();
    Ok(df.group_by(group_by_cols).agg([len().alias("count")]))
}

pub async fn transform(df: DataFrame, opts: DFOpts) -> Result<DataFrame, OxenError> {
    let df = transform_lazy(df.lazy(), opts.clone()).await?;
    Ok(transform_slice_lazy(df, &opts)?.collect()?)
}

pub async fn transform_lazy(mut df: LazyFrame, opts: DFOpts) -> Result<LazyFrame, OxenError> {
    // INVARIANT: this runs on the caller's async runtime. Any eager Polars work added below
    // (`.collect()` / `.collect_schema()`) must run inside `spawn_blocking` (see the filter and
    // take branches): a cloud-backed `LazyFrame` drives `block_in_place`, which panics on the
    // server's current-thread runtime.
    log::debug!("transform_lazy Got transform ops {opts:?}");
    if let Some(vstack) = opts.clone().vstack {
        log::debug!("transform_lazy Got files to stack {vstack:?}");
        let mut frames = vec![df];
        for path in vstack.iter() {
            let empty_opts = DFOpts::empty();
            let extension = path.extension().and_then(OsStr::to_str).ok_or_else(|| {
                OxenError::basic_str(format!("Cannot vstack file without extension: {path:?}"))
            })?;

            let new_df = _read_lazy_df_with_extension(path.clone(), extension, &empty_opts).await?;
            frames.push(new_df);
        }
        df = concat(frames, Default::default())
            .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    }

    if let Some(col_vals) = opts.add_col_vals() {
        df = add_col_lazy(
            df,
            &col_vals.name,
            &col_vals.value,
            &col_vals.dtype,
            opts.at,
        )
        .await?;
    }

    if let Some(data) = &opts.add_row {
        df = add_row(df, data.to_owned()).await?;
    }

    match opts.get_filter() {
        Ok(filter) => {
            if let Some(filter) = filter {
                // `filter_df` resolves the frame schema (`collect_schema`) to build the predicate;
                // run it off the async runtime so a cloud frame doesn't block_in_place-panic.
                df = task::spawn_blocking(move || filter_df(df, &filter)).await??;
            }
        }
        Err(err) => {
            log::error!("Could not parse filter: {err}");
        }
    }

    if let Some(sql) = opts.sql.clone()
        && let Some(repo_dir) = opts.repo_dir.as_ref()
    {
        let repo = LocalRepository::from_dir(repo_dir)?;
        df = sql::query_df_from_repo(sql, &repo, &opts.path.clone().unwrap_or_default(), &opts)
            .await?
            .lazy();
    }

    if opts.should_randomize {
        log::debug!("transform_lazy randomizing df");
        let shuffle_col = "__oxen_shuffle";
        df = df
            .with_row_index(shuffle_col, None)
            .with_column(col(shuffle_col).shuffle(Some(rand::random())))
            .sort([shuffle_col], Default::default())
            .drop([shuffle_col]);
    }

    if let Some(columns) = opts.unique_columns() {
        df = unique_df(df, columns)?;
    }

    if let Some(columns) = opts.unique_count_columns() {
        df = unique_count_df(df, columns)?;
    }

    if let Some(sort_by) = &opts.sort_by {
        df = df.sort([sort_by], Default::default());
    }

    if opts.should_reverse {
        df = df.reverse();
    }

    if let Some(columns) = opts.columns_names()
        && !columns.is_empty()
    {
        log::debug!("transform_lazy selecting columns: {columns:?}");
        let cols = columns.iter().map(col).collect::<Vec<Expr>>();
        df = df.select(&cols);
    }

    if let Some(names) = &opts.rename_col {
        if names.contains(":") {
            let parts = names.split(":").collect::<Vec<&str>>();
            let old_name = parts[0];
            let new_name = parts[1];

            if old_name.is_empty() || new_name.is_empty() {
                log::error!("Invalid rename_col format: old name and new name cannot be empty");
                return Err(OxenError::basic_str(format!(
                    "Invalid rename_col format `{names}`"
                )));
            }

            df = df.rename([old_name], [new_name], true);
        } else {
            log::error!("Invalid rename_col format: {names}");
            return Err(OxenError::basic_str(format!(
                "Invalid rename_col format '{names}', expected 'old_name:new_name'"
            )));
        }
    }

    // These ops should be the last ops since they depends on order
    if let Some(indices) = opts.take_indices() {
        // `take` collects the frame, so run it off the async runtime: a cloud frame's collect
        // drives Polars `block_in_place`, which panics on the server's current-thread runtime.
        // Mirrors the spawn_blocking-wrapped collects in `add_col_lazy`/`add_row`.
        let df_to_take = df.clone();
        match task::spawn_blocking(move || take(df_to_take, indices)).await? {
            Ok(new_df) => {
                df = new_df.lazy();
            }
            Err(err) => {
                log::error!("error taking indices from df {err:?}")
            }
        }
    }
    Ok(df)
}

// Separate out slice transform because it needs to be done after other transforms
pub fn transform_slice_lazy(mut df: LazyFrame, opts: &DFOpts) -> Result<LazyFrame, OxenError> {
    // Maybe slice it up
    df = slice(df, opts);
    df = head(df, opts);
    df = tail(df, opts);

    if let Some(item) = opts.column_at() {
        // `collect` is real S3 IO for a cloud-backed frame, and col/index come from user input;
        // propagate errors rather than panicking via `unwrap`.
        let full_df = df.collect()?;
        let value = full_df.column(&item.col)?.get(item.index)?;
        let s1 = Column::Series(Series::new(PlSmallStr::from_str(""), &[value]).into());
        let df = DataFrame::new(vec![s1])?;
        return Ok(df.lazy());
    }

    log::debug!("transform_slice_lazy before collect");
    Ok(df)
}

pub fn strip_excluded_cols(df: DataFrame) -> Result<DataFrame, OxenError> {
    let schema = df.schema();
    let excluded_cols = EXCLUDE_OXEN_COLS
        .iter()
        .map(|c| c.to_string())
        .collect::<HashSet<String>>();
    let fields = schema
        .iter_fields()
        .map(|f| f.name.to_string())
        .collect::<Vec<String>>();
    let select_cols = fields
        .iter()
        .filter(|c| !excluded_cols.contains(*c))
        .map(|c| c.to_string())
        .collect::<Vec<String>>();
    let cols = select_cols.iter().map(col).collect::<Vec<Expr>>();
    let result = df.lazy().select(&cols).collect();
    result.map_err(|e| OxenError::basic_str(format!("{e:?}")))
}

fn head(df: LazyFrame, opts: &DFOpts) -> LazyFrame {
    if let Some(head) = opts.head {
        df.slice(0, head as u32)
    } else {
        df
    }
}

fn tail(df: LazyFrame, opts: &DFOpts) -> LazyFrame {
    if let Some(tail) = opts.tail {
        df.slice(-(tail as i64), tail as u32)
    } else {
        df
    }
}

pub fn slice_df(df: DataFrame, start: usize, end: usize) -> Result<DataFrame, OxenError> {
    let mut opts = DFOpts::empty();
    opts.slice = Some(format!("{start}..{end}"));
    log::debug!("slice_df with opts: {opts:?}");
    let df = df.lazy();
    let df = slice(df, &opts);
    df.collect()
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))
}

pub fn paginate_df(df: DataFrame, page_opts: &PaginateOpts) -> Result<DataFrame, OxenError> {
    let mut opts = DFOpts::empty();
    opts.slice = Some(format!(
        "{}..{}",
        page_opts.page_size * (page_opts.page_num - 1),
        page_opts.page_size * page_opts.page_num
    ));
    let df = df.lazy();
    let df = slice(df, &opts);
    df.collect()
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))
}

fn slice(df: LazyFrame, opts: &DFOpts) -> LazyFrame {
    log::debug!("SLICE {:?}", opts.slice);
    if let Some((start, end)) = opts.slice_indices() {
        log::debug!("SLICE with indices {start:?}..{end:?}");
        if start >= end {
            panic!("Slice error: Start must be greater than end.");
        }
        let len = end - start;
        df.slice(start, len as u32)
    } else {
        df
    }
}

pub fn df_add_row_num(df: DataFrame) -> Result<DataFrame, OxenError> {
    df.with_row_index(PlSmallStr::from_str(constants::ROW_NUM_COL_NAME), Some(0))
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))
}

pub fn df_add_row_num_starting_at(df: DataFrame, start: u32) -> Result<DataFrame, OxenError> {
    df.with_row_index(
        PlSmallStr::from_str(constants::ROW_NUM_COL_NAME),
        Some(start),
    )
    .map_err(|e| OxenError::basic_str(format!("{e:?}")))
}

pub fn any_val_to_bytes(value: &AnyValue) -> Vec<u8> {
    match value {
        AnyValue::Null => Vec::<u8>::new(),
        AnyValue::Int64(val) => val.to_le_bytes().to_vec(),
        AnyValue::Int32(val) => val.to_le_bytes().to_vec(),
        AnyValue::Int8(val) => val.to_le_bytes().to_vec(),
        AnyValue::Float32(val) => val.to_le_bytes().to_vec(),
        AnyValue::Float64(val) => val.to_le_bytes().to_vec(),
        AnyValue::String(val) => val.as_bytes().to_vec(),
        AnyValue::StringOwned(val) => val.as_bytes().to_vec(),
        AnyValue::Boolean(val) => vec![if *val { 1 } else { 0 }],
        // TODO: handle rows with lists...
        // AnyValue::List(val) => {
        //     match val.dtype() {
        //         DataType::Int32 => {},
        //         DataType::Float32 => {},
        //         DataType::String => {},
        //         DataType::UInt8 => {},
        //         x => panic!("unable to parse list with value: {} and type: {:?}", x, x.inner_dtype())
        //     }
        // },
        AnyValue::Datetime(val, TimeUnit::Milliseconds, _) => val.to_le_bytes().to_vec(),
        _ => value.to_string().as_bytes().to_vec(),
    }
}

pub fn any_val_to_json(value: AnyValue) -> Value {
    match value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(b) => Value::Bool(b),
        AnyValue::Int32(n) => json!(n),
        AnyValue::Int64(n) => json!(n),
        AnyValue::Float32(f) => json!(f),
        AnyValue::Float64(f) => json!(f),
        AnyValue::String(s) => Value::String(s.to_string()),
        AnyValue::StringOwned(s) => Value::String(s.to_string()),
        AnyValue::List(l) => series_to_json_array(&l),
        AnyValue::StructOwned(s) => {
            let mut map = serde_json::Map::new();

            let values = &s.0; // &Vec<AnyValue<'_>>
            let fields = &s.1; // &Vec<Field>

            for (i, field) in fields.iter().enumerate() {
                if let Some(value) = values.get(i) {
                    let field_name = field.name();
                    let field_json = any_val_to_json(value.clone());

                    map.insert(field_name.to_string(), field_json);
                }
            }

            Value::Object(map)
        }
        AnyValue::Struct(_len, struct_array, fields) => struct_array_to_json(struct_array, fields),

        other => panic!("Unsupported dtype in JSON conversion: {other:?}"),
    }
}

fn series_to_json_array(series: &polars::prelude::Series) -> Value {
    let mut array = Vec::with_capacity(series.len());
    for i in 0..series.len() {
        let val = series.get(i).unwrap_or(AnyValue::Null);
        array.push(any_val_to_json(val));
    }
    Value::Array(array)
}

fn struct_array_to_json(
    struct_array: &polars::prelude::StructArray,
    fields: &[polars::prelude::Field],
) -> Value {
    match DataFrame::try_from(struct_array.clone()) {
        Ok(df) => {
            let mut rows = Vec::with_capacity(df.height());
            for i in 0..df.height() {
                let mut map = serde_json::Map::new();
                for col in df.get_columns() {
                    let val = col.get(i).unwrap_or(AnyValue::Null);
                    map.insert(col.name().to_string(), any_val_to_json(val));
                }
                rows.push(Value::Object(map));
            }
            Value::Array(rows)
        }
        Err(e) => {
            log::warn!("Failed to convert StructArray to DataFrame: {e:?}");
            let mut map = serde_json::Map::new();
            for (i, field) in fields.iter().enumerate() {
                if let Some(values) = struct_array.values().get(i) {
                    let len = values.len();
                    if len > 0 {
                        let val_str = format!("{values:?}");
                        // Look for actual URL or string content
                        if let Some(start) = val_str.find("https://") {
                            // Extract URL starting from https://
                            let rest = &val_str[start..];
                            if let Some(end) = rest.find('"').or(rest.find(']')) {
                                let url = &rest[..end];
                                map.insert(
                                    field.name().to_string(),
                                    Value::String(url.to_string()),
                                );
                                continue;
                            }
                        } else if let Some(start) = val_str.find("http://") {
                            let rest = &val_str[start..];
                            if let Some(end) = rest.find('"').or(rest.find(']')) {
                                let url = &rest[..end];
                                map.insert(
                                    field.name().to_string(),
                                    Value::String(url.to_string()),
                                );
                                continue;
                            }
                        }
                    }
                }
                map.insert(field.name().to_string(), Value::Null);
            }
            if map.is_empty() {
                Value::Null
            } else {
                Value::Object(map)
            }
        }
    }
}

fn polars_time_unit_to_duckdb(tu: TimeUnit) -> duckdb::types::TimeUnit {
    match tu {
        TimeUnit::Nanoseconds => duckdb::types::TimeUnit::Nanosecond,
        TimeUnit::Microseconds => duckdb::types::TimeUnit::Microsecond,
        TimeUnit::Milliseconds => duckdb::types::TimeUnit::Millisecond,
    }
}

pub fn value_to_tosql(value: AnyValue) -> Box<dyn ToSql> {
    match value {
        AnyValue::String(s) => Box::new(s.to_string()),
        AnyValue::StringOwned(s) => Box::new(s.to_string()),
        AnyValue::Int32(n) => Box::new(n),
        AnyValue::Int64(n) => Box::new(n),
        AnyValue::Float32(f) => Box::new(f),
        AnyValue::Float64(f) => Box::new(f),
        AnyValue::Boolean(b) => Box::new(b),
        AnyValue::Null => Box::new(None::<i32>),
        AnyValue::Datetime(val, tu, _tz) => Box::new(duckdb::types::Value::Timestamp(
            polars_time_unit_to_duckdb(tu),
            val,
        )),
        AnyValue::DatetimeOwned(val, tu, _tz) => Box::new(duckdb::types::Value::Timestamp(
            polars_time_unit_to_duckdb(tu),
            val,
        )),
        AnyValue::List(l) => Box::new(series_to_json_array(&l).to_string()),
        AnyValue::StructOwned(s) => {
            let json_value = any_val_to_json(AnyValue::StructOwned(s));
            Box::new(json_value.to_string())
        }
        other => panic!("Unsupported dtype: {other:?}"),
    }
}

pub fn df_hash_rows(df: DataFrame) -> Result<DataFrame, OxenError> {
    let num_rows = df.height() as i64;

    let mut col_names = vec![];
    let schema = df.schema();
    for field in schema.iter_fields() {
        col_names.push(col(field.name().clone()));
    }
    // println!("Hashing: {:?}", col_names);
    // println!("{:?}", df);

    let df = df
        .lazy()
        .select([
            all(),
            as_struct(col_names)
                .apply(
                    move |s| {
                        // log::debug!("s: {:?}", s);

                        let pb = ProgressBar::new(num_rows as u64);
                        // downcast to struct
                        let ca = s.struct_()?;
                        let s_a = &ca.fields_as_series();
                        let num_rows = s_a[0].len();

                        let mut hashes = vec![];
                        for i in 0..num_rows {
                            // log::debug!("row: {:?}", i);
                            let mut buffer: Vec<u8> = vec![];
                            for series in s_a.iter() {
                                let elem = series.get(i).unwrap();
                                // log::debug!("column: {:?} elem: {:?}", j, elem);
                                let mut elem_bytes = any_val_to_bytes(&elem);
                                buffer.append(&mut elem_bytes);
                            }
                            pb.inc(1);
                            let result = hasher::hash_buffer(&buffer);
                            hashes.push(result);
                        }
                        pb.finish_and_clear();

                        Ok(Some(Column::Series(
                            Series::new(PlSmallStr::from_str(""), hashes).into(),
                        )))
                    },
                    GetOutput::from_type(polars::prelude::DataType::String),
                )
                .alias(constants::ROW_HASH_COL_NAME),
        ])
        .collect()
        .unwrap();
    log::debug!("Hashed rows: {df}");
    Ok(df)
}

// Maybe pass in fields here?
pub fn df_hash_rows_on_cols(
    df: DataFrame,
    hash_fields: &[String],
    out_col_name: &str,
) -> Result<DataFrame, PolarsError> {
    let num_rows = df.height() as i64;

    log::debug!("df_hash_rows_on_cols df is {df:?}");
    log::debug!("df_hash_rows_on_cols hash_fields is {hash_fields:?}");
    log::debug!("df_hash_rows_on_cols out_col_name is {out_col_name:?}");

    // Create a vector to store columns to be hashed
    let col_names = df
        .schema()
        .iter_fields()
        .filter_map(|field| {
            let field_name = field.name();
            if hash_fields.contains(&field_name.to_string()) {
                Some(col(field_name.clone()))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // This is to allow asymmetric target hashing for added / removed cols in default behavior
    if col_names.is_empty() {
        let null_string_col = lit(Null {}).alias(out_col_name);
        return df.lazy().with_column(null_string_col).collect();
    }

    // Continue as before
    let df = df
        .lazy()
        .select([
            all(),
            as_struct(col_names)
                .apply(
                    move |s| {
                        let pb = ProgressBar::new(num_rows as u64);
                        // downcast to struct
                        let ca = s.struct_()?;
                        let s_a = &ca.fields_as_series();
                        let num_rows = s_a[0].len();

                        let mut hashes = vec![];
                        for i in 0..num_rows {
                            // log::debug!("row: {:?}", i);
                            let mut buffer: Vec<u8> = vec![];
                            for series in s_a.iter() {
                                let elem = series.get(i).unwrap();
                                // log::debug!("\telem: {:?}", elem);
                                let mut elem_bytes = any_val_to_bytes(&elem);
                                buffer.append(&mut elem_bytes);
                            }
                            pb.inc(1);
                            let result = hasher::hash_buffer(&buffer);
                            hashes.push(result);
                        }
                        pb.finish_and_clear();

                        Ok(Some(Column::Series(
                            Series::new(PlSmallStr::from_str(""), hashes).into(),
                        )))
                    },
                    GetOutput::from_type(polars::prelude::DataType::String),
                )
                .alias(out_col_name),
        ])
        .collect()?;
    log::debug!("Hashed rows: {df}");
    Ok(df)
}

struct CsvDialect {
    delimiter: u8,
    quote_char: Option<u8>,
}

/// Parse the user-specified CSV delimiter/quote overrides from `DFOpts`. Either may be `None`, in
/// which case that value is sniffed from the data instead.
fn parse_user_csv_opts(opts: &DFOpts) -> Result<(Option<u8>, Option<u8>), OxenError> {
    let user_delimiter = match &opts.delimiter {
        Some(delimiter) => {
            if delimiter.len() != 1 {
                return Err(OxenError::InvalidDelimiter);
            }
            Some(delimiter.as_bytes()[0])
        }
        None => None,
    };

    let user_quote = match opts.quote_char.as_ref() {
        Some(s) => {
            let b = s.as_bytes();
            if !b.is_empty() {
                Some(b[0])
            } else {
                return Err(OxenError::InvalidQuoteChar);
            }
        }
        None => None,
    };

    Ok((user_delimiter, user_quote))
}

/// Combine sniffer output with the user overrides into a final dialect, falling back to RFC 4180
/// defaults when sniffing failed.
fn build_csv_dialect<E: std::fmt::Debug>(
    sniffed: Result<qsv_sniffer::metadata::Metadata, E>,
    user_delimiter: Option<u8>,
    user_quote: Option<u8>,
) -> CsvDialect {
    match sniffed {
        Ok(metadata) => {
            log::debug!("Sniffed csv dialect: {:?}", metadata.dialect);
            CsvDialect {
                delimiter: user_delimiter.unwrap_or(metadata.dialect.delimiter),
                quote_char: user_quote.or_else(|| Some(sniffed_quote(&metadata))),
            }
        }
        Err(err) => {
            log::warn!("Error sniffing csv -> {err:?}");
            CsvDialect {
                delimiter: user_delimiter.unwrap_or(DEFAULT_DELIMITER),
                quote_char: user_quote.or(Some(DEFAULT_QUOTE_CHAR)),
            }
        }
    }
}

fn sniff_csv_dialect(path: impl AsRef<Path>, opts: &DFOpts) -> Result<CsvDialect, OxenError> {
    let (user_delimiter, user_quote) = parse_user_csv_opts(opts)?;
    if let (Some(delimiter), Some(_)) = (user_delimiter, user_quote) {
        return Ok(CsvDialect {
            delimiter,
            quote_char: user_quote,
        });
    }
    Ok(build_csv_dialect(
        qsv_sniffer::Sniffer::new().sniff_path(&path),
        user_delimiter,
        user_quote,
    ))
}

const DEFAULT_QUOTE_CHAR: u8 = b'"';
const DEFAULT_DELIMITER: u8 = b',';

/// Never disable quoting from the sniffer: extract the detected
/// quote char or fall back to the RFC 4180 double-quote default.
fn sniffed_quote(metadata: &qsv_sniffer::metadata::Metadata) -> u8 {
    match metadata.dialect.quote {
        qsv_sniffer::metadata::Quote::Some(ch) => ch,
        qsv_sniffer::metadata::Quote::None => DEFAULT_QUOTE_CHAR,
    }
}

pub async fn read_df(path: impl AsRef<Path>, opts: DFOpts) -> Result<DataFrame, OxenError> {
    log::debug!("Reading df with path: {:?}", path.as_ref());
    let path = path.as_ref();
    if !path.exists() {
        return Err(OxenError::path_does_not_exist(path));
    }

    let extension = path.extension().and_then(OsStr::to_str);

    if let Some(extension) = extension {
        read_df_with_extension(path, extension, &opts).await
    } else {
        let err =
            format!("Could not load data frame with path: {path:?} and extension: {extension:?}");
        Err(OxenError::basic_str(err))
    }
}

/// Read a tabular file from a local filesystem path. For version files held by a `VersionStore`,
/// use [`read_version_df`] instead so access goes through the store's byte interface.
pub async fn read_df_with_extension(
    path: impl AsRef<Path>,
    extension: impl AsRef<str>,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref().to_path_buf();
    p_read_df_with_extension(path, extension.as_ref(), opts.clone()).await
}

async fn _read_lazy_df_with_extension(
    path: impl AsRef<Path> + Send + 'static,
    extension: impl AsRef<str>,
    opts: &DFOpts,
) -> Result<LazyFrame, OxenError> {
    let path = path.as_ref().to_path_buf();
    let extension = extension.as_ref().to_string();
    let opts_clone = opts.clone();

    task::spawn_blocking(move || {
        if !path.exists() {
            return Err(OxenError::entry_does_not_exist(&path));
        }

        log::debug!("Reading df with extension {:?} {:?}", &extension, &path);

        match extension.as_str() {
            "ndjson" | "jsonl" => read_df_jsonl(&path),
            "json" => read_df_json(&path),
            "csv" | "data" => {
                let dialect = sniff_csv_dialect(&path, &opts_clone)?;
                read_df_csv(&path, dialect.delimiter, dialect.quote_char)
            }
            "tsv" => {
                let dialect = sniff_csv_dialect(&path, &opts_clone)?;
                read_df_csv(&path, b'\t', dialect.quote_char)
            }
            "parquet" => read_df_parquet(&path),
            "arrow" => {
                if opts_clone.sql.is_some() {
                    return Err(OxenError::basic_str(
                        "Error: SQL queries are not supported for .arrow files",
                    ));
                }
                read_df_arrow(&path)
            }
            _ => {
                let err = format!(
                    "Could not load data frame with path: {path:?} and extension: {extension}"
                );
                Err(OxenError::basic_str(err))
            }
        }
    })
    .await
    .map_err(|e| OxenError::basic_str(format!("Task panicked: {e}")))?
}

pub async fn p_read_df_with_extension(
    path: impl AsRef<Path> + Send + 'static,
    extension: impl AsRef<str>,
    opts: DFOpts,
) -> Result<DataFrame, OxenError> {
    let df_lazy = _read_lazy_df_with_extension(path, extension.as_ref(), &opts).await?;
    collect_with_opts(df_lazy, opts).await
}

/// Apply any `DFOpts` transform to a lazy frame and collect it into a `DataFrame` off the async
/// runtime. Shared by the local and S3 read paths. The IO-bearing transform pipeline
/// (`transform_lazy`) runs on the caller's runtime; the eager slice and the final collect then
/// share one `spawn_blocking` so a cloud-backed frame's collect never drives `block_in_place` on
/// the server's current-thread runtime.
pub(crate) async fn collect_with_opts(
    df_lazy: LazyFrame,
    opts: DFOpts,
) -> Result<DataFrame, OxenError> {
    if opts.has_transform() {
        let transformed = transform_lazy(df_lazy, opts.clone()).await?;
        task::spawn_blocking(move || {
            transform_slice_lazy(transformed, &opts)?
                .collect()
                .map_err(OxenError::from)
        })
        .await?
    } else {
        task::spawn_blocking(move || df_lazy.collect().map_err(OxenError::from)).await?
    }
}

/// Head bytes fetched as the CSV/TSV dialect sniff sample. qsv_sniffer reads whole lines and stops
/// once the cumulative size passes its 16 KiB default sample, so it normally inspects ~16 KiB; the
/// extra headroom here covers wide rows. If we have trouble sniffing wide rows, we can try
/// increasing this value.
const CSV_SNIFF_SAMPLE_BYTES: u64 = 64 * 1024;

/// Read a tabular version file by hash through the `VersionStore` byte interface.
///
/// The store is immutable, content-addressed byte storage, so this never asks it for a writable
/// handle. Local backends read the version file from disk. For S3, JSON content and the CSV/TSV
/// dialect sniff sample are pulled through the store's own `get_version`/`get_version_chunk`; the
/// columnar formats (Parquet/CSV/TSV/JSONL/IPC) additionally use a cloud-aware Polars scan as a
/// read-only optimization so Polars can prune via range requests instead of reading the whole
/// object.
pub async fn read_version_df(
    version_store: &Arc<dyn VersionStore>,
    hash: &str,
    extension: impl AsRef<str>,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let extension = extension.as_ref();
    match version_store.version_location(hash).await? {
        VersionLocation::Local(path) => {
            p_read_df_with_extension(path, extension, opts.clone()).await
        }
        VersionLocation::S3 {
            url,
            region,
            endpoint_url,
            ..
        } => {
            read_s3_version_df(
                version_store,
                hash,
                &url,
                &region,
                endpoint_url,
                extension,
                opts,
            )
            .await
        }
    }
}

/// S3 branch of [`read_version_df`]. Columnar formats use a cloud-aware Polars scan; JSON and the
/// CSV/TSV sniff sample are read through the store's byte interface.
async fn read_s3_version_df(
    version_store: &Arc<dyn VersionStore>,
    hash: &str,
    url: &str,
    region: &str,
    endpoint_url: Option<String>,
    extension: &str,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    log::debug!("Reading S3 version df {url} with extension {extension}");
    let cloud_opts = {
        let region: &str = region;
        let endpoint_url = endpoint_url.as_deref();
        let mut config = vec![("aws_region", region.to_string())];
        if let Some(endpoint) = endpoint_url {
            config.push(("aws_endpoint_url", endpoint.to_string()));
            config.push(("aws_allow_http", "true".to_string()));
            config.push(("aws_virtual_hosted_style_request", "false".to_string()));
        }
        CloudOptions::from_untyped_config(url, config.iter().map(|(k, v)| (*k, v.as_str())))
    }?;
    let url = url.to_string();

    let df_lazy = match extension {
        "ndjson" | "jsonl" => {
            task::spawn_blocking(move || read_s3_jsonl(&url, cloud_opts)).await??
        }
        "json" => {
            // `JsonReader` is byte-stream only (no cloud reader), so pull the bytes through the
            // store and parse them in memory.
            let bytes = version_store.get_version(hash).await?;
            task::spawn_blocking(move || read_json_bytes(bytes)).await??
        }
        "csv" | "data" | "tsv" => {
            // `.tsv` pins the tab delimiter; only the quote character is sniffed.
            let force_delim = (extension == "tsv").then_some(b'\t');
            let dialect = resolve_s3_csv_dialect(version_store, hash, opts, force_delim).await?;
            task::spawn_blocking(move || {
                read_s3_csv(&url, cloud_opts, dialect.delimiter, dialect.quote_char)
            })
            .await??
        }
        "parquet" => task::spawn_blocking(move || read_s3_parquet(&url, cloud_opts)).await??,
        "arrow" => {
            if opts.sql.is_some() {
                return Err(OxenError::internal_error(
                    "SQL queries are not supported for .arrow files",
                ));
            }
            task::spawn_blocking(move || read_s3_ipc(&url, cloud_opts)).await??
        }
        _ => {
            return Err(OxenError::internal_error(format!(
                "Could not load data frame from S3 with unsupported extension: {extension}"
            )));
        }
    };

    collect_with_opts(df_lazy, opts.clone()).await
}

/// Resolve the CSV/TSV dialect for an S3 version file: honor explicit `DFOpts` overrides, otherwise
/// sniff a head sample fetched through the store's `get_version_chunk` (full parity with the local
/// path, which also samples only the head). `force_delim` pins the delimiter for `.tsv` while still
/// sniffing the quote character.
async fn resolve_s3_csv_dialect(
    version_store: &Arc<dyn VersionStore>,
    hash: &str,
    opts: &DFOpts,
    force_delim: Option<u8>,
) -> Result<CsvDialect, OxenError> {
    let (user_delimiter, user_quote) = parse_user_csv_opts(opts)?;
    let delimiter = force_delim.or(user_delimiter);
    if let (Some(delimiter), Some(_)) = (delimiter, user_quote) {
        return Ok(CsvDialect {
            delimiter,
            quote_char: user_quote,
        });
    }
    // Fixed-size head sample. `sniff_reader` needs Read+Seek and rewinds per pass, so it can't
    // consume the forward-only `get_version_stream` directly; a buffer (hence a byte cap) is
    // required. If huge first lines sniff wrong, raise this cap. Streaming whole lines would match
    // local exactly but reintroduces local's unbounded single-line memory cost.
    let sample = version_store
        .get_version_chunk(hash, 0, CSV_SNIFF_SAMPLE_BYTES)
        .await?;
    // Sniffing parses the sample (CPU-bound); keep it off the async runtime.
    let sniffed =
        task::spawn_blocking(move || qsv_sniffer::Sniffer::new().sniff_reader(Cursor::new(sample)))
            .await?;
    Ok(build_csv_dialect(sniffed, delimiter, user_quote))
}

fn read_s3_parquet(url: &str, cloud_opts: CloudOptions) -> Result<LazyFrame, OxenError> {
    let args = ScanArgsParquet {
        cloud_options: Some(cloud_opts),
        ..Default::default()
    };
    LazyFrame::scan_parquet(url, args).map_err(OxenError::from)
}

fn read_s3_ipc(url: &str, cloud_opts: CloudOptions) -> Result<LazyFrame, OxenError> {
    let args = ScanArgsIpc {
        cloud_options: Some(cloud_opts),
        ..Default::default()
    };
    LazyFrame::scan_ipc(url, args).map_err(OxenError::from)
}

fn read_s3_jsonl(url: &str, cloud_opts: CloudOptions) -> Result<LazyFrame, OxenError> {
    LazyJsonLineReader::new(url)
        .with_infer_schema_length(Some(NonZeroUsize::new(10000).unwrap()))
        .with_cloud_options(Some(cloud_opts))
        .finish()
        .map_err(OxenError::from)
}

fn read_s3_csv(
    url: &str,
    cloud_opts: CloudOptions,
    delimiter: u8,
    quote_char: Option<u8>,
) -> Result<LazyFrame, OxenError> {
    base_lazy_csv_reader(url, delimiter, quote_char)
        .with_cloud_options(Some(cloud_opts))
        .finish()
        .map_err(OxenError::from)
}

fn read_json_bytes(bytes: Vec<u8>) -> Result<LazyFrame, OxenError> {
    let df = JsonReader::new(Cursor::new(bytes))
        .infer_schema_len(Some(NonZeroUsize::new(10000).unwrap()))
        .finish()?;
    Ok(df.lazy())
}

/// Read a version file by hash whose extension isn't known to the caller, resolving it from the
/// committed file node. Version files are content-addressed (stored as `.../data`), so the
/// extension never comes from the stored object itself.
pub async fn maybe_read_version_df(
    repo: &LocalRepository,
    version_store: &Arc<dyn VersionStore>,
    hash: &str,
    path: impl AsRef<Path>,
    commit_id: &str,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let commit = repositories::commits::get_by_id(repo, commit_id)?
        .ok_or_else(|| OxenError::commit_id_does_not_exist(commit_id))?;
    let node = repositories::tree::get_file_by_path(repo, &commit, &path)?
        .ok_or_else(|| OxenError::entry_does_not_exist(&path))?;
    read_version_df(version_store, hash, node.extension(), opts).await
}

pub fn scan_df(
    path: impl AsRef<Path>,
    opts: &DFOpts,
    total_rows: usize,
) -> Result<LazyFrame, OxenError> {
    let input_path = path.as_ref();
    log::debug!("Scanning df {input_path:?}");
    let extension = input_path.extension().and_then(OsStr::to_str);
    scan_df_with_extension(input_path, extension, opts, total_rows)
}

pub fn scan_df_with_extension(
    path: impl AsRef<Path>,
    extension: Option<&str>,
    opts: &DFOpts,
    total_rows: usize,
) -> Result<LazyFrame, OxenError> {
    let path = path.as_ref();
    p_scan_df_with_extension(path, extension, opts, total_rows)
}

fn p_scan_df_with_extension(
    path: impl AsRef<Path>,
    extension: Option<&str>,
    opts: &DFOpts,
    total_rows: usize,
) -> Result<LazyFrame, OxenError> {
    let input_path = path.as_ref();
    log::debug!("Scanning df {input_path:?} with extension {extension:?}");
    let err = format!("Unknown file type scan_df {input_path:?} {extension:?}");
    match extension {
        Some(extension) => match extension {
            "ndjson" => scan_df_jsonl(path, total_rows),
            "jsonl" => scan_df_jsonl(path, total_rows),
            "json" => scan_df_json(path),
            "csv" | "data" => {
                let dialect = sniff_csv_dialect(&path, opts)?;
                scan_df_csv(path, dialect.delimiter, dialect.quote_char, total_rows)
            }
            "tsv" => {
                let dialect = sniff_csv_dialect(&path, opts)?;
                scan_df_csv(path, b'\t', dialect.quote_char, total_rows)
            }
            "parquet" => scan_df_parquet(path, total_rows),
            "arrow" => scan_df_arrow(path, total_rows),
            _ => Err(OxenError::basic_str(err)),
        },
        None => Err(OxenError::basic_str(err)),
    }
}

pub fn get_size(path: impl AsRef<Path>) -> Result<DataFrameSize, OxenError> {
    let input_path = path.as_ref();
    let extension = input_path.extension().and_then(OsStr::to_str);
    get_size_with_extension(input_path, extension)
}

pub fn get_size_with_extension(
    path: impl AsRef<Path>,
    extension: Option<&str>,
) -> Result<DataFrameSize, OxenError> {
    let path = path.as_ref();
    p_get_size_with_extension(path, extension)
}

fn p_get_size_with_extension(
    path: impl AsRef<Path>,
    extension: Option<&str>,
) -> Result<DataFrameSize, OxenError> {
    let input_path = path.as_ref();
    log::debug!("Getting size of df {input_path:?} with extension {extension:?}");

    // Don't need that many rows to get the width
    let num_scan_rows = constants::DEFAULT_PAGE_SIZE;
    let mut lazy_df = scan_df_with_extension(&path, extension, &DFOpts::empty(), num_scan_rows)?;
    let schema = lazy_df
        .collect_schema()
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    let width = schema.len();

    let err = format!("Unknown file type get_size {input_path:?} {extension:?}");

    match extension {
        Some(extension) => match extension {
            "csv" | "tsv" => {
                let mut opts = CountLinesOpts::empty();
                opts.remove_trailing_blank_line = true;

                // Remove one line to account for CSV/TSV headers
                let (mut height, _) = fs::count_lines(path, opts)?;
                height -= 1; // Adjusting for header

                Ok(DataFrameSize { width, height })
            }
            "data" | "jsonl" | "ndjson" => {
                let mut opts = CountLinesOpts::empty();
                opts.remove_trailing_blank_line = true;

                let (height, _) = fs::count_lines(path, opts)?;

                Ok(DataFrameSize { width, height })
            }
            "parquet" => {
                let file = File::open(input_path)?;
                let mut reader = ParquetReader::new(file);
                let height = reader.num_rows()?;
                Ok(DataFrameSize { width, height })
            }
            "arrow" => {
                let file = File::open(input_path)?;
                // arrow is fast to .finish() so we can just do it here
                let reader = IpcReader::new(file);
                let height = reader.finish().unwrap().height();
                Ok(DataFrameSize { width, height })
            }
            "json" => {
                let df = lazy_df
                    .collect()
                    .map_err(|_| OxenError::basic_str("Could not collect json df"))?;
                let height = df.height();
                Ok(DataFrameSize { width, height })
            }
            _ => Err(OxenError::basic_str(err)),
        },
        None => Err(OxenError::basic_str(err)),
    }
}

pub fn write_df_json<P: AsRef<Path>>(df: &mut DataFrame, output: P) -> Result<(), OxenError> {
    let output = output.as_ref();
    log::debug!("Writing file {output:?}");
    log::debug!("{df:?}");
    let f = std::fs::File::create(output).unwrap();
    JsonWriter::new(f)
        .with_json_format(JsonFormat::Json)
        .finish(df)
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    Ok(())
}

pub fn write_df_jsonl<P: AsRef<Path>>(df: &mut DataFrame, output: P) -> Result<(), OxenError> {
    let output = output.as_ref();
    log::debug!("Writing file {output:?}");
    let f = std::fs::File::create(output).unwrap();
    JsonWriter::new(f)
        .with_json_format(JsonFormat::JsonLines)
        .finish(df)
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    Ok(())
}

pub fn write_df_csv<P: AsRef<Path>>(
    df: &mut DataFrame,
    output: P,
    delimiter: u8,
) -> Result<(), OxenError> {
    let output = output.as_ref();
    log::debug!("Writing file {output:?}");
    let f = std::fs::File::create(output).unwrap();
    CsvWriter::new(f)
        .include_header(true)
        .with_separator(delimiter)
        .finish(df)
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    Ok(())
}

pub fn write_df_parquet<P: AsRef<Path>>(df: &mut DataFrame, output: P) -> Result<(), OxenError> {
    let output = output.as_ref();
    log::debug!("Writing file {output:?}");
    match std::fs::File::create(output) {
        Ok(f) => {
            ParquetWriter::new(f)
                .finish(df)
                .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
            Ok(())
        }
        Err(err) => {
            let error_str = format!("Could not create file {err:?}");
            Err(OxenError::basic_str(error_str))
        }
    }
}

pub fn write_df_arrow<P: AsRef<Path>>(df: &mut DataFrame, output: P) -> Result<(), OxenError> {
    let output = output.as_ref();
    log::debug!("Writing file {output:?}");
    let f = std::fs::File::create(output).unwrap();
    IpcWriter::new(f)
        .finish(df)
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    Ok(())
}

pub fn write_df(df: &mut DataFrame, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let extension = path.extension().and_then(OsStr::to_str);
    let err = format!("Unknown file type write_df {path:?} {extension:?}");

    match extension {
        Some(extension) => match extension {
            "ndjson" => write_df_jsonl(df, path),
            "jsonl" => write_df_jsonl(df, path),
            "json" => write_df_json(df, path),
            "tsv" => write_df_csv(df, path, b'\t'),
            "csv" => write_df_csv(df, path, b','),
            "parquet" => write_df_parquet(df, path),
            "arrow" => write_df_arrow(df, path),
            _ => Err(OxenError::basic_str(err)),
        },
        None => Err(OxenError::basic_str(err)),
    }
}

pub async fn copy_df(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
) -> Result<DataFrame, OxenError> {
    let mut df = read_df(input, DFOpts::empty()).await?;
    write_df_arrow(&mut df, output)?;
    Ok(df)
}

pub async fn copy_df_add_row_num(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
) -> Result<DataFrame, OxenError> {
    let df = read_df(input, DFOpts::empty()).await?;
    let mut df = df
        .lazy()
        .with_row_index("_row_num", Some(0))
        .collect()
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    write_df_arrow(&mut df, output)?;
    Ok(df)
}

pub async fn show_path(input: impl AsRef<Path>, opts: DFOpts) -> Result<DataFrame, OxenError> {
    log::debug!("Got opts {opts:?}");
    let df = read_df(input, opts.clone()).await?;
    log::debug!("Transform finished");
    if opts.column_at().is_some() {
        for val in df.get(0).unwrap() {
            match val {
                polars::prelude::AnyValue::List(vals) => {
                    for val in vals.iter() {
                        println!("{val}")
                    }
                }
                _ => {
                    println!("{val}")
                }
            }
        }
    } else if opts.should_page {
        let output = pretty_print::df_to_pager(&df, &opts)?;
        match minus::page_all(output) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error while paging: {e}");
            }
        }
    } else {
        let pretty_df = pretty_print::df_to_str(&df);
        println!("{pretty_df}");
    }
    Ok(df)
}

pub fn get_schema_with_extension(
    path: impl AsRef<Path>,
    extension: Option<&str>,
) -> Result<crate::model::Schema, OxenError> {
    let input_path = path.as_ref();
    let opts = DFOpts::empty();
    let total_rows = constants::DEFAULT_PAGE_SIZE;
    let mut df = scan_df_with_extension(input_path, extension, &opts, total_rows)?;
    let schema = df
        .collect_schema()
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    Ok(crate::model::Schema::from_polars(&schema))
}

pub fn get_schema(input: impl AsRef<Path>) -> Result<crate::model::Schema, OxenError> {
    let opts = DFOpts::empty();
    // don't need many rows to get schema
    let total_rows = constants::DEFAULT_PAGE_SIZE;
    let mut df = scan_df(input, &opts, total_rows)?;
    let schema = df
        .collect_schema()
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;

    Ok(crate::model::Schema::from_polars(&schema))
}

pub fn schema_to_string<P: AsRef<Path>>(
    input: P,
    flatten: bool,
    opts: &DFOpts,
) -> Result<String, OxenError> {
    let mut df = scan_df(input, opts, constants::DEFAULT_PAGE_SIZE)?;
    let schema = df
        .collect_schema()
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;

    if flatten {
        let result = polars_schema_to_flat_str(&schema);
        Ok(result)
    } else {
        let mut table = Table::new();
        table.set_header(vec!["column", "dtype"]);

        for field in schema.iter_fields() {
            let dtype = DataType::from_polars(field.dtype());
            let field_str = field.name().to_string();
            let dtype_str = DataType::as_str(&dtype);
            table.add_row(vec![field_str, dtype_str]);
        }

        Ok(format!("{table}"))
    }
}

pub fn polars_schema_to_flat_str(schema: &Schema) -> String {
    let mut result = String::new();
    for (i, field) in schema.iter_fields().enumerate() {
        if i != 0 {
            result = format!("{result},");
        }

        let dtype = DataType::from_polars(field.dtype());
        let field_str = field.name().to_string();
        let dtype_str = DataType::as_str(&dtype);
        result = format!("{result}{field_str}:{dtype_str}");
    }

    result
}

pub async fn show_node(
    repo: LocalRepository,
    node: &MerkleTreeNode,
    opts: DFOpts,
) -> Result<DataFrame, OxenError> {
    let file_node = node.file()?;
    log::debug!("Opening chunked reader");

    let df = if file_node.name().ends_with("parquet") {
        let chunk_reader = ChunkReader::new(repo, file_node)?;
        let parquet_reader = ParquetReader::new(chunk_reader);
        log::debug!("Reading chunked parquet");

        match parquet_reader.finish() {
            Ok(df) => {
                log::debug!("Finished reading chunked parquet");
                Ok(df)
            }
            err => Err(OxenError::basic_str(format!(
                "Could not read chunked parquet: {err:?}"
            ))),
        }?
    } else if file_node.name().ends_with("arrow") {
        let chunk_reader = ChunkReader::new(repo, file_node)?;
        let parquet_reader = IpcReader::new(chunk_reader);
        log::debug!("Reading chunked arrow");

        match parquet_reader.finish() {
            Ok(df) => {
                log::debug!("Finished reading chunked arrow");
                Ok(df)
            }
            err => Err(OxenError::basic_str(format!(
                "Could not read chunked arrow: {err:?}"
            ))),
        }?
    } else {
        let chunk_reader = ChunkReader::new(repo, file_node)?;
        let json_reader = JsonLineReader::new(chunk_reader);

        match json_reader.finish() {
            Ok(df) => {
                log::debug!("Finished reading line delimited json");
                Ok(df)
            }
            err => Err(OxenError::basic_str(format!(
                "Could not read chunked json: {err:?}"
            ))),
        }?
    };

    let df: PolarsResult<DataFrame> = if opts.has_transform() {
        let df = transform(df, opts).await?;
        let pretty_df = pretty_print::df_to_str(&df);
        println!("{pretty_df}");
        Ok(df)
    } else {
        let pretty_df = pretty_print::df_to_str(&df);
        println!("{pretty_df}");
        Ok(df)
    };

    Ok(df?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::df::{filter, tabular};
    use crate::test::{self, TEST_DATA_DIR};
    use crate::view::JsonDataFrameView;
    use crate::{error::OxenError, opts::DFOpts};
    use itertools::Itertools;
    use tokio::task;

    #[test]
    fn test_filter_single_expr() -> Result<(), OxenError> {
        let query = Some("label == dog".to_string());
        let df = df!(
            "image" => &["0000.jpg", "0001.jpg", "0002.jpg"],
            "label" => &["cat", "dog", "unknown"],
            "min_x" => &["0.0", "1.0", "2.0"],
            "max_x" => &["3.0", "4.0", "5.0"],
        )
        .unwrap();

        let filter = filter::parse(query)?.unwrap();
        let filtered_df = tabular::filter_df(df.lazy(), &filter)?.collect().unwrap();

        assert_eq!(
            r"shape: (1, 4)
┌──────────┬───────┬───────┬───────┐
│ image    ┆ label ┆ min_x ┆ max_x │
│ ---      ┆ ---   ┆ ---   ┆ ---   │
│ str      ┆ str   ┆ str   ┆ str   │
╞══════════╪═══════╪═══════╪═══════╡
│ 0001.jpg ┆ dog   ┆ 1.0   ┆ 4.0   │
└──────────┴───────┴───────┴───────┘",
            format!("{filtered_df}")
        );

        Ok(())
    }

    #[test]
    fn test_filter_multiple_or_expr() -> Result<(), OxenError> {
        let query = Some("label == dog || label == cat".to_string());
        let df = df!(
            "image" => &["0000.jpg", "0001.jpg", "0002.jpg"],
            "label" => &["cat", "dog", "unknown"],
            "min_x" => &["0.0", "1.0", "2.0"],
            "max_x" => &["3.0", "4.0", "5.0"],
        )
        .unwrap();

        let filter = filter::parse(query)?.unwrap();
        let filtered_df = tabular::filter_df(df.lazy(), &filter)?.collect().unwrap();

        println!("{filtered_df}");

        assert_eq!(
            r"shape: (2, 4)
┌──────────┬───────┬───────┬───────┐
│ image    ┆ label ┆ min_x ┆ max_x │
│ ---      ┆ ---   ┆ ---   ┆ ---   │
│ str      ┆ str   ┆ str   ┆ str   │
╞══════════╪═══════╪═══════╪═══════╡
│ 0000.jpg ┆ cat   ┆ 0.0   ┆ 3.0   │
│ 0001.jpg ┆ dog   ┆ 1.0   ┆ 4.0   │
└──────────┴───────┴───────┴───────┘",
            format!("{filtered_df}")
        );

        Ok(())
    }

    #[test]
    fn test_filter_multiple_and_expr() -> Result<(), OxenError> {
        let query = Some("label == dog && is_correct == true".to_string());
        let df = df!(
            "image" => &["0000.jpg", "0001.jpg", "0002.jpg"],
            "label" => &["dog", "dog", "unknown"],
            "min_x" => &[0.0, 1.0, 2.0],
            "max_x" => &[3.0, 4.0, 5.0],
            "is_correct" => &[true, false, false],
        )
        .unwrap();

        let filter = filter::parse(query)?.unwrap();
        let filtered_df = tabular::filter_df(df.lazy(), &filter)?.collect().unwrap();

        println!("{filtered_df}");

        assert_eq!(
            r"shape: (1, 5)
┌──────────┬───────┬───────┬───────┬────────────┐
│ image    ┆ label ┆ min_x ┆ max_x ┆ is_correct │
│ ---      ┆ ---   ┆ ---   ┆ ---   ┆ ---        │
│ str      ┆ str   ┆ f64   ┆ f64   ┆ bool       │
╞══════════╪═══════╪═══════╪═══════╪════════════╡
│ 0000.jpg ┆ dog   ┆ 0.0   ┆ 3.0   ┆ true       │
└──────────┴───────┴───────┴───────┴────────────┘",
            format!("{filtered_df}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_unique_single_field() -> Result<(), OxenError> {
        let fields = "label";
        let df = df!(
            "image" => &["0000.jpg", "0001.jpg", "0002.jpg"],
            "label" => &["dog", "dog", "unknown"],
            "min_x" => &[0.0, 1.0, 2.0],
            "max_x" => &[3.0, 4.0, 5.0],
            "is_correct" => &[true, false, false],
        )
        .unwrap();

        let mut opts = DFOpts::from_unique(fields);
        // sort for tests because it comes back random
        opts.sort_by = Some(String::from("image"));
        let filtered_df = tabular::transform(df, opts).await?;

        println!("{filtered_df}");

        assert_eq!(
            r"shape: (2, 5)
┌──────────┬─────────┬───────┬───────┬────────────┐
│ image    ┆ label   ┆ min_x ┆ max_x ┆ is_correct │
│ ---      ┆ ---     ┆ ---   ┆ ---   ┆ ---        │
│ str      ┆ str     ┆ f64   ┆ f64   ┆ bool       │
╞══════════╪═════════╪═══════╪═══════╪════════════╡
│ 0000.jpg ┆ dog     ┆ 0.0   ┆ 3.0   ┆ true       │
│ 0002.jpg ┆ unknown ┆ 2.0   ┆ 5.0   ┆ false      │
└──────────┴─────────┴───────┴───────┴────────────┘",
            format!("{filtered_df}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_unique_multi_field() -> Result<(), OxenError> {
        let fields = "image,label";
        let df = df!(
            "image" => &["0000.jpg", "0000.jpg", "0002.jpg"],
            "label" => &["dog", "dog", "dog"],
            "min_x" => &[0.0, 1.0, 2.0],
            "max_x" => &[3.0, 4.0, 5.0],
            "is_correct" => &[true, false, false],
        )
        .unwrap();

        let mut opts = DFOpts::from_unique(fields);
        // sort for tests because it comes back random
        opts.sort_by = Some(String::from("image"));
        let filtered_df = tabular::transform(df, opts).await?;

        println!("{filtered_df}");

        assert_eq!(
            r"shape: (2, 5)
┌──────────┬───────┬───────┬───────┬────────────┐
│ image    ┆ label ┆ min_x ┆ max_x ┆ is_correct │
│ ---      ┆ ---   ┆ ---   ┆ ---   ┆ ---        │
│ str      ┆ str   ┆ f64   ┆ f64   ┆ bool       │
╞══════════╪═══════╪═══════╪═══════╪════════════╡
│ 0000.jpg ┆ dog   ┆ 0.0   ┆ 3.0   ┆ true       │
│ 0002.jpg ┆ dog   ┆ 2.0   ┆ 5.0   ┆ false      │
└──────────┴───────┴───────┴───────┴────────────┘",
            format!("{filtered_df}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_unique_count_single_field() -> Result<(), OxenError> {
        let df = df!(
            "image" => &["0000.jpg", "0001.jpg", "0002.jpg", "0003.jpg"],
            "label" => &["dog", "dog", "unknown", "dog"],
        )
        .unwrap();

        let mut opts = DFOpts::empty();
        opts.unique_count = Some(String::from("label"));
        opts.sort_by = Some(String::from("label"));
        let result_df = tabular::transform(df, opts).await?;

        println!("{result_df}");

        assert_eq!(
            r"shape: (2, 2)
┌─────────┬───────┐
│ label   ┆ count │
│ ---     ┆ ---   │
│ str     ┆ u32   │
╞═════════╪═══════╡
│ dog     ┆ 3     │
│ unknown ┆ 1     │
└─────────┴───────┘",
            format!("{result_df}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_read_json() -> Result<(), OxenError> {
        let df = tabular::read_df_json(test::test_text_json())?.collect()?;

        println!("{df}");

        assert_eq!(
            r"shape: (2, 3)
┌─────┬───────────┬──────────┐
│ id  ┆ text      ┆ category │
│ --- ┆ ---       ┆ ---      │
│ i64 ┆ str       ┆ str      │
╞═════╪═══════════╪══════════╡
│ 1   ┆ I love it ┆ positive │
│ 1   ┆ I hate it ┆ negative │
└─────┴───────────┴──────────┘",
            format!("{df}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_read_jsonl() -> Result<(), OxenError> {
        let df = match task::spawn_blocking(move || -> Result<DataFrame, OxenError> {
            tabular::read_df_jsonl(test::test_text_jsonl())?
                .collect()
                .map_err(OxenError::from)
        })
        .await?
        {
            Ok(df) => df,
            Err(e) => return Err(e),
        };

        println!("{df}");

        assert_eq!(
            r"shape: (2, 3)
┌─────┬───────────┬──────────┐
│ id  ┆ text      ┆ category │
│ --- ┆ ---       ┆ ---      │
│ i64 ┆ str       ┆ str      │
╞═════╪═══════════╪══════════╡
│ 1   ┆ I love it ┆ positive │
│ 1   ┆ I hate it ┆ negative │
└─────┴───────────┴──────────┘",
            format!("{df}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sniff_empty_rows_carriage_return_csv() -> Result<(), OxenError> {
        let opts = DFOpts::empty();
        let df = tabular::read_df(test::test_csv_empty_rows_carriage_return(), opts).await?;
        assert_eq!(df.width(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_sniff_delimiter_tabs() -> Result<(), OxenError> {
        let opts = DFOpts::empty();
        let df = tabular::read_df(test::test_tabs_csv(), opts).await?;
        assert_eq!(df.width(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_sniff_emoji_csv() -> Result<(), OxenError> {
        let opts = DFOpts::empty();
        let df = tabular::read_df(test::test_emojis(), opts).await?;
        assert_eq!(df.width(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_slice_parquet_lazy() -> Result<(), OxenError> {
        let mut opts = DFOpts::empty();
        opts.slice = Some("329..333".to_string());
        let df = tabular::scan_df_parquet(test::test_1k_parquet(), 333)?;
        let df = tabular::transform_lazy(df, opts.clone()).await?;

        let mut df = match task::spawn_blocking(move || -> Result<DataFrame, OxenError> {
            tabular::transform_slice_lazy(df.lazy(), &opts)?
                .collect()
                .map_err(OxenError::from)
        })
        .await?
        {
            Ok(df) => df,
            Err(e) => return Err(e),
        };
        println!("{df:?}");

        assert_eq!(df.width(), 3);
        assert_eq!(df.height(), 4);

        let json = JsonDataFrameView::json_from_df(&mut df);
        println!("{}", json[0]);
        assert_eq!(
            Some("Advanced Encryption Standard"),
            json[0]["title"].as_str()
        );
        assert_eq!(Some("April 26"), json[1]["title"].as_str());
        assert_eq!(Some("Anisotropy"), json[2]["title"].as_str());
        assert_eq!(Some("Alpha decay"), json[3]["title"].as_str());

        Ok(())
    }

    #[tokio::test]
    async fn test_slice_parquet_full_read() -> Result<(), OxenError> {
        let mut opts = DFOpts::empty();
        opts.slice = Some("329..333".to_string());
        let mut df = tabular::read_df(test::test_1k_parquet(), opts).await?;
        println!("{df:?}");

        assert_eq!(df.width(), 3);
        assert_eq!(df.height(), 4);

        let json = JsonDataFrameView::json_from_df(&mut df);
        println!("{}", json[0]);
        assert_eq!(
            Some("Advanced Encryption Standard"),
            json[0]["title"].as_str()
        );
        assert_eq!(Some("April 26"), json[1]["title"].as_str());
        assert_eq!(Some("Anisotropy"), json[2]["title"].as_str());
        assert_eq!(Some("Alpha decay"), json[3]["title"].as_str());

        Ok(())
    }

    #[tokio::test]
    async fn test_parse_file_with_unmatched_quotes() -> Result<(), OxenError> {
        let df = tabular::read_df(test::test_spam_ham(), DFOpts::empty()).await?;
        assert_eq!(df.width(), 2);
        assert_eq!(df.height(), 100);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_csv_with_quoted_fields_default() -> Result<(), OxenError> {
        let df = tabular::read_df(
            TEST_DATA_DIR
                .join("test")
                .join("csvs")
                .join("quoted_fields.csv"),
            DFOpts::empty(),
        )
        .await?;
        assert_eq!(df.width(), 3);
        assert_eq!(df.height(), 10);

        let desc_col = df.column("description").unwrap();
        let alice_desc = desc_col.str().unwrap().get(0).unwrap();
        assert!(
            alice_desc.contains(','),
            "Alice's description should contain a comma: {alice_desc}"
        );
        Ok(())
    }

    #[test]
    fn test_sniff_csv_dialect_detects_quote_char() -> Result<(), OxenError> {
        let dialect = sniff_csv_dialect(
            TEST_DATA_DIR
                .join("test")
                .join("csvs")
                .join("quoted_fields.csv"),
            &DFOpts::empty(),
        )?;
        assert_eq!(dialect.delimiter, b',');
        assert_eq!(dialect.quote_char, Some(b'"'));
        Ok(())
    }

    #[test]
    fn test_reject_empty_quote() -> Result<(), OxenError> {
        let r = sniff_csv_dialect(
            TEST_DATA_DIR
                .join("test")
                .join("csvs")
                .join("quoted_fields.csv"),
            &{
                let mut opts = DFOpts::empty();
                opts.quote_char = Some("".to_string());
                opts
            },
        );
        assert!(matches!(r, Err(OxenError::InvalidQuoteChar)));
        Ok(())
    }

    #[tokio::test]
    async fn test_csv_video_captions_quoting() -> Result<(), OxenError> {
        let df = tabular::read_df(
            TEST_DATA_DIR
                .join("test")
                .join("csvs")
                .join("caption_video_gen_fmt.csv"),
            DFOpts::empty(),
        )
        .await?;
        assert_eq!(df.height(), 10);
        assert_eq!(df.width(), 2);
        let columns = df
            .get_column_names()
            .iter()
            .map(|c| c.to_string())
            .sorted()
            .collect::<Vec<_>>();
        assert_eq!(columns, vec!["caption", "media_path"]);
        let nl = if cfg!(windows) { "\r\n" } else { "\n" };
        let expected = format!(
            "[VISUAL]: Jade Mills, a woman in her 60s with blonde shoulder-length hair, sits in a professional podcast interview setting. She wears a dusty pink blazer over a white high-neck lace top with intricate detailing. The background features a soft teal-green gradient with large windows showing blurred outdoor scenery. A professional black microphone on a boom arm is positioned to her right. Jade Mills gazes slightly upward and to her left with a contemplative expression, her eyes looking off-camera. Her posture is upright and engaged, seated on what appears to be a dark chair or couch with teal cushions visible at the edge of the frame.{nl}{nl}[CHARACTER_SPEECH]: Jade Mills: I don't want a baby in a wife on the road with me, and he left. So..."
        );
        assert_eq!(
            df.column("caption")
                .unwrap()
                .get(0)
                .unwrap()
                .str_value()
                .to_string()
                .as_str(),
            expected
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_any_val_to_json_primitive_types() -> Result<(), OxenError> {
        use polars::prelude::AnyValue;
        use serde_json::Value;
        use serde_json::json;

        let val = AnyValue::Null;
        let json = tabular::any_val_to_json(val);
        assert_eq!(json, Value::Null);

        let val = AnyValue::Boolean(true);
        let json = tabular::any_val_to_json(val);
        assert_eq!(json, Value::Bool(true));

        let val = AnyValue::Int32(42);
        let json = tabular::any_val_to_json(val);
        assert_eq!(json, json!(42));

        let val = AnyValue::Int64(42);
        let json = tabular::any_val_to_json(val);
        assert_eq!(json, json!(42));

        let val = AnyValue::Float32(3.41);
        let json = tabular::any_val_to_json(val);
        if let serde_json::Value::Number(num) = json {
            let float_val = num.as_f64().unwrap();
            assert!(
                (float_val - 3.41).abs() < 0.0001,
                "Float32 value should be approximately 3.41"
            );
        } else {
            panic!("Expected a JSON number");
        }

        let val = AnyValue::Float64(3.41);
        let json = tabular::any_val_to_json(val);
        assert_eq!(json, json!(3.41));

        let val = AnyValue::String("hello");
        let json = tabular::any_val_to_json(val);
        assert_eq!(json, Value::String("hello".to_string()));

        let val = AnyValue::StringOwned("hello".to_string().into());
        let json = tabular::any_val_to_json(val);
        assert_eq!(json, Value::String("hello".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_any_val_to_json_list_types() -> Result<(), OxenError> {
        use polars::prelude::{AnyValue, PlSmallStr, Series};
        use serde_json::json;

        let s = Series::new(PlSmallStr::from_str("ints"), &[1, 2, 3]);
        let val = AnyValue::List(s);
        let json = tabular::any_val_to_json(val);
        assert_eq!(json, json!([1, 2, 3]));

        let s = Series::new(PlSmallStr::from_str("floats"), &[1.1, 2.2, 3.3]);
        let val = AnyValue::List(s);
        let json = tabular::any_val_to_json(val);
        assert_eq!(json, json!([1.1, 2.2, 3.3]));

        let s = Series::new(PlSmallStr::from_str("strings"), &["a", "b", "c"]);
        let val = AnyValue::List(s);
        let json = tabular::any_val_to_json(val);
        assert_eq!(json, json!(["a", "b", "c"]));

        let s = Series::new(PlSmallStr::from_str("bools"), &[true, false, true]);
        let val = AnyValue::List(s);
        let json = tabular::any_val_to_json(val);
        assert_eq!(json, json!([true, false, true]));

        Ok(())
    }

    #[test]
    fn test_struct_owned_to_json() -> Result<(), OxenError> {
        use polars::datatypes::DataType as PolarsDataType;
        use polars::prelude::{AnyValue, Field, PlSmallStr};

        let fields = vec![
            Field::new(PlSmallStr::from_str("name"), PolarsDataType::String),
            Field::new(PlSmallStr::from_str("age"), PolarsDataType::Int32),
            Field::new(PlSmallStr::from_str("active"), PolarsDataType::Boolean),
        ];

        let values = vec![
            AnyValue::String("John Doe"),
            AnyValue::Int32(30),
            AnyValue::Boolean(true),
        ];

        let struct_owned = AnyValue::StructOwned(Box::new((values, fields)));

        let json = tabular::any_val_to_json(struct_owned);

        if let serde_json::Value::Object(map) = json {
            assert_eq!(map.len(), 3);
            assert_eq!(map.get("name").unwrap(), "John Doe");
            assert_eq!(map.get("age").unwrap(), 30);
            assert_eq!(map.get("active").unwrap(), true);
        } else {
            panic!("Expected a JSON object");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_transform_randomize() -> Result<(), OxenError> {
        let mut opts = DFOpts::empty();
        opts.should_randomize = true;
        let df = tabular::read_df(test::test_1k_parquet(), opts).await?;
        // All rows should still be present, just in a different order
        let height = df.height();
        assert!(height > 0);
        assert_eq!(df.width(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_transform_rename_col() -> Result<(), OxenError> {
        let mut opts = DFOpts::empty();
        opts.rename_col = Some("title:new_title".to_string());
        let df = tabular::read_df(test::test_1k_parquet(), opts).await?;
        assert!(df.height() > 0);
        assert!(df.column("new_title").is_ok());
        assert!(df.column("title").is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_transform_add_col() -> Result<(), OxenError> {
        let mut opts = DFOpts::empty();
        opts.add_col = Some("new_col:test_val:str".to_string());
        let df = tabular::read_df(test::test_1k_parquet(), opts).await?;
        assert!(df.height() > 0);
        assert_eq!(df.width(), 4);
        assert!(df.column("new_col").is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_transform_add_row() -> Result<(), OxenError> {
        let mut opts = DFOpts::empty();
        let base_df = tabular::read_df(test::test_1k_parquet(), DFOpts::empty()).await?;
        let expected_height = base_df.height() + 1;
        opts.add_row =
            Some(r#"{"id":"999","url":"https://example.com","title":"new_title"}"#.to_string());
        let df = tabular::read_df(test::test_1k_parquet(), opts).await?;
        assert_eq!(df.height(), expected_height);
        Ok(())
    }

    #[tokio::test]
    async fn test_transform_vstack() -> Result<(), OxenError> {
        let base_df = tabular::read_df(test::test_1k_parquet(), DFOpts::empty()).await?;
        let base_height = base_df.height();

        let mut opts = DFOpts::empty();
        opts.vstack = Some(vec![test::test_1k_parquet()]);
        let df = tabular::read_df(test::test_1k_parquet(), opts).await?;
        assert_eq!(df.height(), base_height * 2);
        assert_eq!(df.width(), base_df.width());
        Ok(())
    }
}
