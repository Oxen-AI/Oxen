use duckdb::ToSql;
use polars::prelude::*;
use serde_json::json;
use std::fs::File;
use std::num::NonZeroUsize;

use crate::constants;
use crate::core::df::filter::DFLogicalOp;
use crate::core::df::pretty_print;
use crate::core::df::sql;
use crate::error::OxenError;
use crate::io::chunk_reader::ChunkReader;
use crate::model::data_frame::schema::DataType;
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::Commit;
use crate::model::DataFrameSize;
use crate::model::LocalRepository;
use crate::opts::{CountLinesOpts, DFOpts, PaginateOpts};
use crate::repositories;
use crate::util::fs;
use crate::util::hasher;

use comfy_table::Table;
use indicatif::ProgressBar;
use serde_json::Value;
use std::ffi::OsStr;
use std::io::Cursor;
use std::path::Path;

use super::filter::{DFFilterExp, DFFilterOp, DFFilterVal};

const READ_ERROR: &str = "Could not read tabular data from path";

fn base_lazy_csv_reader(path: impl AsRef<Path>, delimiter: u8) -> LazyCsvReader {
    let path = path.as_ref();
    let reader = LazyCsvReader::new(path);
    reader
        .with_infer_schema_length(Some(10000))
        .with_ignore_errors(true)
        .with_has_header(true)
        .with_truncate_ragged_lines(true)
        .with_separator(delimiter)
        .with_eol_char(b'\n')
        .with_quote_char(Some(b'"'))
        .with_rechunk(true)
        .with_encoding(CsvEncoding::LossyUtf8)
}

pub fn read_df_csv(path: impl AsRef<Path>, delimiter: u8) -> Result<LazyFrame, OxenError> {
    let reader = base_lazy_csv_reader(path.as_ref(), delimiter);
    reader
        .finish()
        .map_err(|_| OxenError::basic_str(format!("{}: {:?}", READ_ERROR, path.as_ref())))
}

pub fn read_df_jsonl(path: impl AsRef<Path>) -> Result<LazyFrame, OxenError> {
    let path = path
        .as_ref()
        .to_str()
        .ok_or(OxenError::basic_str("Could not convert path to string"))?;
    LazyJsonLineReader::new(path)
        .with_infer_schema_length(Some(NonZeroUsize::new(10000).unwrap()))
        .finish()
        .map_err(|_| OxenError::basic_str(format!("{}: {:?}", READ_ERROR, path)))
}

pub fn scan_df_json(path: impl AsRef<Path>) -> Result<LazyFrame, OxenError> {
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
    // );
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
    total_rows: usize,
) -> Result<LazyFrame, OxenError> {
    let reader = base_lazy_csv_reader(path.as_ref(), delimiter);
    reader
        .with_n_rows(Some(total_rows))
        .finish()
        .map_err(|_| OxenError::basic_str(format!("{}: {:?}", READ_ERROR, path.as_ref())))
}

pub fn scan_df_jsonl(path: impl AsRef<Path>, total_rows: usize) -> Result<LazyFrame, OxenError> {
    let path = path
        .as_ref()
        .to_str()
        .ok_or(OxenError::basic_str("Could not convert path to string"))?;
    LazyJsonLineReader::new(path)
        .with_infer_schema_length(Some(NonZeroUsize::new(10000).unwrap()))
        .with_n_rows(Some(total_rows))
        .finish()
        .map_err(|_| OxenError::basic_str(format!("{}: {:?}", READ_ERROR, path)))
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

fn scan_df_arrow(path: impl AsRef<Path>, total_rows: usize) -> Result<LazyFrame, OxenError> {
    let args = ScanArgsIpc {
        n_rows: Some(total_rows),
        ..Default::default()
    };

    LazyFrame::scan_ipc(&path, args)
        .map_err(|_| OxenError::basic_str(format!("{}: {:?}", READ_ERROR, path.as_ref())))
}

pub fn add_col_lazy(
    df: LazyFrame,
    name: &str,
    val: &str,
    dtype: &str,
    at: Option<usize>,
) -> Result<LazyFrame, OxenError> {
    let mut df = df
        .collect()
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;

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

pub fn add_row(df: LazyFrame, data: String) -> Result<LazyFrame, OxenError> {
    let df = df
        .collect()
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    let new_row = row_from_str_and_schema(data, df.schema())?;
    log::debug!("add_row og df: {:?}", df);
    log::debug!("add_row new_row: {:?}", new_row);
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
    schema: Schema,
) -> Result<DataFrame, OxenError> {
    if serde_json::from_str::<Value>(data.as_ref()).is_ok() {
        return parse_str_to_df(data);
    }

    let values: Vec<&str> = data.as_ref().split(',').collect();

    if values.len() != schema.len() {
        return Err(OxenError::basic_str(format!(
            "Error: Added row must have same number of columns as df\nRow columns: {}\ndf columns: {}", values.len(), schema.len())
        ));
    }

    let mut vec: Vec<Column> = Vec::new();

    for ((name, dtype), value) in schema.iter_names_and_dtypes().zip(values.into_iter()) {
        let typed_val = val_from_str_and_dtype(value, dtype);
        match Series::from_any_values_and_dtype(name.clone(), &[typed_val], dtype, false) {
            Ok(series) => {
                vec.push(Column::Series(series));
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
    match JsonLineReader::new(cursor).finish() {
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
        _ => panic!("Currently do not support data type {}", dtype),
    }
}

fn val_from_df_and_filter<'a>(df: &mut LazyFrame, filter: &'a DFFilterVal) -> AnyValue<'a> {
    if let Some(value) = df
        .collect_schema()
        .expect("Unable to get schema from data frame")
        .iter_fields()
        .find(|f| f.name == filter.field)
    {
        val_from_str_and_dtype(&filter.value, value.dtype())
    } else {
        log::error!("Unknown field {:?}", filter.field);
        AnyValue::Null
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
        val => panic!("Unknown data type for [{}] to create literal", val),
    }
}

fn filter_from_val(df: &mut LazyFrame, filter: &DFFilterVal) -> Expr {
    let val = val_from_df_and_filter(df, filter);
    let val = lit_from_any(&val);
    match filter.op {
        DFFilterOp::EQ => col(&filter.field).eq(val),
        DFFilterOp::GT => col(&filter.field).gt(val),
        DFFilterOp::LT => col(&filter.field).lt(val),
        DFFilterOp::GTE => col(&filter.field).gt_eq(val),
        DFFilterOp::LTE => col(&filter.field).lt_eq(val),
        DFFilterOp::NEQ => col(&filter.field).neq(val),
    }
}

fn filter_df(mut df: LazyFrame, filter: &DFFilterExp) -> Result<LazyFrame, OxenError> {
    log::debug!("Got filter: {:?}", filter);
    if filter.vals.is_empty() {
        return Ok(df);
    }
    let mut vals = filter.vals.iter();
    let mut expr: Expr = filter_from_val(&mut df, vals.next().unwrap());
    for op in &filter.logical_ops {
        let chain_expr: Expr = filter_from_val(&mut df, vals.next().unwrap());

        match op {
            DFLogicalOp::AND => expr = expr.and(chain_expr),
            DFLogicalOp::OR => expr = expr.or(chain_expr),
        }
    }

    Ok(df.filter(expr))
}

fn unique_df(df: LazyFrame, columns: Vec<String>) -> Result<LazyFrame, OxenError> {
    log::debug!("Got unique: {:?}", columns);
    Ok(df.unique(Some(columns), UniqueKeepStrategy::First))
}

pub fn transform(df: DataFrame, opts: DFOpts) -> Result<DataFrame, OxenError> {
    let df = transform_lazy(df.lazy(), opts.clone())?;
    Ok(transform_slice_lazy(df, opts)?.collect()?)
}

pub fn transform_new(df: LazyFrame, opts: DFOpts) -> Result<LazyFrame, OxenError> {
    //    let height = df.height();
    let df = transform_lazy(df, opts.clone())?;
    transform_slice_lazy(df, opts)
}

pub fn transform_lazy(mut df: LazyFrame, opts: DFOpts) -> Result<LazyFrame, OxenError> {
    log::debug!("transform_lazy Got transform ops {:?}", opts);
    if let Some(vstack) = &opts.vstack {
        log::debug!("transform_lazy Got files to stack {:?}", vstack);
        for path in vstack.iter() {
            let opts = DFOpts::empty();
            let new_df = read_df(path, opts).map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
            df = df
                .collect()
                .map_err(|e| OxenError::basic_str(format!("{e:?}")))?
                .vstack(&new_df)
                .map_err(|e| OxenError::basic_str(format!("{e:?}")))?
                .lazy();
        }
    }

    if let Some(col_vals) = opts.add_col_vals() {
        df = add_col_lazy(
            df,
            &col_vals.name,
            &col_vals.value,
            &col_vals.dtype,
            opts.at,
        )?;
    }

    if let Some(data) = &opts.add_row {
        df = add_row(df, data.to_owned())?;
    }

    match opts.get_filter() {
        Ok(filter) => {
            if let Some(filter) = filter {
                df = filter_df(df, &filter)?;
            }
        }
        Err(err) => {
            log::error!("Could not parse filter: {err}");
        }
    }

    if let Some(sql) = opts.sql.clone() {
        if let Some(repo_dir) = opts.repo_dir.as_ref() {
            let repo = LocalRepository::from_dir(repo_dir)?;
            df = sql::query_df_from_repo(sql, &repo)?.lazy();
        }
    }

    if let Some(columns) = opts.unique_columns() {
        df = unique_df(df, columns)?;
    }

    if let Some(sort_by) = &opts.sort_by {
        df = df.sort([sort_by], Default::default());
    }

    if opts.should_reverse {
        df = df.reverse();
    }

    if let Some(columns) = opts.columns_names() {
        if !columns.is_empty() {
            let cols = columns.iter().map(col).collect::<Vec<Expr>>();
            df = df.select(&cols);
        }
    }

    // These ops should be the last ops since they depends on order
    if let Some(indices) = opts.take_indices() {
        match take(df.clone(), indices) {
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
pub fn transform_slice_lazy(mut df: LazyFrame, opts: DFOpts) -> Result<LazyFrame, OxenError> {
    // Maybe slice it up
    df = slice(df, &opts);
    df = head(df, &opts);
    df = tail(df, &opts);

    if let Some(item) = opts.column_at() {
        let full_df = df.collect().unwrap();
        let value = full_df.column(&item.col).unwrap().get(item.index).unwrap();
        let s1 = Column::Series(Series::new(PlSmallStr::from_str(""), &[value]));
        let df = DataFrame::new(vec![s1]).unwrap();
        return Ok(df.lazy());
    }

    log::debug!("transform_slice_lazy before collect");
    Ok(df)
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
    opts.slice = Some(format!("{}..{}", start, end));
    log::debug!("slice_df with opts: {:?}", opts);
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
        log::debug!("SLICE with indices {:?}..{:?}", start, end);
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
        AnyValue::List(l) => {
            let json_array = match l.dtype() {
                polars::prelude::DataType::Int64 => {
                    let vec: Vec<i64> = l.i64().unwrap().into_iter().flatten().collect();
                    json!(vec)
                }
                polars::prelude::DataType::Int32 => {
                    let vec: Vec<i32> = l.i32().unwrap().into_iter().flatten().collect();
                    json!(vec)
                }
                polars::prelude::DataType::Float64 => {
                    let vec: Vec<f64> = l.f64().unwrap().into_iter().flatten().collect();
                    json!(vec)
                }
                polars::prelude::DataType::Float32 => {
                    let vec: Vec<f32> = l.f32().unwrap().into_iter().flatten().collect();
                    json!(vec)
                }
                polars::prelude::DataType::String => {
                    let vec: Vec<String> = l
                        .str()
                        .unwrap()
                        .into_iter()
                        .flatten()
                        .map(|s| s.to_string())
                        .collect();
                    json!(vec)
                }
                polars::prelude::DataType::Boolean => {
                    let vec: Vec<bool> = l.bool().unwrap().into_iter().flatten().collect();
                    json!(vec)
                }
                dtype => {
                    panic!("Unsupported dtype: {:?}", dtype)
                }
            };
            Box::new(json_array.to_string())
        }
        other => panic!("Unsupported dtype: {:?}", other),
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

                        Ok(Some(Column::Series(Series::new(
                            PlSmallStr::from_str(""),
                            hashes,
                        ))))
                    },
                    GetOutput::from_type(polars::prelude::DataType::String),
                )
                .alias(constants::ROW_HASH_COL_NAME),
        ])
        .collect()
        .unwrap();
    log::debug!("Hashed rows: {}", df);
    Ok(df)
}

// Maybe pass in fields here?
pub fn df_hash_rows_on_cols(
    df: DataFrame,
    hash_fields: &[String],
    out_col_name: &str,
) -> Result<DataFrame, OxenError> {
    let num_rows = df.height() as i64;

    log::debug!("df_hash_rows_on_cols df is {:?}", df);
    log::debug!("df_hash_rows_on_cols hash_fields is {:?}", hash_fields);
    log::debug!("df_hash_rows_on_cols out_col_name is {:?}", out_col_name);

    // Create a vector to store columns to be hashed
    let mut col_names = vec![];
    let schema = df.schema();
    for field in schema.iter_fields() {
        let field_name = field.name().to_string();
        if hash_fields.contains(&field_name) {
            col_names.push(col(field.name().clone()));
        }
    }

    // This is to allow asymmetric target hashing for added / removed cols in default behavior
    if col_names.is_empty() {
        let null_string_col = lit(Null {}).alias(out_col_name);
        return Ok(df.lazy().with_column(null_string_col).collect()?);
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
                            log::debug!("row: {:?}", i);
                            let mut buffer: Vec<u8> = vec![];
                            for series in s_a.iter() {
                                let elem = series.get(i).unwrap();
                                log::debug!("\telem: {:?}", elem);
                                let mut elem_bytes = any_val_to_bytes(&elem);
                                buffer.append(&mut elem_bytes);
                            }
                            pb.inc(1);
                            let result = hasher::hash_buffer(&buffer);
                            hashes.push(result);
                        }
                        pb.finish_and_clear();

                        Ok(Some(Column::Series(Series::new(
                            PlSmallStr::from_str(""),
                            hashes,
                        ))))
                    },
                    GetOutput::from_type(polars::prelude::DataType::String),
                )
                .alias(out_col_name),
        ])
        .collect()
        .unwrap();
    log::debug!("Hashed rows: {}", df);
    Ok(df)
}

fn sniff_db_csv_delimiter(path: impl AsRef<Path>, opts: &DFOpts) -> Result<u8, OxenError> {
    if let Some(delimiter) = &opts.delimiter {
        if delimiter.len() != 1 {
            return Err(OxenError::basic_str("Delimiter must be a single character"));
        }
        return Ok(delimiter.as_bytes()[0]);
    }

    match qsv_sniffer::Sniffer::new().sniff_path(&path) {
        Ok(metadata) => Ok(metadata.dialect.delimiter),
        Err(err) => {
            let err = format!("Error sniffing csv {:?} -> {:?}", path.as_ref(), err);
            log::warn!("{}", err);
            Ok(b',')
        }
    }
}

pub fn read_df(path: impl AsRef<Path>, opts: DFOpts) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    if !path.exists() {
        return Err(OxenError::entry_does_not_exist(path));
    }

    let extension = path.extension().and_then(OsStr::to_str);
    let err = format!("Could not load data frame with path: {path:?} and extension: {extension:?}");

    if let Some(extension) = extension {
        read_df_with_extension(path, extension, &opts)
    } else {
        Err(OxenError::basic_str(err))
    }
}

pub fn read_df_with_extension(
    path: impl AsRef<Path>,
    extension: impl AsRef<str>,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let extension = extension.as_ref();
    if !path.exists() {
        return Err(OxenError::entry_does_not_exist(path));
    }

    let df = match extension {
        "ndjson" => read_df_jsonl(path),
        "jsonl" => read_df_jsonl(path),
        "json" => read_df_json(path),
        "csv" | "data" => {
            let delimiter = sniff_db_csv_delimiter(path, opts)?;
            read_df_csv(path, delimiter)
        }
        "tsv" => read_df_csv(path, b'\t'),
        "parquet" => read_df_parquet(path),
        "arrow" => {
            if opts.sql.is_some() {
                return Err(OxenError::basic_str(
                    "Error: SQL queries are not supported for .arrow files",
                ));
            }
            read_df_arrow(path)
        }
        _ => {
            let err = format!(
                "Could not load data frame with path: {path:?} and extension: {extension:?}"
            );
            Err(OxenError::basic_str(err))
        }
    }?;

    // log::debug!("Read finished");
    if opts.has_transform() {
        let df = transform_new(df, opts.clone())?;
        Ok(df.collect()?)
    } else {
        Ok(df.collect()?)
    }
}

pub fn maybe_read_df_with_extension(
    repo: &LocalRepository,
    version_path: impl AsRef<Path>,
    path: impl AsRef<Path>,
    commit_id: &str,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let version_path = version_path.as_ref();
    if !version_path.exists() {
        return Err(OxenError::entry_does_not_exist(path));
    }

    let extension = version_path.extension().and_then(OsStr::to_str);

    if let Some(extension) = extension {
        read_df_with_extension(path, extension, opts)
    } else {
        let commit = repositories::commits::get_by_id(repo, commit_id)?;
        if let Some(commit) = commit {
            try_to_read_extension_from_node(repo, version_path, path, &commit, opts)
        } else {
            let err = format!("Could not find commit: {commit_id}");
            Err(OxenError::basic_str(err))
        }
    }
}

fn try_to_read_extension_from_node(
    repo: &LocalRepository,
    version_path: impl AsRef<Path>,
    path: impl AsRef<Path>,
    commit: &Commit,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let node = repositories::tree::get_file_by_path(repo, commit, &path)?;
    if let Some(file_node) = node {
        read_df_with_extension(&version_path, file_node.extension, opts)
    } else {
        let err = format!("Could not find file node {:?}", path.as_ref());
        Err(OxenError::basic_str(err))
    }
}

pub fn scan_df(
    path: impl AsRef<Path>,
    opts: &DFOpts,
    total_rows: usize,
) -> Result<LazyFrame, OxenError> {
    log::debug!("Scanning df {:?}", path.as_ref());
    let input_path = path.as_ref();
    let extension = input_path.extension().and_then(OsStr::to_str);
    let err = format!("Unknown file type scan_df {input_path:?} {extension:?}");

    match extension {
        Some(extension) => match extension {
            "ndjson" => scan_df_jsonl(path, total_rows),
            "jsonl" => scan_df_jsonl(path, total_rows),
            "json" => scan_df_json(path),
            "csv" | "data" => {
                let delimiter = sniff_db_csv_delimiter(&path, opts)?;
                scan_df_csv(path, delimiter, total_rows)
            }
            "tsv" => scan_df_csv(path, b'\t', total_rows),
            "parquet" => scan_df_parquet(path, total_rows),
            "arrow" => scan_df_arrow(path, total_rows),
            _ => Err(OxenError::basic_str(err)),
        },
        None => Err(OxenError::basic_str(err)),
    }
}

pub fn get_size(path: impl AsRef<Path>) -> Result<DataFrameSize, OxenError> {
    // Don't need that many rows to get the width
    let num_scan_rows = constants::DEFAULT_PAGE_SIZE;
    let mut lazy_df = scan_df(&path, &DFOpts::empty(), num_scan_rows)?;
    let schema = lazy_df
        .collect_schema()
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    let width = schema.len();

    let input_path = path.as_ref();
    let extension = input_path.extension().and_then(OsStr::to_str);
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
    log::debug!("Writing file {:?}", output);
    log::debug!("{:?}", df);
    let f = std::fs::File::create(output).unwrap();
    JsonWriter::new(f)
        .with_json_format(JsonFormat::Json)
        .finish(df)
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    Ok(())
}

pub fn write_df_jsonl<P: AsRef<Path>>(df: &mut DataFrame, output: P) -> Result<(), OxenError> {
    let output = output.as_ref();
    log::debug!("Writing file {:?}", output);
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
    log::debug!("Writing file {:?}", output);
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
    log::debug!("Writing file {:?}", output);
    match std::fs::File::create(output) {
        Ok(f) => {
            ParquetWriter::new(f)
                .finish(df)
                .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
            Ok(())
        }
        Err(err) => {
            let error_str = format!("Could not create file {:?}", err);
            Err(OxenError::basic_str(error_str))
        }
    }
}

pub fn write_df_arrow<P: AsRef<Path>>(df: &mut DataFrame, output: P) -> Result<(), OxenError> {
    let output = output.as_ref();
    log::debug!("Writing file {:?}", output);
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

pub fn copy_df(input: impl AsRef<Path>, output: impl AsRef<Path>) -> Result<DataFrame, OxenError> {
    let mut df = read_df(input, DFOpts::empty())?;
    write_df_arrow(&mut df, output)?;
    Ok(df)
}

pub fn copy_df_add_row_num(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
) -> Result<DataFrame, OxenError> {
    let df = read_df(input, DFOpts::empty())?;
    let mut df = df
        .lazy()
        .with_row_index("_row_num", Some(0))
        .collect()
        .map_err(|e| OxenError::basic_str(format!("{e:?}")))?;
    write_df_arrow(&mut df, output)?;
    Ok(df)
}

pub fn show_path(input: impl AsRef<Path>, opts: DFOpts) -> Result<DataFrame, OxenError> {
    log::debug!("Got opts {:?}", opts);
    let df = read_df(input, opts.clone())?;
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
                eprintln!("Error while paging: {}", e);
            }
        }
    } else {
        let pretty_df = pretty_print::df_to_str(&df);
        println!("{pretty_df}");
    }
    Ok(df)
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
            let dtype_str = String::from(DataType::as_str(&dtype));
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
        let dtype_str = String::from(DataType::as_str(&dtype));
        result = format!("{result}{field_str}:{dtype_str}");
    }

    result
}

pub fn show_node(
    repo: LocalRepository,
    node: &MerkleTreeNode,
    opts: DFOpts,
) -> Result<DataFrame, OxenError> {
    let file_node = node.file()?;
    log::debug!("Opening chunked reader");

    let df = if file_node.name.ends_with("parquet") {
        let chunk_reader = ChunkReader::new(repo, file_node)?;
        let parquet_reader = ParquetReader::new(chunk_reader);
        log::debug!("Reading chunked parquet");

        match parquet_reader.finish() {
            Ok(df) => {
                log::debug!("Finished reading chunked parquet");
                Ok(df)
            }
            err => Err(OxenError::basic_str(format!(
                "Could not read chunked parquet: {:?}",
                err
            ))),
        }?
    } else if file_node.name.ends_with("arrow") {
        let chunk_reader = ChunkReader::new(repo, file_node)?;
        let parquet_reader = IpcReader::new(chunk_reader);
        log::debug!("Reading chunked arrow");

        match parquet_reader.finish() {
            Ok(df) => {
                log::debug!("Finished reading chunked arrow");
                Ok(df)
            }
            err => Err(OxenError::basic_str(format!(
                "Could not read chunked arrow: {:?}",
                err
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
                "Could not read chunked json: {:?}",
                err
            ))),
        }?
    };

    let df: PolarsResult<DataFrame> = if opts.has_transform() {
        let df = transform(df, opts)?;
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
    use crate::core::df::{filter, tabular};
    use crate::view::JsonDataFrameView;
    use crate::{error::OxenError, opts::DFOpts};
    use polars::prelude::*;

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

    #[test]
    fn test_unique_single_field() -> Result<(), OxenError> {
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
        let filtered_df = tabular::transform(df, opts)?;

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

    #[test]
    fn test_unique_multi_field() -> Result<(), OxenError> {
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
        let filtered_df = tabular::transform(df, opts)?;

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

    #[test]
    fn test_read_json() -> Result<(), OxenError> {
        let df = tabular::read_df_json("data/test/text/test.json")?.collect()?;

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

    #[test]
    fn test_read_jsonl() -> Result<(), OxenError> {
        let df = tabular::read_df_jsonl("data/test/text/test.jsonl")?.collect()?;

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

    #[test]
    fn test_sniff_empty_rows_carriage_return_csv() -> Result<(), OxenError> {
        let opts = DFOpts::empty();
        let df = tabular::read_df("data/test/csvs/empty_rows_carriage_return.csv", opts)?;
        assert_eq!(df.width(), 4);
        Ok(())
    }

    #[test]
    fn test_sniff_delimiter_tabs() -> Result<(), OxenError> {
        let opts = DFOpts::empty();
        let df = tabular::read_df("data/test/csvs/tabs.csv", opts)?;
        assert_eq!(df.width(), 4);
        Ok(())
    }

    #[test]
    fn test_sniff_emoji_csv() -> Result<(), OxenError> {
        let opts = DFOpts::empty();
        let df = tabular::read_df("data/test/csvs/emojis.csv", opts)?;
        assert_eq!(df.width(), 2);
        Ok(())
    }

    #[test]
    fn test_slice_parquet_lazy() -> Result<(), OxenError> {
        let mut opts = DFOpts::empty();
        opts.slice = Some("329..333".to_string());
        let df = tabular::scan_df_parquet("data/test/parquet/wiki_1k.parquet", 333)?;
        let df = tabular::transform_lazy(df, opts.clone())?;
        let mut df = tabular::transform_slice_lazy(df.lazy(), opts)?.collect()?;
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

    #[test]
    fn test_slice_parquet_full_read() -> Result<(), OxenError> {
        let mut opts = DFOpts::empty();
        opts.slice = Some("329..333".to_string());
        let mut df = tabular::read_df("data/test/parquet/wiki_1k.parquet", opts)?;
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
}
