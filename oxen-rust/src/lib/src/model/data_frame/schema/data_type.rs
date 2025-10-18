//! Core Oxen data types to convert between Polars and DuckDB DataFrames and Schemas
//!

use crate::model::data_frame::schema::Field;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Boolean,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Binary,
    Date,
    Time,
    Datetime,
    Embedding(usize),
    List(Box<DataType>),
    Struct(Box<Vec<Field>>),
    Duration,
    Null,
    Unknown,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl DataType {
    pub fn from_string(s: impl AsRef<str>) -> DataType {
        match s.as_ref() {
            "bool" => DataType::Boolean,
            "uint8" => DataType::UInt8,
            "u16" => DataType::UInt16,
            "u32" => DataType::UInt32,
            "u64" => DataType::UInt64,
            "i8" => DataType::Int8,
            "i16" => DataType::Int16,
            "int" => DataType::Int32,
            "i32" => DataType::Int32,
            "i64" => DataType::Int64,
            "float" => DataType::Float32,
            "f32" => DataType::Float32,
            "double" => DataType::Float64,
            "f64" => DataType::Float64,
            "str" => DataType::String,
            "binary" => DataType::Binary,
            "date" => DataType::Date,
            "datetime" => DataType::Datetime,
            "time" => DataType::Time,
            "duration" => DataType::Duration,
            "struct" => DataType::Struct(Box::default()),
            "null" => DataType::Null,
            "list[bool]" => DataType::List(Box::new(DataType::Boolean)),
            "list[uint8]" => DataType::List(Box::new(DataType::UInt8)),
            "list[u16]" => DataType::List(Box::new(DataType::UInt16)),
            "list[u32]" => DataType::List(Box::new(DataType::UInt32)),
            "list[u64]" => DataType::List(Box::new(DataType::UInt64)),
            "list[i8]" => DataType::List(Box::new(DataType::Int8)),
            "list[i16]" => DataType::List(Box::new(DataType::Int16)),
            "list[i32]" => DataType::List(Box::new(DataType::Int32)),
            "list[i64]" => DataType::List(Box::new(DataType::Int64)),
            "list[f32]" => DataType::List(Box::new(DataType::Float32)),
            "list[f64]" => DataType::List(Box::new(DataType::Float64)),
            "list[str]" => DataType::List(Box::new(DataType::String)),
            "list[binary]" => DataType::List(Box::new(DataType::Binary)),
            "list[date]" => DataType::List(Box::new(DataType::Date)),
            "list[time]" => DataType::List(Box::new(DataType::Time)),
            "list[?]" => DataType::List(Box::new(DataType::Unknown)),
            type_name => {
                // handle embedding[N] types
                if let Some(captures) = regex::Regex::new(r"^embedding\[(\d+)\]$")
                    .unwrap()
                    .captures(type_name)
                {
                    if let Some(size) = captures.get(1) {
                        return DataType::Embedding(size.as_str().parse::<usize>().unwrap());
                    }
                }
                log::error!("TODO: from_string unknown type {type_name}");
                DataType::Unknown
            }
        }
    }

    pub fn as_str(&self) -> String {
        match self {
            DataType::Boolean => "bool".to_string(),
            DataType::UInt8 => "uint8".to_string(),
            DataType::UInt16 => "u16".to_string(),
            DataType::UInt32 => "u32".to_string(),
            DataType::UInt64 => "u64".to_string(),
            DataType::Int8 => "i8".to_string(),
            DataType::Int16 => "i16".to_string(),
            DataType::Int32 => "i32".to_string(),
            DataType::Int64 => "i64".to_string(),
            DataType::Float32 => "f32".to_string(),
            DataType::Float64 => "f64".to_string(),
            DataType::String => "str".to_string(),
            DataType::Binary => "binary".to_string(),
            DataType::Date => "date".to_string(),
            DataType::Datetime => "datetime".to_string(),
            DataType::Time => "time".to_string(),
            DataType::Duration => "duration".to_string(),
            DataType::Struct(_) => "struct".to_string(),
            DataType::List(val) => match **val {
                DataType::Boolean => "list[bool]".to_string(),
                DataType::UInt8 => "list[uint8]".to_string(),
                DataType::UInt16 => "list[u16]".to_string(),
                DataType::UInt32 => "list[u32]".to_string(),
                DataType::UInt64 => "list[u64]".to_string(),
                DataType::Int8 => "list[i8]".to_string(),
                DataType::Int16 => "list[i16]".to_string(),
                DataType::Int32 => "list[i32]".to_string(),
                DataType::Int64 => "list[i64]".to_string(),
                DataType::Float32 => "list[f32]".to_string(),
                DataType::Float64 => "list[f64]".to_string(),
                DataType::String => "list[str]".to_string(),
                DataType::Date => "list[date]".to_string(),
                DataType::Time => "list[time]".to_string(),
                DataType::Binary => "list[binary]".to_string(),
                _ => "list[?]".to_string(),
            },
            DataType::Embedding(size) => format!("embedding[{size}]").to_string(),
            DataType::Null => "null".to_string(),
            DataType::Unknown => {
                // DO NOT USE {} HERE, IT WILL CAUSE A STACK OVERFLOW
                log::error!("TODO: as_str unknown DataType::Unknown type {self:?}");
                "?".to_string()
            }
        }
    }

    pub fn to_polars(&self) -> polars::prelude::DataType {
        match self {
            DataType::Boolean => polars::prelude::DataType::Boolean,
            DataType::UInt8 => polars::prelude::DataType::UInt8,
            DataType::UInt16 => polars::prelude::DataType::UInt16,
            DataType::UInt32 => polars::prelude::DataType::UInt32,
            DataType::UInt64 => polars::prelude::DataType::UInt64,
            DataType::Int8 => polars::prelude::DataType::Int8,
            DataType::Int16 => polars::prelude::DataType::Int16,
            DataType::Int32 => polars::prelude::DataType::Int32,
            DataType::Int64 => polars::prelude::DataType::Int64,
            DataType::Float32 => polars::prelude::DataType::Float32,
            DataType::Float64 => polars::prelude::DataType::Float64,
            DataType::String => polars::prelude::DataType::String,
            DataType::Binary => polars::prelude::DataType::Binary,
            DataType::Date => polars::prelude::DataType::Date,
            DataType::Time => polars::prelude::DataType::Time,
            DataType::Embedding(_) => {
                polars::prelude::DataType::List(Box::new(polars::prelude::DataType::Float32))
            }
            DataType::Duration => {
                polars::prelude::DataType::Duration(polars::prelude::TimeUnit::Milliseconds)
            }
            DataType::Struct(vals) => {
                polars::prelude::DataType::Struct(vals.iter().map(|f| f.to_polars()).collect())
            }
            DataType::Datetime => {
                polars::prelude::DataType::Datetime(polars::prelude::TimeUnit::Milliseconds, None)
            }
            DataType::List(val) => polars::prelude::DataType::List(Box::new(val.to_polars())),
            DataType::Null => polars::prelude::DataType::Null,
            DataType::Unknown => {
                polars::prelude::DataType::Unknown(polars::prelude::UnknownKind::Any)
            }
        }
    }

    pub fn from_polars(dtype: &polars::prelude::DataType) -> Self {
        match dtype {
            polars::prelude::DataType::Boolean => DataType::Boolean,
            polars::prelude::DataType::UInt8 => DataType::UInt8,
            polars::prelude::DataType::UInt16 => DataType::UInt16,
            polars::prelude::DataType::UInt32 => DataType::UInt32,
            polars::prelude::DataType::UInt64 => DataType::UInt64,
            polars::prelude::DataType::Int8 => DataType::Int8,
            polars::prelude::DataType::Int16 => DataType::Int16,
            polars::prelude::DataType::Int32 => DataType::Int32,
            polars::prelude::DataType::Int64 => DataType::Int64,
            polars::prelude::DataType::Float32 => DataType::Float32,
            polars::prelude::DataType::Float64 => DataType::Float64,
            polars::prelude::DataType::String => DataType::String,
            polars::prelude::DataType::Binary => DataType::Binary,
            polars::prelude::DataType::Datetime(_, _) => DataType::Datetime,
            polars::prelude::DataType::Time => DataType::Time,
            polars::prelude::DataType::Duration(_) => DataType::Duration,
            polars::prelude::DataType::List(inner) => {
                log::debug!("Converting List type with inner: {inner:?}");
                DataType::List(Box::new(Self::from_polars(inner)))
            }
            polars::prelude::DataType::Unknown(_) => DataType::Unknown,
            polars::prelude::DataType::Null => DataType::Null,
            _ => DataType::Unknown,
        }
    }

    pub fn to_sql(&self) -> String {
        match self {
            DataType::Boolean => "BOOL".to_string(),
            DataType::UInt8 => "UTINYINT".to_string(), // unsigned one-byte integer
            DataType::UInt16 => "USMALLINT".to_string(), // unsigned two-byte integer
            DataType::UInt32 => "UINTEGER".to_string(), // unsigned four-byte integer
            DataType::UInt64 => "UBIGINT".to_string(), // unsigned eight-byte integer
            DataType::Int8 => "TINYINT".to_string(),   // signed one-byte integer
            DataType::Int16 => "SMALLINT".to_string(), // signed two-byte integer
            DataType::Int32 => "INTEGER".to_string(),  // signed four-byte integer
            DataType::Int64 => "BIGINT".to_string(),   // signed eight-byte integer
            DataType::Float32 => "FLOAT".to_string(), // alias for REAL, single precision floating-point number (4 bytes)
            DataType::Float64 => "DOUBLE".to_string(), // double-precision floating point number
            DataType::String => "VARCHAR".to_string(), // variable-length character string
            DataType::Binary => "BINARY".to_string(), // binary data
            DataType::Date => "DATE".to_string(),     // calendar date (year, month day)
            DataType::Time => "TIME".to_string(),     // time of day (no time zone)
            DataType::Datetime => "DATETIME".to_string(), // combination of time and date
            DataType::Duration => "INTERVAL".to_string(),
            DataType::Struct(_) => "JSON".to_string(),
            DataType::Embedding(size) => format!("FLOAT[{size}]").to_string(),
            DataType::List(dtype) => match dtype.as_ref() {
                DataType::Boolean => "BOOL[]".to_string(),
                DataType::UInt8 => "UTINYINT[]".to_string(),
                DataType::UInt16 => "USMALLINT[]".to_string(),
                DataType::UInt32 => "UINTEGER[]".to_string(),
                DataType::UInt64 => "UBIGINT[]".to_string(),
                DataType::Int8 => "TINYINT[]".to_string(),
                DataType::Int16 => "SMALLINT[]".to_string(),
                DataType::Int32 => "INTEGER[]".to_string(),
                DataType::Int64 => "BIGINT[]".to_string(),
                DataType::Float32 => "FLOAT[]".to_string(),
                DataType::Float64 => "DOUBLE[]".to_string(),
                DataType::String => "VARCHAR[]".to_string(),
                DataType::Binary => "BINARY[]".to_string(),
                DataType::Date => "DATE[]".to_string(),
                DataType::Time => "TIME[]".to_string(),
                DataType::Datetime => "DATETIME[]".to_string(),
                _ => {
                    log::error!("TODO: to_sql unknown SQL DataType::List type {dtype}");
                    "UNKNOWN[]".to_string()
                }
            }, // https://duckdb.org/docs/sql/data_types/list
            DataType::Null => "NULL".to_string(), // null value
            DataType::Unknown => {
                log::error!("TODO: to_sql unknown SQL DataType::Unknown type {self}");
                "UNKNOWN".to_string()
            }
        }
    }

    pub fn from_sql(s: impl AsRef<str>) -> Self {
        match s.as_ref() {
            "BOOL" => DataType::Boolean,
            "UTINYINT" => DataType::UInt8, // unsigned one-byte integer
            "USMALLINT" => DataType::UInt16, // unsigned two-byte integer
            "UINTEGER" => DataType::UInt32, // unsigned four-byte integer
            "UBIGINT" => DataType::UInt64, // unsigned eight-byte integer
            "TINYINT" => DataType::Int8,   // signed one-byte integer
            "SMALLINT" => DataType::Int16, // signed two-byte integer
            "INTEGER" => DataType::Int32,  // signed four-byte integer
            "BIGINT" => DataType::Int64,   // signed eight-byte integer
            "FLOAT" => DataType::Float32, // alias for REAL, single precision floating-point number (4 bytes)
            "DOUBLE" => DataType::Float64, // double-precision floating point number
            "VARCHAR" => DataType::String, // variable-length character string
            "BLOB" => DataType::Binary,   // binary data
            "DATE" => DataType::Date,     // calendar date (year, month day)
            "TIME" => DataType::Time,     // time of day (no time zone)
            "TIMESTAMP" => DataType::Datetime, // combination of time and date
            "DATETIME" => DataType::Datetime, // combination of time and date
            "TIMESTAMP WITH TIME ZONE" => DataType::Datetime, // combination of time and date that uses the current time zone
            "TIMESTAMPTZ" => DataType::Datetime, // combination of time and date that uses the current time zone
            "NULL" => DataType::Null,            // null value
            "UUID" => DataType::String,
            "BOOLEAN" => DataType::Boolean,
            "INTERVAL" => DataType::Duration,
            "JSON" => DataType::Struct(Box::default()),
            "BOOL[]" => DataType::List(Box::new(DataType::Boolean)),
            "UTINYINT[]" => DataType::List(Box::new(DataType::UInt8)),
            "USMALLINT[]" => DataType::List(Box::new(DataType::UInt16)),
            "UINTEGER[]" => DataType::List(Box::new(DataType::UInt32)),
            "UBIGINT[]" => DataType::List(Box::new(DataType::UInt64)),
            "TINYINT[]" => DataType::List(Box::new(DataType::Int8)),
            "SMALLINT[]" => DataType::List(Box::new(DataType::Int16)),
            "INTEGER[]" => DataType::List(Box::new(DataType::Int32)),
            "BIGINT[]" => DataType::List(Box::new(DataType::Int64)),
            "FLOAT[]" => DataType::List(Box::new(DataType::Float32)),
            "DOUBLE[]" => DataType::List(Box::new(DataType::Float64)),
            "VARCHAR[]" => DataType::List(Box::new(DataType::String)),
            "BLOB[]" => DataType::List(Box::new(DataType::Binary)),
            "DATE[]" => DataType::List(Box::new(DataType::Date)),
            "TIME[]" => DataType::List(Box::new(DataType::Time)),
            type_name => {
                // Handle FLOAT[N] types
                if let Some(captures) = regex::Regex::new(r"^FLOAT\[(\d+)\]$")
                    .unwrap()
                    .captures(type_name)
                {
                    if let Some(size) = captures.get(1) {
                        log::debug!("FLOAT[{}]", size.as_str());
                        return DataType::Embedding(size.as_str().parse::<usize>().unwrap());
                    }
                }

                log::error!("TODO: from_sql unknown SQL type {}", s.as_ref());
                DataType::Unknown
            }
        }
    }
}
