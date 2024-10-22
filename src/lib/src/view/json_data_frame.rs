// This format is deprecated favor of the new JSON format

use std::io::BufWriter;
use std::str;

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use crate::core::df::tabular;
use crate::model::DataFrameSize;
use crate::opts::PaginateOpts;
use crate::{model::Schema, opts::DFOpts};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrame {
    pub schema: Schema,
    pub view_schema: Schema,
    pub view_size: DataFrameSize,
    pub full_size: DataFrameSize,
    pub data: serde_json::Value,
}

impl JsonDataFrame {
    pub fn empty(schema: &Schema) -> JsonDataFrame {
        JsonDataFrame {
            schema: schema.to_owned(),
            view_schema: schema.to_owned(),
            view_size: DataFrameSize {
                height: 0,
                width: 0,
            },
            full_size: DataFrameSize {
                height: 0,
                width: 0,
            },
            data: serde_json::Value::Null,
        }
    }

    pub fn from_df(df: &mut DataFrame) -> JsonDataFrame {
        let schema = Schema::from_polars(&df.schema());
        JsonDataFrame {
            schema: schema.to_owned(),
            view_schema: schema.to_owned(),
            view_size: DataFrameSize {
                height: df.height(),
                width: df.width(),
            },
            full_size: DataFrameSize {
                height: df.height(),
                width: df.width(),
            },
            data: JsonDataFrame::json_data(df),
        }
    }

    pub fn from_df_paginated(df: DataFrame, opts: &PaginateOpts) -> JsonDataFrame {
        let full_height = df.height();
        let full_width = df.width();

        let page_size = opts.page_size;
        let page = opts.page_num;

        let start = if page == 0 { 0 } else { page_size * (page - 1) };
        let end = page_size * page;

        let schema = Schema::from_polars(&df.schema());
        let mut opts = DFOpts::empty();
        opts.slice = Some(format!("{}..{}", start, end));
        let mut view_df = tabular::transform(df, opts).unwrap();
        let view_schema = Schema::from_polars(&view_df.schema());

        JsonDataFrame {
            schema,
            view_schema,
            view_size: DataFrameSize {
                height: view_df.height(),
                width: view_df.width(),
            },
            full_size: DataFrameSize {
                height: full_height,
                width: full_width,
            },
            data: JsonDataFrame::json_data(&mut view_df),
        }
    }

    pub fn from_df_opts(df: DataFrame, opts: DFOpts) -> JsonDataFrame {
        let full_height = df.height();
        let full_width = df.width();

        let schema = Schema::from_polars(&df.schema());
        let mut view_df = tabular::transform(df, opts).unwrap();
        let view_schema = Schema::from_polars(&view_df.schema());

        JsonDataFrame {
            schema,
            view_schema,
            view_size: DataFrameSize {
                height: view_df.height(),
                width: view_df.width(),
            },
            full_size: DataFrameSize {
                height: full_height,
                width: full_width,
            },
            data: JsonDataFrame::json_data(&mut view_df),
        }
    }

    pub fn to_df(&self) -> DataFrame {
        if self.data == serde_json::Value::Null {
            DataFrame::empty()
        } else {
            // The fields were coming out of order, so we need to reorder them
            let columns = self.schema.fields_names();
            log::debug!("Got columns: {:?}", columns);

            match &self.data {
                serde_json::Value::Array(arr) => {
                    if !arr.is_empty() {
                        let data = self.data.to_string();
                        let content = Cursor::new(data.as_bytes());
                        log::debug!("Deserializing df: [{}]", data);
                        let df = JsonReader::new(content).finish().unwrap();

                        let opts = DFOpts::from_column_names(columns);
                        tabular::transform(df, opts).unwrap()
                    } else {
                        let cols = columns
                            .iter()
                            .map(|name| {
                                Column::Series(Series::new(
                                    PlSmallStr::from_str(name),
                                    Vec::<&str>::new(),
                                ))
                            })
                            .collect::<Vec<Column>>();
                        DataFrame::new(cols).unwrap()
                    }
                }
                _ => {
                    log::error!("Could not parse non-array json data: {:?}", self.data);
                    DataFrame::empty()
                }
            }
        }
    }

    pub fn from_slice(
        df: &mut DataFrame,
        og_schema: Schema,
        og_size: DataFrameSize,
        view_schema: Schema,
    ) -> JsonDataFrame {
        JsonDataFrame {
            schema: og_schema,
            view_schema,
            view_size: DataFrameSize {
                height: df.height(),
                width: df.width(),
            },
            full_size: og_size,
            data: JsonDataFrame::json_data(df),
        }
    }

    fn json_data(df: &mut DataFrame) -> serde_json::Value {
        log::debug!("Serializing df: [{}]", df);

        // TODO: serialize to json
        let data: Vec<u8> = Vec::new();
        let mut buf = BufWriter::new(data);

        let mut writer = JsonWriter::new(&mut buf).with_json_format(JsonFormat::Json);
        writer.finish(df).expect("Could not write df json buffer");

        let buffer = buf.into_inner().expect("Could not get buffer");

        let json_str = str::from_utf8(&buffer).unwrap();

        serde_json::from_str(json_str).unwrap()
    }
}
