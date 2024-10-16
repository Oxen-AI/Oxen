pub mod data_frame_size;
pub mod schema;
pub mod update_result;

use crate::model::data_frame::data_frame_size::DataFrameSize;
use crate::model::data_frame::schema::Schema;

use serde::{Deserialize, Serialize};

use polars::frame::DataFrame;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFrameSchemaSize {
    pub schema: Schema,
    pub size: DataFrameSize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFrameSliceSchemas {
    pub source: DataFrameSchemaSize,
    pub slice: DataFrameSchemaSize,
}

pub struct DataFrameSlice {
    pub schemas: DataFrameSliceSchemas,
    pub slice: DataFrame,
    pub total_entries: usize,
}

impl DataFrameSchemaSize {
    pub fn from_df_size(data_frame_size: &DataFrameSize, schema: &Schema) -> DataFrameSchemaSize {
        DataFrameSchemaSize {
            schema: schema.to_owned(),
            size: DataFrameSize {
                height: data_frame_size.height,
                width: data_frame_size.width,
            },
        }
    }
    pub fn from_df(df: &DataFrame, schema: &Schema) -> DataFrameSchemaSize {
        DataFrameSchemaSize {
            schema: schema.to_owned(),
            size: DataFrameSize {
                height: df.height(),
                width: df.width(),
            },
        }
    }
}
