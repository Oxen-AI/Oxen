use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFrameSize {
    pub height: usize,
    pub width: usize,
}

impl DataFrameSize {
    pub fn is_empty(&self) -> bool {
        self.height == 0 && self.width == 0
    }
    pub fn from_df(df: &DataFrame) -> DataFrameSize {
        DataFrameSize {
            height: df.height(),
            width: df.width(),
        }
    }
}
