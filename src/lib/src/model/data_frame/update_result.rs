use crate::error::OxenError;
use polars::frame::DataFrame;

#[derive(Debug)]
pub enum UpdateResult {
    Success(String, DataFrame),
    Error(String, OxenError),
}
