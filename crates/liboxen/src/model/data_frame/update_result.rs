use crate::error::OxenError;
use polars::frame::DataFrame;

#[derive(Debug)]
pub enum UpdateResult {
    Success(String, Option<DataFrame>),
    Error(String, OxenError),
}
