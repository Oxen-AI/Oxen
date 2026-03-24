use std::path::PathBuf;

use crate::error::OxenError;

use super::PaginateOpts;

#[derive(Clone, Debug)]
pub struct EmbeddingQueryOpts {
    pub path: PathBuf,  // path to the data frame
    pub column: String, // embedding column to query
    pub query: String,  // key=value
    pub name: String,   // name of the similarity column
    pub pagination: PaginateOpts,
}

impl EmbeddingQueryOpts {
    pub fn parse_query(&self) -> Result<(String, String), OxenError> {
        let parts: Vec<&str> = self.query.split('=').collect();
        if parts.len() != 2 {
            return Err(OxenError::basic_str(
                "Query must be in the format key=value",
            ));
        }
        Ok((parts[0].trim().to_string(), parts[1].trim().to_string()))
    }
}
