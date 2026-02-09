use serde::Deserialize;
use utoipa::IntoParams;

#[derive(Deserialize, Debug, IntoParams)]
pub struct PageNumQuery {
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}

#[derive(Deserialize, Debug, IntoParams)]
pub struct PageNumVersionQuery {
    pub page: Option<usize>,
    pub page_size: Option<usize>,
    pub api_version: Option<String>,
    /// Depth of nested directory traversal. 0 = current directory only (default), 1 = include immediate children, -1 = unlimited.
    pub depth: Option<isize>,
}
