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
}
