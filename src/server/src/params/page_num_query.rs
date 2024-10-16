use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct PageNumQuery {
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}

#[derive(Deserialize, Debug)]
pub struct PageNumVersionQuery {
    pub page: Option<usize>,
    pub page_size: Option<usize>,
    pub api_version: Option<String>,
}
