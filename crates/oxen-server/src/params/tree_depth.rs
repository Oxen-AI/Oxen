use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct TreeDepthQuery {
    pub depth: Option<i32>,
    pub subtrees: Option<String>,
    pub is_download: Option<bool>,
}
