use serde::Deserialize;
use utoipa::IntoParams;

#[derive(Deserialize, Debug, IntoParams)]
pub struct TreeDepthQuery {
    pub depth: Option<i32>,
    pub subtrees: Option<String>,
    pub is_download: Option<bool>,
}
