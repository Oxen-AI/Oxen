use serde::Deserialize;
use utoipa::IntoParams;

#[derive(Deserialize, Debug, IntoParams)]
pub struct WorkspaceListQuery {
    pub name: Option<String>,
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}
