use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::model::parsed_resource::ParsedResourceView;

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct ParseResourceResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub resource: ParsedResourceView,
}
