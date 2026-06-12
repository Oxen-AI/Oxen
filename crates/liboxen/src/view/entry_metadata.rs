use serde::{Deserialize, Serialize};

use super::StatusMessage;
use crate::model::entry::metadata_entry::MetadataEntry;
use crate::view::entries::EMetadataEntry;

use utoipa::ToSchema;

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct MetadataEntryResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub entry: MetadataEntry,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct EMetadataEntryResponseView {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub entry: EMetadataEntry,
}
