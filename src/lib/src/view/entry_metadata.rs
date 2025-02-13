use serde::{Deserialize, Serialize};

use crate::model::{entry::metadata_entry::MetadataEntryView, MetadataEntry};

use super::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct MetadataEntryResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub entry: MetadataEntry,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct MetadataEntryResponseView {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub entry: MetadataEntryView,
}
