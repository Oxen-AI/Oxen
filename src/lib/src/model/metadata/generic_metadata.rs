use serde::{Deserialize, Serialize};

use crate::model::metadata::{
    MetadataAudio, MetadataDir, MetadataImage, MetadataTabular, MetadataText, MetadataVideo,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum GenericMetadata {
    MetadataDir(MetadataDir),
    MetadataText(MetadataText),
    MetadataImage(MetadataImage),
    MetadataVideo(MetadataVideo),
    MetadataAudio(MetadataAudio),
    MetadataTabular(MetadataTabular),
}

impl std::fmt::Display for GenericMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            GenericMetadata::MetadataDir(metadata) => write!(f, "{}", metadata),
            GenericMetadata::MetadataText(metadata) => write!(f, "{}", metadata),
            GenericMetadata::MetadataImage(metadata) => write!(f, "{}", metadata),
            GenericMetadata::MetadataVideo(metadata) => write!(f, "{}", metadata),
            GenericMetadata::MetadataAudio(metadata) => write!(f, "{}", metadata),
            GenericMetadata::MetadataTabular(metadata) => write!(f, "{}", metadata),
        }
    }
}
