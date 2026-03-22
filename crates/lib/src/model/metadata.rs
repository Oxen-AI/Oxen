// Metadata per data type
pub mod metadata_audio;
pub mod metadata_dir;
pub mod metadata_image;
pub mod metadata_tabular;
pub mod metadata_text;
pub mod metadata_video;

pub mod generic_metadata;

pub mod to_duckdb_sql;

pub use metadata_audio::MetadataAudio;
pub use metadata_dir::MetadataDir;
pub use metadata_image::MetadataImage;
pub use metadata_tabular::MetadataTabular;
pub use metadata_text::MetadataText;
pub use metadata_video::MetadataVideo;
