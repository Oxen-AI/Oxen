use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct MetadataVideo {
    pub video: MetadataVideoImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct MetadataVideoImpl {
    pub num_seconds: f64,
    pub width: usize,
    pub height: usize,
}

#[derive(Deserialize, Debug, IntoParams, ToSchema)]
pub struct VideoThumbnail {
    #[schema(example = 320)]
    pub width: Option<u32>,
    #[schema(example = 240)]
    pub height: Option<u32>,
    #[schema(example = 1.0)]
    pub timestamp: Option<f64>,
    #[schema(example = true)]
    pub thumbnail: Option<bool>,
}

impl MetadataVideo {
    pub fn new(num_seconds: f64, width: usize, height: usize) -> Self {
        Self {
            video: MetadataVideoImpl {
                num_seconds,
                width,
                height,
            },
        }
    }
}

impl std::fmt::Display for MetadataVideo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MetadataVideo({}x{} {}s)",
            self.video.width, self.video.height, self.video.num_seconds
        )
    }
}
