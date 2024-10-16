use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataVideo {
    pub video: MetadataVideoImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataVideoImpl {
    pub num_seconds: f64,
    pub width: usize,
    pub height: usize,
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
