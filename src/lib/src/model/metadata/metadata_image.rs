use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum ImgColorSpace {
    // 8-bit
    RGB,
    RGBA,
    Grayscale,
    GrayscaleAlpha,

    // 16-bit
    Rgb16,
    Rgba16,
    Grayscale16,
    GrayscaleAlpha16,

    // 32-bit float
    Rgb32F,
    Rgba32F,

    Unknown,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataImage {
    pub image: MetadataImageImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataImageImpl {
    pub width: u32,
    pub height: u32,
    pub color_space: Option<ImgColorSpace>,
}

#[derive(Deserialize, Debug)]
pub struct ImgResize {
    pub width: Option<u32>,
    pub height: Option<u32>,
}

impl MetadataImage {
    pub fn new(width: u32, height: u32) -> Self {
        Self {
            image: MetadataImageImpl {
                width,
                height,
                color_space: None,
            },
        }
    }
}

impl std::fmt::Display for MetadataImage {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MetadataImage({}x{})",
            self.image.width, self.image.height
        )
    }
}
