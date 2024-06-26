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
    pub width: usize,
    pub height: usize,
    pub color_space: ImgColorSpace, // RGB, RGBA, etc.
}

#[derive(Deserialize, Debug)]
pub struct ImgResize {
    pub width: Option<u32>,
    pub height: Option<u32>,
}

impl MetadataImage {
    pub fn new(width: usize, height: usize, color_space: ImgColorSpace) -> Self {
        Self {
            image: MetadataImageImpl {
                width,
                height,
                color_space,
            },
        }
    }
}
