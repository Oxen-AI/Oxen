use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataAudio {
    pub audio: MetadataAudioImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataAudioImpl {
    pub num_seconds: f64,
    pub num_channels: usize,
    pub sample_rate: usize,
}

impl MetadataAudio {
    pub fn new(num_seconds: f64, num_channels: usize, sample_rate: usize) -> Self {
        Self {
            audio: MetadataAudioImpl {
                num_seconds,
                num_channels,
                sample_rate,
            },
        }
    }
}

impl std::fmt::Display for MetadataAudio {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MetadataAudio({} channels, {} Hz, {} seconds)",
            self.audio.num_channels, self.audio.sample_rate, self.audio.num_seconds
        )
    }
}
