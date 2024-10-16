use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataText {
    pub text: MetadataTextImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataTextImpl {
    pub num_lines: usize,
    pub num_chars: usize,
}

impl MetadataText {
    pub fn new(num_lines: usize, num_chars: usize) -> Self {
        Self {
            text: MetadataTextImpl {
                num_lines,
                num_chars,
            },
        }
    }
}

impl std::fmt::Display for MetadataText {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MetadataText({} lines, {} chars)",
            self.text.num_lines, self.text.num_chars
        )
    }
}
