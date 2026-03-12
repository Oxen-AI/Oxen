//! Custom data types that have some extra functionality or sytnax sugar
//!

use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum CustomDataType {
    Path,
    Unknown,
}

impl fmt::Display for CustomDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl CustomDataType {
    pub fn from_string(s: impl AsRef<str>) -> CustomDataType {
        match s.as_ref() {
            "path" => CustomDataType::Path,
            _ => CustomDataType::Unknown,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            CustomDataType::Path => "path",
            CustomDataType::Unknown => "?",
        }
    }
}
