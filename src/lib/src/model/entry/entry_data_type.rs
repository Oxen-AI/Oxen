use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Deserialize, Serialize, Debug, Clone, Eq, Hash, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum EntryDataType {
    Dir,
    Text,
    Image,
    Video,
    Audio,
    Tabular,
    Binary,
}

impl EntryDataType {
    pub fn to_emoji(&self) -> String {
        match *self {
            EntryDataType::Dir => "📁".to_string(),
            EntryDataType::Text => "📄".to_string(),
            EntryDataType::Image => "📸".to_string(),
            EntryDataType::Video => "🎥".to_string(),
            EntryDataType::Audio => "🎵".to_string(),
            EntryDataType::Tabular => "📊".to_string(),
            EntryDataType::Binary => "📦".to_string(),
        }
    }
}

impl FromStr for EntryDataType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "dir" => Ok(EntryDataType::Dir),
            "text" => Ok(EntryDataType::Text),
            "image" => Ok(EntryDataType::Image),
            "video" => Ok(EntryDataType::Video),
            "audio" => Ok(EntryDataType::Audio),
            "tabular" => Ok(EntryDataType::Tabular),
            "binary" => Ok(EntryDataType::Binary),
            _ => Err(()),
        }
    }
}

impl fmt::Display for EntryDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            EntryDataType::Dir => write!(f, "dir"),
            EntryDataType::Text => write!(f, "text"),
            EntryDataType::Image => write!(f, "image"),
            EntryDataType::Video => write!(f, "video"),
            EntryDataType::Audio => write!(f, "audio"),
            EntryDataType::Tabular => write!(f, "tabular"),
            EntryDataType::Binary => write!(f, "binary"),
        }
    }
}
