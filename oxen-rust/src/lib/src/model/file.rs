use super::User;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub enum FileContents {
    Text(String),
    Binary(Vec<u8>),
}

impl Serialize for FileContents {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            FileContents::Text(text) => serializer.serialize_str(text),
            FileContents::Binary(bytes) => serializer.serialize_bytes(bytes),
        }
    }
}

impl<'de> Deserialize<'de> for FileContents {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FileContentsVisitor;

        impl Visitor<'_> for FileContentsVisitor {
            type Value = FileContents;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or byte array")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(FileContents::Text(value.to_owned()))
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(FileContents::Binary(value.to_vec()))
            }
        }

        deserializer.deserialize_any(FileContentsVisitor)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FileNew {
    pub path: PathBuf,
    pub contents: FileContents,
    pub user: User,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TempFileNew {
    pub path: PathBuf,
    pub contents: FileContents,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TempFilePathNew {
    pub path: PathBuf,
    pub temp_file_path: PathBuf,
}

impl FileNew {
    pub fn new_text(path: impl AsRef<Path>, contents: impl AsRef<str>, user: User) -> FileNew {
        FileNew {
            path: path.as_ref().to_path_buf(),
            contents: FileContents::Text(contents.as_ref().to_string()),
            user,
        }
    }

    pub fn new_binary(path: impl AsRef<Path>, contents: impl AsRef<[u8]>, user: User) -> FileNew {
        FileNew {
            path: path.as_ref().to_path_buf(),
            contents: FileContents::Binary(contents.as_ref().to_vec()),
            user,
        }
    }
}
