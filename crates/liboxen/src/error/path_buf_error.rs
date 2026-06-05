//! # PathBufError
//!
//! Struct that wraps a PathBuf and implements the necessary traits for errors.
//!

use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct PathBufError(PathBuf);

impl From<&Path> for PathBufError {
    fn from(p: &Path) -> Self {
        PathBufError(p.to_path_buf())
    }
}

impl From<PathBuf> for PathBufError {
    fn from(p: PathBuf) -> Self {
        PathBufError(p)
    }
}

impl std::fmt::Display for PathBufError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0.to_string_lossy())
    }
}

impl std::error::Error for PathBufError {}
