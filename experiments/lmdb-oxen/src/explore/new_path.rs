//!
//! New fundamental path types:
//!     - `Name`: A file name of the name of a directory. No components.
//!     - `AbsolutePath`: An absolute path to a file or directory. Always starts at the file system root.
//!     - `RelativePath`: A relative path to a file or directory within a repository. Always starts with `./`.
//!                       Is rooted at the repository root. Has 0 or more components.
//!
use std::{
    io,
    path::{Path, PathBuf, StripPrefixError},
};

use crate::explore::{
    interfaces::{Accumulator, Builder},
    lazy_merkle_lmdb::MerkleTreeL,
    scratch::{Hash, Repository, RepositoryTree},
};
use thiserror::Error;

//
//  N a m e
//

pub struct Name(String);

#[derive(Debug, Error)]
pub enum NameError {
    #[error("No name found for path: '{0}'")]
    PathHasNoName(PathBuf),
    #[error("Path has non UTF-8 name: '{0}'")]
    NonUtf8Name(PathBuf),
}

/// Gets the name of the file or directory only.
impl TryFrom<PathBuf> for Name {
    type Error = NameError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        match path.file_name() {
            Some(name) => match name.to_str() {
                Some(name) => Ok(Name(name.to_string())),
                None => Err(NameError::NonUtf8Name(path)),
            },
            None => Err(NameError::PathHasNoName(path)),
        }
    }
}

//
//  A b s o l u t e    P a t h
//

pub struct AbsolutePath(PathBuf);

impl AbsolutePath {
    /// Converts the path to its canonical form and returns an `AbsolutePath`.
    pub fn new(path: PathBuf) -> Result<Self, io::Error> {
        let absolute = path.canonicalize()?;
        Ok(Self(absolute))
    }

    pub fn join(&self, name: &Name) -> Self {
        Self(self.0.join(name.0.as_str()))
    }
}

//
//  R e l a t i v e    P a t h
//

/// The relative path to a file or directory within a repository.
pub struct RelativePath(Vec<String>);

#[derive(Debug, Error)]
pub enum RelativePathError {
    #[error("Path is not relative to the repository root: {0}")]
    NotRelativeToRepoRoot(#[from] StripPrefixError),
    #[error("Path contains a non-UTF-8 component: {0}")]
    NonUtf8Name(PathBuf),
}

impl RelativePath {
    pub fn new(repo: &Repository, path: &Path) -> Result<Self, RelativePathError> {
        let relative = path.strip_prefix(&repo.root)?;
        let components = {
            let mut components = Vec::new();
            for c in relative.components().into_iter() {
                let Some(part) = c.as_os_str().to_str() else {
                    return Err(RelativePathError::NonUtf8Name(path.to_path_buf()));
                };
                components.push(part.to_string());
            }
            components
        };
        Ok(Self(components))
    }

    #[inline]
    pub fn components<'a>(&'a self) -> impl Iterator<Item = &'a str> {
        self.0.iter().map(|s| s.as_str())
    }

    /// Appends the file or directory name to this current path, creating a new relative path.
    pub fn join(&self, file_or_directory: &Name) -> RelativePath {
        let mut components = self.0.clone();
        components.push(file_or_directory.0.clone());
        RelativePath(components)
    }

    pub fn builder() -> RelativePathBuilder {
        RelativePathBuilder(RelativePath(vec![]))
    }

    /// SAFETY: callers **MUST** guarenetee that each part is a single component of a real
    ///         relative path. There **MUST NOT** be any path separators in the parts nor
    ///         can there be any `'.'` or `'..'` components. Each component must be a valid
    ///         file or directory name.
    pub(crate) unsafe fn from_parts(parts: Vec<String>) -> Self {
        Self(parts)
    }
}

pub struct RelativePathBuilder(RelativePath);

impl Accumulator<&RepositoryTree> for RelativePathBuilder {
    fn accumulate(&mut self, node: &RepositoryTree) -> &mut Self {
        self.0.0.push(node.name().to_string());
        self
    }
}

impl Builder<RelativePath> for RelativePathBuilder {
    fn build(self) -> RelativePath {
        self.0
    }
}
