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

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The name of a file or directory. Exactly one component and no path separators are allowed.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Name(String);

impl Name {
    #[inline(always)]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Error)]
pub enum NameError {
    #[error("No name found for path: '{0}'")]
    PathHasNoName(PathBuf),
    #[error("Path has non UTF-8 name: '{0}'")]
    NonUtf8Name(PathBuf),
}

/// Gets the name of the file or directory only.
impl<'a> TryFrom<&'a Path> for Name {
    type Error = NameError;

    fn try_from(path: &'a Path) -> Result<Self, Self::Error> {
        match path.file_name() {
            Some(name) => match name.to_str() {
                Some(name) => Ok(Name(name.to_string())),
                None => Err(NameError::NonUtf8Name(path.to_path_buf())),
            },
            None => Err(NameError::PathHasNoName(path.to_path_buf())),
        }
    }
}

/// An absolute path in the filesystem. Always starts at the root. Has zero or more components.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AbsolutePath(PathBuf);

impl AbsolutePath {
    /// Converts the path to its canonical form and returns an `AbsolutePath`.
    pub fn new(path: PathBuf) -> Result<Self, io::Error> {
        let absolute = path.canonicalize()?;
        Ok(Self(absolute))
    }

    /// Create a new [AbsolutePath] that points to the supplied file or directory [Name].
    pub fn join(&self, name: &Name) -> Self {
        Self(self.0.join(name.0.as_str()))
    }

    /// Makes a new `AbsolutePath` from a `RelativePath` relative to a repository's root.
    pub fn from(repo_root: &AbsolutePath, p: &RelativePath) -> Self {
        // `repo.root` is already canonicalized by construction
        let mut abs_path = repo_root.0.clone();
        // `p` is a relative path of 1 or more components
        abs_path.extend(p.components());
        Self(abs_path)
    }

    /// Provide a standard Path reference to this absolute path.
    pub fn as_path(&self) -> &Path {
        self.0.as_path()
    }
}

/// Unwrap an absolute path into a canonicalized std::path::PathBuf.
impl From<AbsolutePath> for PathBuf {
    fn from(abs_path: AbsolutePath) -> Self {
        abs_path.0
    }
}

/// The relative path to a file or directory. Is context-dependent on whichever root created it.
/// Always starts with "./" and has one or more components.
/// Intended for use within an oxen repository.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RelativePath(Vec<Name>);

#[derive(Debug, Error)]
pub enum RelativePathError {
    #[error("Path is not relative to the root: {0}")]
    NotRelativeToRoot(#[from] StripPrefixError),
    #[error("Path contains a non-UTF-8 component: {0}")]
    NonUtf8Name(PathBuf),
    #[error("Path is not canonical: {0}")]
    NotCanonical(#[from] io::Error),
}

impl RelativePath {
    /// Creates a new [RelativePath] in the repository's root.
    /// Is an error if `path` is not relative to the repository root or contains a non-UTF-8 component.
    pub fn new(root: &AbsolutePath, path: &Path) -> Result<Self, RelativePathError> {
        let path = path
            .canonicalize()
            .map_err(RelativePathError::NotCanonical)?;
        let relative = path.strip_prefix(root.as_path())?;
        let components = {
            let mut components = Vec::new();
            for c in relative.components() {
                let Some(part) = c.as_os_str().to_str() else {
                    return Err(RelativePathError::NonUtf8Name(path.to_path_buf()));
                };
                components.push(Name(part.to_string()));
            }
            components
        };
        Ok(Self(components))
    }

    #[inline]
    pub fn components(&self) -> impl Iterator<Item = &Name> {
        self.0.iter()
    }

    /// Appends the file or directory name to this current path, creating a new relative path.
    pub fn join(&self, file_or_directory: &Name) -> RelativePath {
        let mut components = self.0.clone();
        components.push(file_or_directory.clone());
        RelativePath(components)
    }

    /// Make a relative path from a sequence of component names.
    ///
    /// SAFETY: callers **MUST** guarenetee that each part is a single component of a real
    ///         relative path. There **MUST NOT** be any path separators in the parts nor
    ///         can there be any `'.'` or `'..'` components. Each component must be a valid
    ///         file or directory name.
    pub(crate) fn from_parts(parts: Vec<Name>) -> Self {
        Self(parts)
    }
}
