//! Contains RAII handles for structs that map to real resources. Drop deallocates resources.
//!
//! Common use cases include:
//!   - a local repository's directory
//!   - a [`LocalRepository`] `struct` instance
//!
use std::{
    ops::Deref,
    path::{Path, PathBuf},
};

use crate::{error::OxenError, model::LocalRepository, test::maybe_cleanup_repo};

/// RAII cleanup for a test repo directory.
///
/// Dropping the guard removes the directory via [`maybe_cleanup_repo`]: errors are swallowed since
/// [`Drop`] can't surface them. The same call runs on the normal-exit path *and* during panic
/// unwind.
///
/// [`Deref`]s to a [`&Path`] so it can be used transparently as the managed repo dir.
#[derive(Debug)]
pub struct RepoDirGuard {
    repo_dir: PathBuf,
}

impl RepoDirGuard {
    pub fn new(repo_dir: PathBuf) -> Self {
        Self { repo_dir }
    }
}

/// Cleanup the guarded directory, attempting to remove it from the filesystem.
impl Drop for RepoDirGuard {
    fn drop(&mut self) {
        if let Err(e) = maybe_cleanup_repo(&self.repo_dir) {
            log::warn!("RepoDirGuard cleanup failed for {:?}: {e}", self.repo_dir);
        }
    }
}

/// A guarded directory derefs into a [`& Path`].
impl Deref for RepoDirGuard {
    type Target = Path;
    fn deref(&self) -> &Self::Target {
        &self.repo_dir
    }
}

/// RAII handle to a freshly-initialized test [`LocalRepository`].
///
/// [`Deref`]s into the inner `LocalRepository` so callers can use it as one transparently.
///
/// The [`Drop`] implementation releases the [`LocalRepository`] state first, then deletes
/// the repository's on-disk directory. Releasing [`LocalRepository`] resources is governed
/// by that type's own [`Drop`].
#[derive(Debug)]
pub struct TestLocalRepo {
    // Drop order matters: `repo` is declared before `_guard` so that on drop
    // the inner `LocalRepository` (and any resources it holds, like an LMDB
    // `heed::Env`) is released *before* the [`RepoDirGuard`] removes the
    // on-disk repo directory.
    repo: LocalRepository,
    _guard: RepoDirGuard,
}

impl TestLocalRepo {
    /// Bring a [`LocalRepository`] under automatic resource management.
    ///
    /// Uses a supplied single-use constructor function to make a new [`LocalRepository`],
    /// is then immeately wrapped into the [`TestLocalRepo`] guard.
    pub fn new<R>(make_repo: R) -> Result<Self, OxenError>
    where
        R: FnOnce() -> Result<LocalRepository, OxenError>,
    {
        let repo = make_repo()?;
        let repo_dir = repo.path.clone();
        Ok(Self {
            repo,
            _guard: RepoDirGuard::new(repo_dir),
        })
    }

    /// Drop the inner [`LocalRepository`]  while keeping the on-disk repo dir alive for further reads.
    ///
    /// Can be used to e.g. release an LMDB env. Calling this method consumes the guard and
    /// returns the guard around the repository's on-disk directory.
    ///
    /// This [`TestLocalRepo`] guard can be reinstated by using the `.try_into()` implementation
    /// on the returned [`RepoDirGuard`].
    pub fn drop_local_repo(self) -> RepoDirGuard {
        let TestLocalRepo { repo, _guard } = self;
        drop(repo);
        _guard
    }
}

/// A guarded repository dereferences to a [`& LocalRepository`].
impl Deref for TestLocalRepo {
    type Target = LocalRepository;

    fn deref(&self) -> &Self::Target {
        &self.repo
    }
}

/// Load a repository from the guarded directory, returning a guarded local repository.
impl TryFrom<RepoDirGuard> for TestLocalRepo {
    type Error = OxenError;
    fn try_from(repo_dir: RepoDirGuard) -> Result<Self, Self::Error> {
        let repo = LocalRepository::from_dir(&repo_dir as &Path)?;
        Ok(TestLocalRepo {
            repo,
            _guard: repo_dir,
        })
    }
}
