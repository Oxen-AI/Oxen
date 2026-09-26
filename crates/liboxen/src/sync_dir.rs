//! The layout of an oxen-server's sync dir: which entries at its top are namespaces, and which
//! directories inside a namespace are repositories.

use std::io;
use std::path::{Path, PathBuf};

use crate::constants::OXEN_HIDDEN_DIR;
use crate::error::OxenError;

/// Directory at the top of the sync dir holding the name table's env.
pub(crate) const NAME_TABLE_DIR: &str = "name_table";

/// Directory at the top of the sync dir holding the repositories placed by UUID.
const REPOS_DIR: &str = "repo";

/// File in the repos dir that tells it apart from a namespace called `repo`.
const REPOS_DIR_MARKER: &str = "placement-v2";

/// Entries at the top of the sync dir holding the server's own state rather than a namespace's
/// repositories, `.oxen` among them for the access-key store a server before 0.59.0 kept there. A
/// directory named for one of these is not reported as a namespace, so a namespace could not be
/// seen under that name either.
const SERVER_OWNED_DIRS: &[&str] = &[NAME_TABLE_DIR, OXEN_HIDDEN_DIR, REPOS_DIR];

/// Whether the entry named `name` at the top of the sync dir is a namespace rather than the
/// server's own state.
pub(crate) fn is_namespace(name: &str) -> bool {
    !SERVER_OWNED_DIRS.contains(&name)
}

/// Whether a namespace called `name`, compared ignoring case, would share a directory with the
/// server's own state.
pub(crate) fn is_server_owned(name: &str) -> bool {
    SERVER_OWNED_DIRS
        .iter()
        .any(|owned| owned.eq_ignore_ascii_case(name))
}

/// The directory where `sync_dir` holds a namespace called `repo` rather than the repos dir,
/// if it does.
pub fn namespace_called_repo(sync_dir: &Path) -> Option<PathBuf> {
    let dir = sync_dir.join(REPOS_DIR);
    (dir.is_dir() && !dir.join(REPOS_DIR_MARKER).is_file()).then_some(dir)
}

/// The namespace directories at the top of `sync_dir`, in path order.
pub fn namespace_dirs(sync_dir: &Path) -> Result<Vec<PathBuf>, OxenError> {
    sorted_dirs(sync_dir, |path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(is_namespace)
            && path.is_dir()
    })
    .map_err(|err| OxenError::internal_error(format!("Cannot read {sync_dir:?}: {err}")))
}

/// The repository directories in `namespace_dir`, in path order.
pub fn repo_dirs(namespace_dir: &Path) -> io::Result<Vec<PathBuf>> {
    sorted_dirs(namespace_dir, |path| path.join(OXEN_HIDDEN_DIR).is_dir())
}

/// The entries of `dir` that `keep` accepts, in path order.
fn sorted_dirs(dir: &Path, keep: impl Fn(&Path) -> bool) -> io::Result<Vec<PathBuf>> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| keep(path))
        .collect();
    dirs.sort();
    Ok(dirs)
}
