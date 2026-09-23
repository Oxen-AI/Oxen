//! Repositories
//!
//! This module is all the domain logic for repositories, and it's sub-modules.
//!

use crate::api::requests::RepoNew;
use crate::config::RepositoryConfig;
use crate::constants;
use crate::core;
use crate::core::db::merkle_node::DEFAULT_MERKLE_NODE_BACKEND;
use crate::core::refs::with_ref_manager;
use crate::core::repo_locks;
use crate::core::v_latest::commits::remove_commit_count_db_from_cache_with_children;
use crate::core::workspaces::workspace_name_index;
use crate::error::OxenError;
use crate::model::Commit;
use crate::model::LocalRepository;
use crate::model::RepoIdentity;
use crate::model::merkle_tree;
use crate::storage::S3Opts;
use crate::util;
use crate::util::fs::AtomicFile;
use bytes::Bytes;
use jwalk::WalkDir;
use regex::Regex;
use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};
use std::sync::LazyLock;
use tokio::task::spawn_blocking;
use uuid::Uuid;

pub mod add;
pub mod branches;
pub mod checkout;
pub mod clean;
pub mod clone;
pub mod commits;
pub mod data_frames;
pub mod diffs;
pub mod download;
pub mod entries;
pub mod fetch;
pub mod fsck;
pub mod init;
pub mod load;
pub mod merge;
pub mod metadata;
pub mod name_table;
pub mod prune;
pub mod pull;
pub mod push;
pub mod remote_mode;
pub mod restore;
pub mod revisions;
pub mod rm;
pub mod save;
pub mod size;
pub mod stats;
pub mod status;
pub mod tree;
pub mod verify;
pub mod workspaces;

pub use add::add;
pub use checkout::checkout;
pub use clone::{clone, clone_url, deep_clone_url};
pub use commits::commit;
pub use download::download;
pub use fetch::{fetch_all, fetch_branch};
pub use init::init;
pub use load::load;
pub use pull::{pull, pull_all, pull_remote_branch};
pub use push::push;
pub use restore::restore;
pub use rm::rm;
pub use save::save;
pub use status::status;
pub use status::status_from_dir;

/// The directory holding the repositories in `namespace`, under `sync_dir`.
///
/// `namespace` must be a single ordinary path component, so the result always stays inside
/// `sync_dir`.
pub fn namespace_dir(sync_dir: &Path, namespace: &str) -> Result<PathBuf, OxenError> {
    Ok(sync_dir.join(plain_segment(namespace)?))
}

/// The directory holding repository `namespace`/`name`, under `sync_dir`.
///
/// `namespace` and `name` must each be a single ordinary path component, so the result always
/// stays inside `sync_dir`. Build every server-side repository path through this rather than
/// joining the parts directly, so an identifier that arrived over the network cannot name a
/// location outside `sync_dir`.
pub fn repo_dir(sync_dir: &Path, namespace: &str, name: &str) -> Result<PathBuf, OxenError> {
    Ok(namespace_dir(sync_dir, namespace)?.join(plain_segment(name)?))
}

/// Rejects anything that is not exactly one ordinary path component: `.`, `..`, a separator, an
/// absolute path, a Windows prefix, or an empty string.
fn plain_segment(segment: &str) -> Result<&OsStr, OxenError> {
    let mut components = Path::new(segment).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(part)), None) => Ok(part),
        _ => Err(OxenError::InvalidRepoIdentifier(segment.into())),
    }
}

pub fn get_by_namespace_and_name(
    sync_dir: &Path,
    namespace: impl AsRef<str>,
    name: impl AsRef<str>,
    server_s3_opts: Option<&S3Opts>,
) -> Result<Option<LocalRepository>, OxenError> {
    let namespace = namespace.as_ref();
    let name = name.as_ref();
    let repo_dir = repo_dir(sync_dir, namespace, name)?;

    if !repo_dir.exists() {
        log::debug!("Repo does not exist: {repo_dir:?}");
        return Ok(None);
    }

    LocalRepository::from_dir_with_server_opts(&repo_dir, server_s3_opts)
        .inspect_err(|err| match err {
            // An unsupported on-disk format is a permanent property of the repo, not a server
            // defect. See docs/deprecations.md.
            OxenError::UnsupportedRepoVersion(version) => {
                log::warn!("Unsupported repo on-disk version {version} at {repo_dir:?}")
            }
            _ => tracing::error!(repo_dir = ?repo_dir, cause = ?err, "Error getting repo from dir"),
        })
        .map(Some)
}

/// Look up a repo by `<namespace>/<name>` under `sync_dir`, off the async worker.
pub async fn get_by_namespace_and_name_async(
    sync_dir: &Path,
    namespace: &str,
    name: &str,
    server_s3_opts: Option<&S3Opts>,
) -> Result<Option<LocalRepository>, OxenError> {
    let sync_dir = sync_dir.to_path_buf();
    let namespace = namespace.to_string();
    let name = name.to_string();
    let server_s3_opts = server_s3_opts.cloned();
    tokio::task::spawn_blocking(move || {
        get_by_namespace_and_name(&sync_dir, &namespace, &name, server_s3_opts.as_ref())
    })
    .await?
}

pub async fn is_empty(repo: &LocalRepository) -> Result<bool, OxenError> {
    let repo = repo.clone();
    tokio::task::spawn_blocking(move || with_ref_manager(&repo, |manager| manager.is_empty()))
        .await?
}

pub fn list_namespaces(sync_dir: &Path) -> Result<Vec<String>, OxenError> {
    log::debug!("repositories::entries::list_namespaces repositories for sync dir: {sync_dir:?}");
    let mut namespaces: Vec<String> = vec![];
    for path in std::fs::read_dir(sync_dir)? {
        let path = path.unwrap().path();
        if is_namespace_dir(&path) {
            let name = path.file_name().unwrap().to_str().unwrap();
            namespaces.push(String::from(name));
        }
    }

    Ok(namespaces)
}

fn is_namespace_dir(path: &Path) -> bool {
    if let Some(name) = path.to_str() {
        // Make sure it is a directory, that doesn't start with .oxen and has repositories in it
        return path.is_dir()
            && !name.starts_with(constants::OXEN_HIDDEN_DIR)
            && list_repos_in_namespace(path).next().is_some();
    }
    false
}

/// Lazily-load each repository in a namespace's directory.
///
/// Skips sub-directories that either don't have an `.oxen/` dir within them or that
/// fail to load via [`LocalRepository::from_dir`].
pub fn list_repos_in_namespace(namespace_path: &Path) -> impl Iterator<Item = LocalRepository> {
    log::debug!(
        "repositories::entries::list_repos_in_namespace repositories for dir: {namespace_path:?}"
    );
    WalkDir::new(namespace_path)
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let local_dir = entry.path();
            let oxen_dir = util::fs::oxen_hidden_dir(&local_dir);
            if !oxen_dir.exists() {
                return None;
            }
            LocalRepository::from_dir(&local_dir).ok()
        })
}

/// Record what this repository and its namespace are called, filling only hints the repository
/// does not already hold.
///
/// Writes nothing when neither argument supplies a hint, when both are already recorded, or when
/// the repository carries no identity. An existing hint is left as it is: a repository's namespace
/// changes by being moved, so [`transfer_namespace`] is what updates that one. The identity to fill
/// is read from the repository's config, so `repo` may have been opened before it was recorded.
///
/// # Errors
/// [`OxenError::LockTimeout`] when a maintenance operation holds the repository.
/// [`OxenError::RepoAlreadyExists`] when filling a hint would complete a name another repository
/// already holds.
pub fn record_name_hints(
    sync_dir: &Path,
    repo: &LocalRepository,
    namespace: Option<&str>,
    name: Option<&str>,
) -> Result<(), OxenError> {
    if namespace.is_none() && name.is_none() {
        return Ok(());
    }

    // Held across the read and the write, so no maintenance operation can run its exclusive
    // section between them. Multiple config writers can still run concurrently.
    let _write = repo_locks::begin_write(repo)?;
    let path = util::fs::config_filepath(&repo.path);
    let mut config = RepositoryConfig::from_file(&path)?;
    let Some(identity) = config.identity.as_mut() else {
        return Ok(());
    };
    let mut changed = false;
    for (hint, value) in [
        (&mut identity.namespace, namespace),
        (&mut identity.name, name),
    ] {
        if let (None, Some(value)) = (&hint, value) {
            *hint = Some(value.to_string());
            changed = true;
        }
    }
    if !changed {
        return Ok(());
    }

    let Some((namespace, name, repo_uuid)) = identity.held_name() else {
        config.save(&path)?;
        return Ok(());
    };

    // Filling the second half gives the repository a name it can be looked up by, so the entry is
    // claimed ahead of the config write and given back where that write does not land.
    let table = name_table::NameTable::open(sync_dir)?;
    table.claim(&namespace, &name, repo_uuid)?;
    if let Err(err) = config.save(&path) {
        if let Err(undo) = table.release(&namespace, &name, repo_uuid) {
            log::error!(
                "Failed to give back {namespace}/{name} after its hint write failed: {undo}"
            );
        }
        return Err(err.into());
    }
    Ok(())
}

/// Move a repository into `to_namespace`, recording `namespace_hint` as what that namespace is
/// called, or clearing the recorded name when it is `None` so no repository is left describing the
/// namespace it came from.
///
/// `to_namespace` addresses the directory while `namespace_hint` is a display name, so where a
/// control plane owns namespaces the first is a UUID and the second is not. They are separate
/// parameters because they may legitimately differ, and coincide only where the server owns its
/// own namespaces. A repository carrying no identity keeps none.
pub fn transfer_namespace(
    sync_dir: &Path,
    repo_name: &str,
    from_namespace: &str,
    to_namespace: &str,
    namespace_hint: Option<&str>,
    server_s3_opts: Option<&S3Opts>,
) -> Result<LocalRepository, OxenError> {
    log::debug!("transfer_namespace from: {from_namespace} to: {to_namespace}");

    let from_dir = repo_dir(sync_dir, from_namespace, repo_name)?;
    let to_dir = repo_dir(sync_dir, to_namespace, repo_name)?;

    if !from_dir.exists() {
        log::debug!("Error while transferring repo: repo does not exist: {from_dir:?}");
        return Err(OxenError::RepoNotFound(Box::new(
            RepoNew::from_namespace_name(from_namespace, repo_name, None),
        )));
    }

    // A repo carrying no identity keeps none.
    let mut config = RepositoryConfig::from_file(util::fs::config_filepath(&from_dir))?;
    // Read before the change below. A move rewrites the namespace half of a recorded name, so a
    // repo recording a name of its own is the one with a name-table entry to move.
    let recorded = config.identity.as_ref().and_then(|identity| {
        let name = identity.name.clone()?;
        Some((identity.namespace.clone(), name, identity.repo_uuid))
    });
    if let Some(identity) = config.identity.as_mut() {
        identity.namespace = namespace_hint.map(str::to_string);
    }

    // ensure DB instance is closed before we move the repo
    core::staged::remove_from_cache_with_children(&from_dir)?;
    core::refs::remove_from_cache(&from_dir)?;
    workspace_name_index::remove_from_cache_with_children(&from_dir);
    remove_commit_count_db_from_cache_with_children(&from_dir);

    // Moved ahead of everything the transfer writes, so a name the destination already holds
    // refuses it with no directory created and no config rewritten.
    if let Some((from, name, repo_uuid)) = &recorded {
        move_recorded_name(sync_dir, from.as_deref(), namespace_hint, name, *repo_uuid)?;
    }
    let give_back_the_name = || {
        if let Some((from, name, repo_uuid)) = &recorded
            && let Err(undo) =
                move_recorded_name(sync_dir, namespace_hint, from.as_deref(), name, *repo_uuid)
        {
            log::error!("Failed to move the name table entry for {name} back: {undo}");
        }
    };

    if let Err(err) = util::fs::create_dir_all(&to_dir) {
        give_back_the_name();
        return Err(err);
    }

    // Written once nothing but the rename can fail, so the rename is the only step whose failure
    // leaves the repo describing a namespace it is not in, with the entry naming the same one.
    if let Err(err) = config.save(util::fs::config_filepath(&from_dir)) {
        give_back_the_name();
        return Err(err.into());
    }
    let moved = util::fs::rename(&from_dir, &to_dir);
    // Again after the move, so no name index handle opened during it answers for `from_dir` and no
    // commit count handle keeps the old files open.
    workspace_name_index::remove_from_cache_with_children(&from_dir);
    remove_commit_count_db_from_cache_with_children(&from_dir);
    moved?;

    let updated_repo =
        get_by_namespace_and_name(sync_dir, to_namespace, repo_name, server_s3_opts)?;
    match updated_repo {
        Some(new_repo) => Ok(new_repo),
        None => Err(OxenError::FailedTransfer),
    }
}

/// Record `name` as sitting in `to` rather than `from` in the server's name table. Swapping `from`
/// and `to` undoes the move. Writes nothing where a repository other than `repo_uuid` holds the
/// name.
///
/// # Errors
/// [`OxenError::RepoAlreadyExists`] when a repository in `to` already holds `name`.
fn move_recorded_name(
    sync_dir: &Path,
    from: Option<&str>,
    to: Option<&str>,
    name: &str,
    repo_uuid: Uuid,
) -> Result<(), OxenError> {
    let table = || name_table::NameTable::open(sync_dir);
    match (from, to) {
        (Some(from), Some(to)) => table()?.move_to_namespace(from, name, to, repo_uuid),
        (Some(from), None) => table()?.release(from, name, repo_uuid),
        (None, Some(to)) => table()?.claim(to, name, repo_uuid).map(|_| ()),
        (None, None) => Ok(()),
    }
}

static VALID_REPO_NAME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[[:alnum:]][[:alnum:]_.\-]+$").unwrap());

// A namespace is addressed by whatever control plane owns namespaces above the server, so it holds
// to the narrower rule those names have to satisfy as well.
static VALID_NAMESPACE_NAME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[[:alnum:]][[:alnum:]_\-]{1,49}$").unwrap());

/// Whether `name` is valid in the repository position: an alphanumeric first character, then one or
/// more alphanumerics, `_`, `.`, or `-`, so at least two characters in all.
pub fn is_valid_repo_name(name: &str) -> bool {
    VALID_REPO_NAME_RE.is_match(name)
}

/// Whether `name` is valid in the namespace position, which is stricter than the repository
/// position: an alphanumeric first character, then one to forty-nine alphanumerics, `_`, or `-`, so
/// no `.` and at most fifty characters in all.
pub fn is_valid_namespace_name(name: &str) -> bool {
    VALID_NAMESPACE_NAME_RE.is_match(name)
}

/// Create a repository under `root_dir`, recording `identity` as who it is.
///
/// # Errors
/// [`OxenError::RepoAlreadyExists`] when `root_dir` already holds the repository, or when another
/// repository holds the name `identity` records.
pub async fn create(
    root_dir: &Path,
    new_repo: RepoNew,
    identity: Option<RepoIdentity>,
    server_s3_opts: Option<&S3Opts>,
) -> Result<LocalRepository, OxenError> {
    if !is_valid_repo_name(&new_repo.name) {
        return Err(OxenError::InvalidRepoName(new_repo.name.into()));
    }
    if !is_valid_namespace_name(&new_repo.namespace) {
        return Err(OxenError::InvalidNamespaceName(new_repo.namespace.into()));
    }
    let dir = repo_dir(root_dir, &new_repo.namespace, &new_repo.name)?;
    // Refused ahead of the claim, so a create the occupied directory turns away records no name.
    if dir.exists() {
        log::error!("Repository already exists {dir:?}");
        return Err(OxenError::RepoAlreadyExists(Box::new(new_repo)));
    }

    // Claimed before the repository is created, so of two creates of one name exactly one
    // proceeds. Only a name this call records is given back when the creation fails: one the
    // repository already held is its own either way.
    let held = identity.as_ref().and_then(RepoIdentity::held_name);
    let claimed = match held.clone() {
        Some((namespace, name, repo_uuid)) => {
            edit_name_table(root_dir, move |table| {
                table.claim(&namespace, &name, repo_uuid)
            })
            .await?
        }
        None => false,
    };
    let give_back = held.filter(|_| claimed);

    let created = create_unclaimed(&dir, new_repo, identity, server_s3_opts).await;
    if created.is_err()
        && let Some((namespace, name, repo_uuid)) = give_back
    {
        let given_back = format!("{namespace}/{name}");
        if let Err(err) = edit_name_table(root_dir, move |table| {
            table.release(&namespace, &name, repo_uuid)
        })
        .await
        {
            log::error!("Failed to give back {given_back} after its create failed: {err}");
        }
    }
    created
}

/// Run `edit` against the server's name table under `sync_dir`, off the async worker.
///
/// The table is opened and `edit`'s transaction commits inside one blocking task.
async fn edit_name_table<R, F>(sync_dir: &Path, edit: F) -> Result<R, OxenError>
where
    F: FnOnce(&name_table::NameTable) -> Result<R, OxenError> + Send + 'static,
    R: Send + 'static,
{
    let sync_dir = sync_dir.to_path_buf();
    spawn_blocking(move || edit(&name_table::NameTable::open(&sync_dir)?)).await?
}

/// Create the repository at `repo_dir`, recording `identity` as who it is, leaving the server's
/// name table alone.
///
/// `repo_dir` must not exist, and `new_repo`'s namespace and name are the caller's to validate.
async fn create_unclaimed(
    repo_dir: &Path,
    mut new_repo: RepoNew,
    identity: Option<RepoIdentity>,
    server_s3_opts: Option<&S3Opts>,
) -> Result<LocalRepository, OxenError> {
    // Create the repo dir
    log::debug!("repositories::create repo dir: {repo_dir:?}");
    util::fs::create_dir_all(repo_dir)?;

    // Create oxen hidden dir
    let hidden_dir = util::fs::oxen_hidden_dir(repo_dir);
    log::debug!("repositories::create hidden dir: {hidden_dir:?}");
    util::fs::create_dir_all(&hidden_dir)?;

    // Create config file
    let config = crate::config::RepositoryConfig {
        storage: new_repo
            .storage_kind
            .map(|kind| crate::storage::StorageConfig {
                kind,
                versions_path: None,
            }),
        merkle_node_backend: Some(
            new_repo
                .merkle_node_backend
                .unwrap_or(DEFAULT_MERKLE_NODE_BACKEND),
        ),
        identity,
        ..Default::default()
    };
    let local_repo = LocalRepository::new_with_server_opts(repo_dir, config, server_s3_opts)?;
    local_repo.save()?;

    // Initialize version store
    let version_store = local_repo.version_store();
    version_store.init().await?;

    // Create history dir
    let history_dir = util::fs::oxen_hidden_dir(repo_dir).join(constants::HISTORY_DIR);
    util::fs::create_dir_all(history_dir)?;

    // Create HEAD file and point it to DEFAULT_BRANCH_NAME
    with_ref_manager(&local_repo, |manager| {
        manager.set_head(constants::DEFAULT_BRANCH_NAME)?;
        Ok(())
    })?;

    // If the user supplied files, add and commit them. An empty list means none were supplied.
    let files = new_repo.files.take().unwrap_or_default();
    if let Some(user) = files.first().map(|file| file.user.clone()) {
        log::debug!("repositories::create files: {:?}", files.len());
        let payloads: Vec<(PathBuf, Bytes)> = files
            .into_iter()
            .map(|file| (repo_dir.join(file.path), file.contents.into_bytes()))
            .collect();
        let paths: Vec<PathBuf> = payloads.iter().map(|(path, _)| path.clone()).collect();

        // Every publish fsyncs, so the whole materialization runs in one offload.
        spawn_blocking(move || {
            for (path, bytes) in &payloads {
                AtomicFile::new(path).write(bytes)?;
            }
            Ok::<(), OxenError>(())
        })
        .await??;

        for path in &paths {
            add(&local_repo, path).await?;
        }

        let commit =
            core::v_latest::commits::commit_with_user(&local_repo, "Initial commit", &user)?;
        branches::create(&local_repo, constants::DEFAULT_BRANCH_NAME, &commit.id)?;
    }

    Ok(local_repo)
}

/// Give back the name the repository at `repo_dir` records, so another repository may take it.
///
/// Writes nothing where the config cannot be read, or records no name of its own, since a
/// repository the server can read no name for holds no entry that can be shown to be its.
pub async fn release_recorded_name(sync_dir: &Path, repo_dir: &Path) -> Result<(), OxenError> {
    let sync_dir = sync_dir.to_path_buf();
    let repo_dir = repo_dir.to_path_buf();
    spawn_blocking(move || {
        let Some((namespace, name, repo_uuid)) = held_name_in_config(&repo_dir) else {
            return Ok(());
        };
        name_table::NameTable::open(&sync_dir)?.release(&namespace, &name, repo_uuid)
    })
    .await?
}

/// The name recorded in the config at `repo_dir`, with the UUID holding it.
///
/// `None` where the config cannot be read, or records no name of its own.
fn held_name_in_config(repo_dir: &Path) -> Option<(String, String, Uuid)> {
    let config = RepositoryConfig::from_file(util::fs::config_filepath(repo_dir))
        .inspect_err(|err| log::warn!("Cannot read the name recorded at {repo_dir:?}: {err}"))
        .ok()?;
    config.identity.as_ref().and_then(RepoIdentity::held_name)
}

/// Removes a repository: its version blobs, then its directory.
///
/// Consumes `repo` so the Merkle node store it owns closes before the directory is removed. A
/// directory removed around an open LMDB env keeps the mapped `data.mdb` and `lock.mdb`, which
/// fails the removal on Windows and on NFS.
pub async fn delete(repo: LocalRepository) -> Result<(), OxenError> {
    if !repo.path.exists() {
        let err = format!("Repository does not exist {:?}", repo.path);
        return Err(OxenError::basic_str(err));
    }

    // Remove the stored version files first: for non-local backends (and local stores with a
    // custom versions_path) they live outside the repo directory.
    repo.version_store().destroy().await?;

    let path = repo.path.clone();
    drop(repo);
    delete_dir(&path).await
}

/// Removes a repository's directory.
///
/// Version blobs held outside the directory survive, so prefer [`delete`] whenever the repository
/// can be opened.
pub async fn delete_dir(path: &Path) -> Result<(), OxenError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<(), OxenError> {
        // Close DB instances before trying to delete the directory
        core::staged::remove_from_cache_with_children(&path)?;
        core::refs::ref_manager::remove_from_cache(&path)?;
        workspace_name_index::remove_from_cache_with_children(&path);
        remove_commit_count_db_from_cache_with_children(&path);

        // Drop cached DuckDB connections too. On NFS, unlinking a still-open file leaves a hidden
        // .nfsXXXX entry that fails the rmdir with ENOTEMPTY.
        core::db::data_frames::df_db::remove_df_db_from_cache_with_children(&path)?;

        log::debug!("Deleting repo directory: {path:?}");
        let removed = util::fs::remove_dir_all(&path);
        // Again after the removal, so no name index or commit count handle opened during it
        // survives.
        workspace_name_index::remove_from_cache_with_children(&path);
        remove_commit_count_db_from_cache_with_children(&path);
        removed
    })
    .await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::api::requests::RepoNew;
    use crate::config::RepositoryConfig;
    use crate::config::UserConfig;
    use crate::constants;
    use crate::constants::OXEN_HIDDEN_DIR;
    use crate::core::db::merkle_node::MerkleNodeBackend;
    use crate::core::repo_locks;
    use crate::core::workspaces::workspace_name_index;
    use crate::error::OxenError;
    use crate::model::file::{FileContents, FileNew};
    use crate::model::{Commit, LocalRepository, RepoIdentity};
    use crate::namespaces;
    use crate::repositories;
    use crate::repositories::name_table::NameTable;
    use crate::sync_dir::NAME_TABLE_DIR;
    use crate::test;
    use crate::util;
    use std::path::{Path, PathBuf};
    use time::OffsetDateTime;
    use uuid::Uuid;

    /// The identity a server that owns its namespaces would record for a new repo.
    fn server_identity(namespace: &str, name: &str) -> Option<RepoIdentity> {
        Some(RepoIdentity::minted(namespace, name))
    }

    #[tokio::test]
    async fn test_delete_removes_custom_versions_path_outside_repo() -> Result<(), OxenError> {
        use crate::config::RepositoryConfig;
        use crate::storage::{StorageConfig, StorageKind};
        use bytes::Bytes;

        test::run_empty_dir_test_async(|dir| async move {
            // A local store whose versions root lives OUTSIDE the repo directory: deleting
            // the repo directory alone would leak it.
            let repo_path = dir.join("repo");
            let custom_root = dir.join("custom-versions");
            util::fs::create_dir_all(util::fs::oxen_hidden_dir(&repo_path))?;

            let repo = LocalRepository::new(
                &repo_path,
                RepositoryConfig {
                    storage: Some(StorageConfig {
                        kind: StorageKind::Local,
                        versions_path: Some(custom_root.clone()),
                    }),
                    ..Default::default()
                },
            )?;
            repo.save()?;

            let data = b"leak check";
            let hash = util::hasher::hash_buffer(data);

            let store = repo.version_store();
            store.init().await?;
            store.store_version(&hash, Bytes::from_static(data)).await?;
            assert!(store.version_exists(&hash).await?);
            assert!(custom_root.exists());

            let index = workspace_name_index::get_index(&repo)?;
            index.put("a-workspace", "a-workspace-id")?;
            drop(index);

            repositories::delete(repo).await?;

            assert!(
                !custom_root.exists(),
                "custom versions root must be removed"
            );
            assert!(!repo_path.exists(), "repo directory must be removed");

            util::fs::create_dir_all(util::fs::oxen_hidden_dir(&repo_path))?;
            let recreated = LocalRepository::new(&repo_path, RepositoryConfig::default())?;
            assert_eq!(
                workspace_name_index::get_index(&recreated)?.get_id_by_name("a-workspace")?,
                None,
                "a repository created at a deleted one's path starts with an empty name index"
            );
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_create_records_identity_under_oxen_server() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo_new = RepoNew::from_namespace_name("ox", "cats", None);
            let repo =
                repositories::create(&sync_dir, repo_new, server_identity("ox", "cats"), None)
                    .await?;

            let identity = repo.identity.clone().expect("create records identity");
            assert_eq!(identity.name.as_deref(), Some("cats"));
            assert_eq!(
                NameTable::open(&sync_dir)?.get("ox", "cats")?,
                Some(identity.repo_uuid),
                "creating a repository claims the name it records"
            );

            // The config on disk is the authoritative record, so it has to hold the same thing.
            drop(repo);
            let reloaded = LocalRepository::from_dir(sync_dir.join("ox").join("cats"))?;
            assert_eq!(reloaded.identity.as_ref(), Some(&identity));

            Ok(())
        })
        .await
    }

    /// A migration records identity while its caller holds the repository it opened beforehand, so
    /// the hints follow the config rather than that snapshot.
    #[tokio::test]
    async fn test_record_name_hints_fills_hints_recorded_after_the_repo_was_opened()
    -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo_new = RepoNew::from_namespace_name("ox", "cats", None);
            let repo = repositories::create(&sync_dir, repo_new, None, None).await?;
            let path = util::fs::config_filepath(&repo.path);

            let mut config = RepositoryConfig::from_file(&path)?;
            config.identity = Some(RepoIdentity::hintless(Uuid::new_v4()));
            config.save(&path)?;

            repositories::record_name_hints(&sync_dir, &repo, Some("bessie"), Some("kittens"))?;

            let identity = RepositoryConfig::from_file(&path)?
                .identity
                .expect("identity is intact");
            assert_eq!(identity.namespace.as_deref(), Some("bessie"));
            assert_eq!(identity.name.as_deref(), Some("kittens"));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_record_name_hints_fills_hints_a_repo_does_not_hold() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo_new = RepoNew::from_namespace_name("ox", "cats", None);
            let repo_uuid = Uuid::new_v4();
            let repo = repositories::create(
                &sync_dir,
                repo_new,
                Some(RepoIdentity::hintless(repo_uuid)),
                None,
            )
            .await?;
            let path = util::fs::config_filepath(&repo.path);

            repositories::record_name_hints(&sync_dir, &repo, Some("bessie"), None)?;
            assert!(
                !sync_dir.join(NAME_TABLE_DIR).exists(),
                "half a name is no name, so nothing is claimed and no table is opened"
            );

            repositories::record_name_hints(&sync_dir, &repo, Some("bessie"), Some("kittens"))?;

            let identity = RepositoryConfig::from_file(&path)?
                .identity
                .expect("identity is intact");
            assert_eq!(identity.namespace.as_deref(), Some("bessie"));
            assert_eq!(identity.name.as_deref(), Some("kittens"));
            let table = NameTable::open(&sync_dir)?;
            assert_eq!(
                table.get("bessie", "kittens")?,
                Some(repo_uuid),
                "completing a name records the repository under it"
            );

            // Back to holding no name, with the next one it would be given already taken, so two
            // repositories cannot come to record one name.
            let mut config = RepositoryConfig::from_file(&path)?;
            config.identity = Some(RepoIdentity::hintless(repo_uuid));
            config.save(&path)?;
            table.claim("bessie", "mittens", Uuid::new_v4())?;

            let err =
                repositories::record_name_hints(&sync_dir, &repo, Some("bessie"), Some("mittens"))
                    .expect_err("a name another repository holds cannot be recorded");
            assert!(
                matches!(err, OxenError::RepoAlreadyExists(_)),
                "expected a name conflict, got {err:?}"
            );
            let identity = RepositoryConfig::from_file(&path)?
                .identity
                .expect("identity is intact");
            assert_eq!(
                (identity.namespace, identity.name),
                (None, None),
                "a refused claim leaves the repository recording no name"
            );

            Ok(())
        })
        .await
    }

    /// A hint already recorded is what the repository is called; a later request restating it
    /// differently must not rename anything.
    #[tokio::test]
    async fn test_record_name_hints_leaves_hints_already_recorded() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo_new = RepoNew::from_namespace_name("ox", "cats", None);
            let repo =
                repositories::create(&sync_dir, repo_new, server_identity("ox", "cats"), None)
                    .await?;
            let path = util::fs::config_filepath(&repo.path);
            let before = std::fs::metadata(&path)?.modified()?;

            repositories::record_name_hints(&sync_dir, &repo, Some("bessie"), Some("kittens"))?;

            let identity = RepositoryConfig::from_file(&path)?
                .identity
                .expect("identity is intact");
            assert_eq!(identity.namespace.as_deref(), Some("ox"));
            assert_eq!(identity.name.as_deref(), Some("cats"));
            assert_eq!(
                std::fs::metadata(&path)?.modified()?,
                before,
                "recording nothing new must not touch the config"
            );

            Ok(())
        })
        .await
    }

    /// Identity stays all-or-nothing: a repo carrying none does not acquire a bare name.
    #[tokio::test]
    async fn test_record_name_hints_leaves_a_repo_without_identity_alone() -> Result<(), OxenError>
    {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo_new = RepoNew::from_namespace_name("ox", "cats", None);
            let repo = repositories::create(&sync_dir, repo_new, None, None).await?;

            repositories::record_name_hints(&sync_dir, &repo, Some("bessie"), Some("kittens"))?;

            let config = RepositoryConfig::from_file(util::fs::config_filepath(&repo.path))?;
            assert_eq!(config.identity, None);

            Ok(())
        })
        .await
    }

    /// A maintenance operation runs with the repository to itself, so the hint write refuses its
    /// turn rather than rewriting the config underneath it.
    #[tokio::test]
    async fn test_record_name_hints_refuses_while_a_maintenance_operation_holds_the_repo()
    -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo_new = RepoNew::from_namespace_name("ox", "cats", None);
            let repo = repositories::create(
                &sync_dir,
                repo_new,
                Some(RepoIdentity::hintless(Uuid::new_v4())),
                None,
            )
            .await?;

            repo_locks::with_repo_exclusive(&repo, async {
                assert!(matches!(
                    repositories::record_name_hints(&sync_dir, &repo, None, Some("kittens")),
                    Err(OxenError::LockTimeout(_))
                ));
                Ok::<(), OxenError>(())
            })
            .await?;

            let identity = RepositoryConfig::from_file(util::fs::config_filepath(&repo.path))?
                .identity
                .expect("identity is intact");
            assert_eq!(identity.name, None, "the hint is untouched");

            Ok(())
        })
        .await
    }

    /// Create `ox/cats` with `identity`, then move it to `bessie`.
    async fn transferred(
        sync_dir: &Path,
        identity: Option<RepoIdentity>,
        namespace_hint: Option<&str>,
    ) -> Result<LocalRepository, OxenError> {
        let repo_new = RepoNew::from_namespace_name("ox", "cats", None);
        drop(repositories::create(sync_dir, repo_new, identity, None).await?);
        repositories::transfer_namespace(sync_dir, "cats", "ox", "bessie", namespace_hint, None)
    }

    #[tokio::test]
    async fn test_transfer_namespace_moves_the_namespace_hint() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let identity = server_identity("ox", "cats");
            let repo_uuid = identity.as_ref().map(|identity| identity.repo_uuid);

            let moved = transferred(&sync_dir, identity, Some("bessie")).await?;

            let moved_identity = moved.identity.as_ref().expect("identity survives the move");
            assert_eq!(moved_identity.namespace.as_deref(), Some("bessie"));
            assert_eq!(
                moved.repo_uuid(),
                repo_uuid,
                "a namespace move must not change the repo's identity"
            );
            let table = NameTable::open(&sync_dir)?;
            assert_eq!(
                (table.get("ox", "cats")?, table.get("bessie", "cats")?),
                (None, repo_uuid),
                "the entry moves to the namespace the repo now records"
            );

            // A second move onto an occupied destination, so the move fails once the config and
            // the entry have both taken the new namespace. The occupant sits where the moved
            // repository needs its `.oxen` directory, so every platform refuses it: a rename will
            // not replace a non-empty directory, and a directory copy cannot descend into a file.
            drop(moved);
            util::fs::write_to_path(sync_dir.join("zoo").join("cats").join(".oxen"), "taken")?;
            repositories::transfer_namespace(&sync_dir, "cats", "bessie", "zoo", Some("zoo"), None)
                .expect_err("a move onto an occupied destination fails");
            let identity = RepositoryConfig::from_file(util::fs::config_filepath(
                &sync_dir.join("bessie").join("cats"),
            ))?
            .identity
            .expect("identity is intact");
            assert_eq!(
                (identity.namespace.as_deref(), table.get("zoo", "cats")?),
                (Some("zoo"), repo_uuid),
                "a failed rename leaves the entry naming the namespace the repo records"
            );

            Ok(())
        })
        .await
    }

    /// A repo created before the server recorded identity must come out of a move still carrying
    /// none, rather than gaining a name hint with no UUID beside it.
    #[tokio::test]
    async fn test_transfer_namespace_leaves_a_repo_without_identity_alone() -> Result<(), OxenError>
    {
        test::run_empty_dir_test_async(|sync_dir| async move {
            assert_eq!(
                transferred(&sync_dir, None, Some("bessie")).await?.identity,
                None
            );
            Ok(())
        })
        .await
    }

    /// The hint is written only once nothing else can fail, so a transfer that cannot even start
    /// leaves the repo describing the namespace it is still in, and its name-table entry where it
    /// was.
    #[tokio::test]
    async fn test_transfer_namespace_leaves_the_hint_alone_when_the_move_cannot_start()
    -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo_new = RepoNew::from_namespace_name("ox", "cats", None);
            let repo =
                repositories::create(&sync_dir, repo_new, server_identity("ox", "cats"), None)
                    .await?;
            let repo_uuid = repo.repo_uuid().expect("a server identity carries a UUID");
            let repo_path = repo.path.clone();
            drop(repo);

            // A file where the destination namespace directory belongs, so the transfer fails
            // creating it, after the point the hint used to be written.
            util::fs::write_to_path(sync_dir.join("bessie"), "not a directory")?;

            let result = repositories::transfer_namespace(
                &sync_dir,
                "cats",
                "ox",
                "bessie",
                Some("bessie"),
                None,
            );
            assert!(result.is_err(), "the transfer must fail");

            let identity = RepositoryConfig::from_file(util::fs::config_filepath(&repo_path))?
                .identity
                .expect("identity is intact");
            assert_eq!(
                identity.namespace.as_deref(),
                Some("ox"),
                "a transfer that never moved anything must not have rewritten the hint"
            );
            let table = NameTable::open(&sync_dir)?;
            assert_eq!(
                (table.get("ox", "cats")?, table.get("bessie", "cats")?),
                (Some(repo_uuid), None),
                "a transfer that never moved anything must not have moved the entry either"
            );

            Ok(())
        })
        .await
    }

    /// Under an auth provider the destination is a UUID, so recording it would put a UUID in a
    /// field that means a human-readable name.
    #[tokio::test]
    async fn test_transfer_namespace_clears_the_hint_when_the_destination_is_not_a_name()
    -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let moved = transferred(&sync_dir, server_identity("ox", "cats"), None).await?;

            let identity = moved.identity.as_ref().expect("identity survives the move");
            assert_eq!(identity.namespace, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_local_repository_api_create_empty_with_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let namespace: &str = "test-namespace";
            let name: &str = "test-repo-name";
            let initial_commit_id = format!("{}", uuid::Uuid::new_v4());
            let timestamp = OffsetDateTime::now_utc();
            let root_commit = Commit {
                id: initial_commit_id,
                parent_ids: vec![],
                message: String::from(constants::INITIAL_COMMIT_MSG),
                author: String::from("Ox"),
                email: String::from("ox@oxen.ai"),
                timestamp,
            };
            let repo_new = RepoNew::from_root_commit(namespace, name, root_commit);
            let _repo = repositories::create(&sync_dir, repo_new, None, None).await?;

            let repo_path = Path::new(&sync_dir)
                .join(Path::new(namespace))
                .join(Path::new(name));
            assert!(repo_path.exists());

            // Test that we can successful load a repository from that dir
            let _repo = LocalRepository::from_dir(&repo_path)?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_local_repository_api_create_with_an_empty_files_list() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let namespace: &str = "test-namespace";
            let name: &str = "test-repo-name";

            // A client can send `"files": []`, which must behave like sending no files at all.
            let repo_new = RepoNew::from_files(namespace, name, vec![], None);
            let repo = repositories::create(&sync_dir, repo_new, None, None).await?;

            assert!(repo.path.exists());
            assert!(
                repositories::commits::list(&repo)?.is_empty(),
                "an empty files list should not produce a commit"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_local_repository_api_create_empty_with_files() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let namespace: &str = "test-namespace";
            let name: &str = "test-repo-name";

            let user = UserConfig::get()?.to_user();
            let files: Vec<FileNew> = vec![FileNew {
                path: PathBuf::from("README"),
                contents: FileContents::Text(String::from("Hello world!")),
                user,
            }];
            let repo_new = RepoNew::from_files(namespace, name, files, None);
            let _repo = repositories::create(&sync_dir, repo_new, None, None).await?;

            let repo_path = Path::new(&sync_dir)
                .join(Path::new(namespace))
                .join(Path::new(name));
            assert!(repo_path.exists());

            // Test that we can successful load a repository from that dir
            let _repo = LocalRepository::from_dir(&repo_path)?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_local_repository_api_create_empty_no_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let namespace: &str = "test-namespace";
            let name: &str = "test-repo-name";
            let repo_new = RepoNew::from_namespace_name(namespace, name, None);
            let _repo = repositories::create(&sync_dir, repo_new, None, None).await?;

            let repo_path = Path::new(&sync_dir)
                .join(Path::new(namespace))
                .join(Path::new(name));
            assert!(repo_path.exists());

            // Test that we can successful load a repository from that dir
            let _repo = LocalRepository::from_dir(&repo_path)?;

            Ok(())
        })
        .await
    }

    #[test]
    fn test_is_valid_repo_name_accepts_valid_names() {
        // Basic alphanumeric
        assert!(repositories::is_valid_repo_name("my-repo"));
        assert!(repositories::is_valid_repo_name("my_repo"));
        assert!(repositories::is_valid_repo_name("my.repo"));
        assert!(repositories::is_valid_repo_name("MyRepo123"));
        assert!(repositories::is_valid_repo_name("a1"));
        assert!(repositories::is_valid_repo_name("Cat-Dog-Classifier"));
        assert!(repositories::is_valid_repo_name("v2.0.1"));
        assert!(repositories::is_valid_repo_name("test_repo.v2"));
    }

    #[test]
    fn test_is_valid_repo_name_rejects_invalid_names() {
        // Spaces
        assert!(!repositories::is_valid_repo_name("repo with spaces"));
        // Starts with non-alphanumeric
        assert!(!repositories::is_valid_repo_name("-repo"));
        assert!(!repositories::is_valid_repo_name(".repo"));
        assert!(!repositories::is_valid_repo_name("_repo"));
        // Too short (must be at least 2 chars)
        assert!(!repositories::is_valid_repo_name("a"));
        assert!(!repositories::is_valid_repo_name(""));
        // Contains special characters
        assert!(!repositories::is_valid_repo_name("repo/name"));
        assert!(!repositories::is_valid_repo_name("repo@name"));
        assert!(!repositories::is_valid_repo_name("repo name"));
        assert!(!repositories::is_valid_repo_name("repo!name"));
    }

    #[test]
    fn test_is_valid_namespace_name_accepts_valid_names() {
        assert!(repositories::is_valid_namespace_name("ox"));
        assert!(repositories::is_valid_namespace_name("my-org"));
        assert!(repositories::is_valid_namespace_name("my_org"));
        assert!(repositories::is_valid_namespace_name("MyOrg123"));
        // A control plane addresses a namespace by UUID.
        assert!(repositories::is_valid_namespace_name(
            &Uuid::new_v4().to_string()
        ));
        assert!(repositories::is_valid_namespace_name(&"a".repeat(50)));
    }

    #[test]
    fn test_is_valid_namespace_name_rejects_invalid_names() {
        // Valid in the repository position, so the two rules cannot be one.
        assert!(!repositories::is_valid_namespace_name("my.org"));
        assert!(!repositories::is_valid_namespace_name("v2.0.1"));
        assert!(!repositories::is_valid_namespace_name(&"a".repeat(51)));
        assert!(!repositories::is_valid_namespace_name("a"));
        assert!(!repositories::is_valid_namespace_name(""));
        assert!(!repositories::is_valid_namespace_name("-org"));
        assert!(!repositories::is_valid_namespace_name("org name"));
        assert!(!repositories::is_valid_namespace_name("org/name"));
    }

    #[tokio::test]
    async fn test_local_repository_api_create_rejects_invalid_name() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let namespace = "test-namespace";
            let name = "repo with spaces";
            let repo_new = RepoNew::from_namespace_name(namespace, name, None);
            let result = repositories::create(&sync_dir, repo_new, None, None).await;

            assert!(result.is_err(), "Expected error but got: {result:?}");
            match result.unwrap_err() {
                OxenError::InvalidRepoName(invalid_name) => {
                    assert_eq!(invalid_name.to_string(), name);
                }
                other => panic!("Expected InvalidRepoName error, got: {other:?}"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_local_repository_api_create_rejects_invalid_namespace() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let namespace = "-invalid-namespace";
            let name = "valid-repo";
            let repo_new = RepoNew::from_namespace_name(namespace, name, None);
            let result = repositories::create(&sync_dir, repo_new, None, None).await;

            assert!(result.is_err(), "Expected error but got: {result:?}");
            match result.unwrap_err() {
                OxenError::InvalidNamespaceName(invalid_name) => {
                    assert_eq!(invalid_name.to_string(), namespace);
                }
                other => panic!("Expected InvalidNamespaceName error, got: {other:?}"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_create_defaults_to_lmdb_backend() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo_new = RepoNew::from_namespace_name("ns", "repo", None);
            let repo = repositories::create(&sync_dir, repo_new, None, None).await?;
            assert_eq!(repo.merkle_node_backend(), MerkleNodeBackend::Lmdb);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_create_honors_requested_merkle_backend() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            // Requests the non-default backend, so this fails if the request is ignored.
            let mut repo_new = RepoNew::from_namespace_name("ns", "repo", None);
            repo_new.merkle_node_backend = Some(MerkleNodeBackend::Filesystem);
            let repo = repositories::create(&sync_dir, repo_new, None, None).await?;
            assert_eq!(repo.merkle_node_backend(), MerkleNodeBackend::Filesystem);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_local_repository_api_list_namespaces_one() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace: &str = "test-namespace";
            let name: &str = "cool-repo";

            let namespace_dir = sync_dir.join(namespace);
            util::fs::create_dir_all(&namespace_dir)?;
            let repo_dir = namespace_dir.join(name);
            repositories::init(&repo_dir)?;

            // The server's own state sits beside the namespaces, so neither listing may report it
            // as one: the name table, and the access-key store `oxen-server add-user` writes.
            drop(NameTable::open(sync_dir)?);
            util::fs::create_dir_all(sync_dir.join(OXEN_HIDDEN_DIR).join("keys"))?;

            let namespaces = repositories::list_namespaces(sync_dir)?;
            assert_eq!(namespaces.len(), 1);
            assert_eq!(namespaces[0], namespace);
            assert_eq!(namespaces::list(sync_dir), vec![namespace]);
            assert!(
                namespaces::get(sync_dir, NAME_TABLE_DIR).is_none(),
                "the server's own directory is not a namespace to look up either"
            );

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_local_repository_api_list_multiple_namespaces() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace_1 = "my-namespace-1";
            let namespace_1_dir = sync_dir.join(namespace_1);

            let namespace_2 = "my-namespace-2";
            let namespace_2_dir = sync_dir.join(namespace_2);

            // We will not create any repos in the last namespace, to test that it gets filtered out
            let namespace_3 = "my-namespace-3";
            let _ = sync_dir.join(namespace_3);

            let _ = repositories::init(namespace_1_dir.join("testing1"))?;
            let _ = repositories::init(namespace_1_dir.join("testing2"))?;
            let _ = repositories::init(namespace_2_dir.join("testing3"))?;

            let repos = repositories::list_namespaces(sync_dir)?;
            assert_eq!(repos.len(), 2);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_local_repository_api_list_multiple_within_namespace() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace = "my-namespace";
            let namespace_dir = sync_dir.join(namespace);

            let _ = repositories::init(namespace_dir.join("testing1"))?;
            let _ = repositories::init(namespace_dir.join("testing2"))?;
            let _ = repositories::init(namespace_dir.join("testing3"))?;

            let repos = repositories::list_repos_in_namespace(&namespace_dir);
            assert_eq!(repos.count(), 3);

            Ok(())
        })
    }

    #[test]
    fn test_repo_dir_rejects_identifiers_that_escape_the_sync_dir() {
        let sync_dir = Path::new("/data");

        for bad in ["..", ".", "", "/", "a/b", "../..", "/etc", "./x"] {
            assert!(
                matches!(
                    repositories::repo_dir(sync_dir, bad, "repo"),
                    Err(OxenError::InvalidRepoIdentifier(_))
                ),
                "namespace {bad:?} should be rejected"
            );
            assert!(
                matches!(
                    repositories::repo_dir(sync_dir, "namespace", bad),
                    Err(OxenError::InvalidRepoIdentifier(_))
                ),
                "name {bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn test_repo_dir_accepts_ordinary_identifiers() -> Result<(), OxenError> {
        let sync_dir = Path::new("/data");

        assert_eq!(
            repositories::repo_dir(sync_dir, "my-namespace", "my-repo")?,
            Path::new("/data/my-namespace/my-repo")
        );
        // A dot inside a segment is ordinary; only a segment that *is* `.` or `..` is not.
        assert_eq!(
            repositories::repo_dir(sync_dir, "ns.1", "repo..name")?,
            Path::new("/data/ns.1/repo..name")
        );
        assert_eq!(
            repositories::namespace_dir(sync_dir, "my-namespace")?,
            Path::new("/data/my-namespace")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_local_repository_api_get_by_name() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace = "my-namespace";
            let name = "my-repo";
            let repo_dir = sync_dir.join(namespace).join(name);
            util::fs::create_dir_all(&repo_dir)?;

            let _ = repositories::init(&repo_dir)?;
            let _repo =
                repositories::get_by_namespace_and_name(sync_dir, namespace, name, None)?.unwrap();
            Ok(())
        })
    }

    #[tokio::test]
    async fn test_local_repository_transfer_namespace() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let old_namespace: &str = "test-namespace-old";
            let new_namespace: &str = "test-namespace-new";

            let old_namespace_dir = sync_dir.join(old_namespace);
            let new_namespace_dir = sync_dir.join(new_namespace);

            let name = "moving-repo";

            let initial_commit_id = format!("{}", uuid::Uuid::new_v4());
            let timestamp = OffsetDateTime::now_utc();
            // Create new namespace
            util::fs::create_dir_all(&new_namespace_dir)?;

            let root_commit = Commit {
                id: initial_commit_id,
                parent_ids: vec![],
                message: String::from(constants::INITIAL_COMMIT_MSG),
                author: String::from("Ox"),
                email: String::from("ox@oxen.ai"),
                timestamp,
            };
            let repo_new = RepoNew::from_root_commit(old_namespace, name, root_commit);
            let _repo = repositories::create(&sync_dir, repo_new, None, None).await?;

            let old_namespace_repos = repositories::list_repos_in_namespace(&old_namespace_dir);
            let new_namespace_repos = repositories::list_repos_in_namespace(&new_namespace_dir);

            assert_eq!(old_namespace_repos.count(), 1);
            assert_eq!(new_namespace_repos.count(), 0);

            // Drop the repo to release its LMDB env before the transfer: on Windows
            // `transfer_namespace` renames via copy-then-remove-source, and a mapped env file
            // can't be removed while this repo holds it open.
            drop(_repo);

            // Transfer to new namespace
            let updated_repo = repositories::transfer_namespace(
                &sync_dir,
                name,
                old_namespace,
                new_namespace,
                Some(new_namespace),
                None,
            )?;

            // Log out updated_repo
            log::debug!("updated_repo: {updated_repo:?}");

            let new_repo_path = sync_dir.join(new_namespace).join(name);
            assert_eq!(updated_repo.path, new_repo_path);

            // Check that the old namespace is empty
            let old_namespace_repos = repositories::list_repos_in_namespace(&old_namespace_dir);
            let new_namespace_repos = repositories::list_repos_in_namespace(&new_namespace_dir);

            assert_eq!(old_namespace_repos.count(), 0);
            assert_eq!(new_namespace_repos.count(), 1);

            Ok(())
        })
        .await
    }
}
