use crate::config::RepositoryConfig;
use crate::constants::SHALLOW_FLAG;
use crate::constants::{self, DEFAULT_VNODE_SIZE};
use crate::core::db::merkle_node::{MerkleNodeBackend, MerkleNodeStore, create_merkle_node_store};
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::merkle_tree::node::FileNode;
use crate::model::{MetadataEntry, Remote, RemoteRepository};
use crate::storage::{
    BLOCK_V1_MIN_OXEN_VERSION, ContentFormat, S3Opts, StorageConfig, VersionStore,
    create_version_store,
};
use crate::util;
use crate::util::fs::AtomicFile;
use crate::view::RepositoryView;

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, SystemTime};

/// Per-process cache of mtime round-trip tolerance, keyed by repo path. Probed at most
/// once per repo per process via `probe_mtime_drift`.
static MTIME_TOLERANCE_CACHE: LazyLock<Mutex<HashMap<PathBuf, Duration>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// In-process model of a local working tree (CLI) or a server-side repo directory.
/// Not part of any API wire shape — `RepositoryView` is what crosses the wire. Held by
/// `Workspace` and `LocalRepositoryWithEntries`, both of which are likewise in-process.
#[derive(Debug, Clone)]
pub struct LocalRepository {
    pub path: PathBuf,
    // Optional remotes to sync the data to
    remote_name: Option<String>, // name of the current remote ("origin" by default)
    min_version: Option<String>, // write the version if it is past v0.18.4
    remotes: Vec<Remote>,        // List of possible remotes
    vnode_size: Option<u64>,     // Size of the vnodes
    subtree_paths: Option<Vec<PathBuf>>, // If the user clones a subtree, we store the paths here so that we know we don't have the full tree
    pub depth: Option<i32>, // If the user clones with a depth, we store the depth here so that we know we don't have the full tree
    pub vfs: Option<bool>,  // Flag for repositories stored on virtual file systems
    pub remote_mode: Option<bool>, // Flag for remote repositories
    pub workspace_name: Option<String>, // ID of the associated workspace for remote mode
    workspaces: Option<Vec<String>>, // List of workspaces for remote mode

    /// Storage backend configuration. Set once at construction and never mutated for the life
    /// of this `LocalRepository` — the source of truth for which backend `version_store` is.
    storage_config: StorageConfig,
    /// Server-wide S3 opts, carried so that operations that derive a new `LocalRepository`
    /// from an existing one (e.g. workspace creation) can rebuild a matching S3 version store
    /// without re-threading the server opts through every function. CLI paths leave this `None`.
    server_s3_opts: Option<S3Opts>,
    /// Built from `storage_config` + `server_s3_opts` at construction. Never replaced.
    version_store: Arc<dyn VersionStore>,
    /// Where Merkle tree nodes are read from and written to. Built from the repo path at
    /// construction and never replaced, mirroring `version_store`. The backend (file vs. another
    /// engine) is a property of the repo chosen once in `create_merkle_node_store`.
    merkle_node_store: Arc<dyn MerkleNodeStore>,
    /// The backend `merkle_node_store` resolved to. Persisted as `merkle_node_backend` in
    /// `config.toml` by [`save`](Self::save) so the choice is the authoritative record on the next load.
    merkle_node_backend: MerkleNodeBackend,
}

#[derive(Debug, Clone)]
pub struct LocalRepositoryWithEntries {
    pub local_repo: LocalRepository,
    pub entries: Option<Vec<MetadataEntry>>,
}

impl LocalRepository {
    /// Load a repo from disk without any server-side S3 opts. Use this from the CLI and any code
    /// path that doesn't talk to the server's storage config — an on-disk `[storage] kind = "s3"`
    /// will surface as [`OxenError::S3BackendMissingServerOpts`]. Server code should call
    /// [`Self::from_dir_with_server_opts`] instead.
    pub fn from_dir(path: impl AsRef<Path>) -> Result<Self, OxenError> {
        Self::from_dir_with_server_opts(path, None)
    }

    /// Server-side variant of [`Self::from_dir`]: threads the server's S3 opts so that
    /// `[storage] kind = "s3"` repos build a real `S3VersionStore`.
    pub fn from_dir_with_server_opts(
        path: impl AsRef<Path>,
        server_s3_opts: Option<&S3Opts>,
    ) -> Result<Self, OxenError> {
        let path = path.as_ref();
        let config_path = util::fs::config_filepath(path);
        let config = RepositoryConfig::from_file(&config_path)?;
        Self::new_with_server_opts(path, config, server_s3_opts)
    }

    /// Get a reference to the storage configuration this repository was constructed with.
    pub fn storage_config(&self) -> &StorageConfig {
        &self.storage_config
    }

    /// Set the repository's content storage format (persisted on the next `save`).
    /// Activating `ContentFormat::BlockV1` makes eligible new writes store chunked;
    /// existing versions keep their current representation until migrated.
    pub fn set_content_format(&mut self, format: ContentFormat) {
        if format == ContentFormat::BlockV1 {
            self.set_min_version_marker(BLOCK_V1_MIN_OXEN_VERSION);
        }
        self.storage_config.content_format = format;
    }

    /// Server-wide S3 opts the repo was constructed with (always `None` on CLI builds).
    /// Operations that derive a new `LocalRepository` from this one — workspace creation, for
    /// example — should pass this back into the constructor so the derived repo's S3 version
    /// store can be rebuilt.
    pub fn server_s3_opts(&self) -> Option<&S3Opts> {
        self.server_s3_opts.as_ref()
    }

    /// Get a reference to the version store.
    pub fn version_store(&self) -> Arc<dyn VersionStore> {
        Arc::clone(&self.version_store)
    }

    /// Get a handle to the Merkle node store backing this repo's tree nodes.
    pub(crate) fn merkle_node_store(&self) -> Arc<dyn MerkleNodeStore> {
        Arc::clone(&self.merkle_node_store)
    }

    /// The backend this repo's Merkle node store resolved to (see `create_merkle_node_store`).
    pub(crate) fn merkle_node_backend(&self) -> MerkleNodeBackend {
        self.merkle_node_backend
    }

    /// Load a repository from the current directory
    /// this traverses up the directory tree until it finds a .oxen/ directory
    pub fn from_current_dir() -> Result<LocalRepository, OxenError> {
        let current_dir = std::env::current_dir().map_err(OxenError::from)?;
        let repo_dir = util::fs::get_repo_root_from_current_dir()
            .ok_or_else(|| OxenError::local_repo_not_found(&current_dir))?;

        LocalRepository::from_dir(&repo_dir)
    }

    /// Instantiate a new repository at a given path from a `RepositoryConfig`. CLI/test helper;
    /// server code that may build S3-backed repos should call [`Self::new_with_server_opts`]
    /// instead.
    ///
    /// Note: does NOT create the repo on disk or write the config file — just instantiates the
    /// struct. To load an existing repo from disk, use [`Self::from_dir`].
    pub fn new(
        path: impl AsRef<Path>,
        config: RepositoryConfig,
    ) -> Result<LocalRepository, OxenError> {
        Self::new_with_server_opts(path, config, None)
    }

    /// Server-side variant of [`Self::new`]: threads server S3 opts so an
    /// `[storage] kind = "s3"` config builds a real `S3VersionStore`.
    pub fn new_with_server_opts(
        path: impl AsRef<Path>,
        config: RepositoryConfig,
        server_s3_opts: Option<&S3Opts>,
    ) -> Result<LocalRepository, OxenError> {
        // Reject a repo whose on-disk format predates what this build supports (e.g. a config
        // still pinned to 0.19.0) with a clear error, rather than letting the unsupported version
        // surface as a panic when it is later read.
        MinOxenVersion::or_earliest(config.min_version.clone())?;
        if config
            .storage
            .as_ref()
            .is_some_and(|storage| storage.content_format == ContentFormat::BlockV1)
            && config.min_version.as_deref() != Some(BLOCK_V1_MIN_OXEN_VERSION)
        {
            return Err(OxenError::basic_str(format!(
                "block-v1 repositories require min_version = {BLOCK_V1_MIN_OXEN_VERSION}"
            )));
        }

        let path = path.as_ref().to_path_buf();
        let storage_config = config.storage.unwrap_or_default();
        let version_store = create_version_store(&path, &storage_config, server_s3_opts)?;
        let (merkle_node_store, merkle_node_backend) =
            create_merkle_node_store(&path, config.merkle_node_backend)?;
        Ok(LocalRepository {
            path,
            remote_name: config.remote_name,
            min_version: config.min_version,
            remotes: config.remotes,
            vnode_size: config.vnode_size,
            subtree_paths: config.subtree_paths,
            depth: config.depth,
            vfs: config.vfs,
            remote_mode: config.remote_mode,
            workspace_name: config.workspace_name,
            workspaces: config.workspaces,
            storage_config,
            server_s3_opts: server_s3_opts.cloned(),
            version_store,
            merkle_node_store,
            merkle_node_backend,
        })
    }

    /// Test-only constructor: clone an existing repo but back it with a caller-supplied version
    /// store.
    ///
    /// `create_version_store` only ever builds an `S3VersionStore` against real AWS, so tests
    /// can't otherwise point a repo at an in-process fixture (e.g. an s3s-backed store). This
    /// clones `base` and swaps in `version_store`, leaving the on-disk path, merkle store, and
    /// config untouched so the repo still resolves its commit/merkle metadata locally while
    /// reading version files from the injected store.
    #[cfg(test)]
    pub fn new_for_testing(base: &LocalRepository, version_store: Arc<dyn VersionStore>) -> Self {
        LocalRepository {
            version_store,
            ..base.clone()
        }
    }

    /// Test-only constructor: clone an existing repo but back it with a caller-supplied Merkle node
    /// store. Lets a test point a repo at a specific backend (e.g. an LMDB store on the repo's own
    /// path) and exercise real commits/reads through it.
    #[cfg(test)]
    pub(crate) fn new_with_merkle_node_store_for_testing(
        base: &LocalRepository,
        merkle_node_store: Arc<dyn MerkleNodeStore>,
    ) -> Self {
        LocalRepository {
            merkle_node_store,
            ..base.clone()
        }
    }

    pub fn from_view(view: RepositoryView) -> Result<LocalRepository, OxenError> {
        let path = std::env::current_dir()?.join(view.name);
        let storage_config = StorageConfig::default();
        let version_store = create_version_store(&path, &storage_config, None)?;
        let (merkle_node_store, merkle_node_backend) = create_merkle_node_store(&path, None)?;
        Ok(LocalRepository {
            path,
            remotes: vec![],
            remote_name: None,
            min_version: None,
            vnode_size: None,
            subtree_paths: None,
            depth: None,
            vfs: None,
            remote_mode: None,
            workspace_name: None,
            workspaces: None,
            storage_config,
            server_s3_opts: None,
            version_store,
            merkle_node_store,
            merkle_node_backend,
        })
    }

    pub fn from_remote(repo: RemoteRepository, path: &Path) -> Result<LocalRepository, OxenError> {
        let path = path.to_owned();
        let storage_config = StorageConfig::default();
        let version_store = create_version_store(&path, &storage_config, None)?;
        let (merkle_node_store, merkle_node_backend) = create_merkle_node_store(&path, None)?;
        Ok(LocalRepository {
            path,
            remotes: vec![repo.remote],
            remote_name: Some(String::from(constants::DEFAULT_REMOTE_NAME)),
            min_version: None,
            vnode_size: None,
            subtree_paths: None,
            depth: None,
            vfs: None,
            remote_mode: None,
            workspace_name: None,
            workspaces: None,
            storage_config,
            server_s3_opts: None,
            version_store,
            merkle_node_store,
            merkle_node_backend,
        })
    }

    pub fn min_version(&self) -> MinOxenVersion {
        // The stored version is validated at construction (`new_with_server_opts`), so this
        // always parses; fall back to LATEST rather than panic.
        MinOxenVersion::or_earliest(self.min_version.clone()).unwrap_or(MinOxenVersion::LATEST)
    }

    pub fn set_remote_name(&mut self, name: impl AsRef<str>) {
        self.remote_name = Some(name.as_ref().to_string());
    }

    pub fn set_min_version(&mut self, version: MinOxenVersion) {
        self.min_version = Some(version.to_string());
    }

    /// Stamp the raw minimum-version marker persisted to `.oxen/config.toml` (on the
    /// next `save`). Binaries that don't recognize the marker refuse to open the
    /// repo with an upgrade hint, so raise it when enabling an on-disk feature
    /// older binaries would misread (e.g. block-v1 chunked storage). The marker
    /// must be a version `MinOxenVersion::from_string` accepts in current binaries.
    pub fn set_min_version_marker(&mut self, version: &str) {
        self.min_version = Some(version.to_string());
    }

    pub fn remotes(&self) -> &Vec<Remote> {
        &self.remotes
    }

    pub fn dirname(&self) -> String {
        String::from(self.path.file_name().unwrap().to_str().unwrap())
    }

    pub fn vnode_size(&self) -> u64 {
        self.vnode_size.unwrap_or(DEFAULT_VNODE_SIZE)
    }

    pub fn set_vnode_size(&mut self, size: u64) {
        self.vnode_size = Some(size);
    }

    pub fn subtree_paths(&self) -> Option<Vec<PathBuf>> {
        self.subtree_paths.as_ref().map(|paths| {
            paths
                .iter()
                .map(|p| {
                    if p == &PathBuf::from(".") {
                        PathBuf::from("")
                    } else {
                        p.clone()
                    }
                })
                .collect()
        })
    }

    pub fn set_subtree_paths(&mut self, paths: Option<Vec<PathBuf>>) {
        self.subtree_paths = paths;
    }

    pub fn depth(&self) -> Option<i32> {
        self.depth
    }

    pub fn set_depth(&mut self, depth: Option<i32>) {
        self.depth = depth;
    }

    pub fn set_remote_mode(&mut self, is_remote: Option<bool>) {
        self.remote_mode = is_remote;
    }

    pub fn is_remote_mode(&self) -> bool {
        self.remote_mode.unwrap_or(false)
    }

    pub fn is_vfs(&self) -> bool {
        self.vfs.unwrap_or(false)
    }

    pub fn set_vfs(&mut self, is_vfs: Option<bool>) {
        self.vfs = is_vfs;
    }

    /// Save the repository configuration to disk
    pub fn save(&self) -> Result<(), OxenError> {
        let config_path = util::fs::config_filepath(&self.path);

        let config = RepositoryConfig {
            remote_name: self.remote_name.clone(),
            remotes: self.remotes.clone(),
            subtree_paths: self.subtree_paths.clone(),
            depth: self.depth,
            min_version: self.min_version.clone(),
            vnode_size: self.vnode_size,
            storage: Some(self.storage_config.clone()),
            vfs: self.vfs,
            remote_mode: self.remote_mode,
            workspace_name: self.workspace_name.clone(),
            workspaces: self.workspaces.clone(),
            merkle_node_backend: Some(self.merkle_node_backend),
        };

        config.save(&config_path)?;
        Ok(())
    }

    pub fn set_remote(&mut self, name: impl AsRef<str>, url: impl AsRef<str>) -> Remote {
        self.remote_name = Some(name.as_ref().to_owned());
        let name = name.as_ref();
        let url = url.as_ref();
        let remote = Remote {
            name: name.to_owned(),
            url: url.to_owned(),
        };
        if self.has_remote(name) {
            // find remote by name and set
            for i in 0..self.remotes.len() {
                if self.remotes[i].name == name {
                    self.remotes[i] = remote.clone()
                }
            }
        } else {
            // we don't have the key, just push
            self.remotes.push(remote.clone());
        }
        remote
    }

    pub fn delete_remote(&mut self, name: impl AsRef<str>) {
        let name = name.as_ref();
        let mut new_remotes: Vec<Remote> = vec![];
        for i in 0..self.remotes.len() {
            if self.remotes[i].name != name {
                new_remotes.push(self.remotes[i].clone());
            }
        }
        self.remotes = new_remotes;
    }

    pub fn has_remote(&self, name: impl AsRef<str>) -> bool {
        let name = name.as_ref();
        for remote in self.remotes.iter() {
            if remote.name == name {
                return true;
            }
        }
        false
    }

    pub fn get_remote(&self, name: impl AsRef<str>) -> Option<Remote> {
        let name = name.as_ref();
        log::trace!("Checking for remote {name} have {}", self.remotes.len());
        for remote in self.remotes.iter() {
            log::trace!("comparing: {name} -> {}", remote.name);
            if remote.name == name {
                return Some(remote.clone());
            }
        }
        None
    }

    pub fn remote(&self) -> Option<Remote> {
        if let Some(name) = &self.remote_name {
            self.get_remote(name)
        } else {
            None
        }
    }

    pub fn add_workspace(&mut self, name: impl AsRef<str>) {
        let workspace_name = name.as_ref();
        let workspaces = self.workspaces.clone().unwrap_or_default();

        let mut new_workspaces = HashSet::new();
        for workspace in workspaces {
            new_workspaces.insert(workspace.clone());
        }

        new_workspaces.insert(workspace_name.to_string());
        self.workspaces = Some(new_workspaces.iter().cloned().collect());
    }

    pub fn delete_workspace(&mut self, name: impl AsRef<str>) -> Result<(), OxenError> {
        let name = name.as_ref();

        if self.workspaces.is_none() {
            return Err(OxenError::basic_str(format!(
                "Error: Cannot delete workspace {name:?} as it does not exist"
            )));
        }

        // TODO: Allow deletions when workspace_name isn't set?
        // This seems like an impossible scenario...
        if self.workspace_name.is_some() && name == self.workspace_name.as_ref().unwrap() {
            return Err(OxenError::basic_str(
                "Error: Cannot delete current workspace",
            ));
        }

        let mut new_workspaces: Vec<String> = vec![];
        let prev_workspaces = self.workspaces.clone().unwrap();
        for workspace in prev_workspaces {
            if workspace != name {
                new_workspaces.push(workspace.clone());
            }
        }
        self.workspaces = Some(new_workspaces);
        Ok(())
    }

    pub fn has_workspace(&self, name: impl AsRef<str>) -> bool {
        let workspace_name = name.as_ref();
        self.workspaces.is_some()
            && self
                .workspaces
                .clone()
                .unwrap()
                .contains(&workspace_name.to_string())
    }

    // TODO: Should we define setting a workspace that's not in the workspaces vec to be an error?
    pub fn set_workspace(&mut self, name: impl AsRef<str>) -> Result<(), OxenError> {
        let workspace_name = name.as_ref();

        if let Some(ws_name) = self
            .workspaces
            .clone()
            .unwrap()
            .iter()
            .find(|ws| ws.starts_with(&format!("{workspace_name}: ")))
        {
            self.workspace_name = Some(ws_name.to_string());
        } else {
            self.add_workspace(workspace_name);
            self.workspace_name = Some(workspace_name.to_string());
        }
        Ok(())
    }

    pub fn num_workspaces(&self) -> usize {
        if let Some(workspaces) = &self.workspaces {
            workspaces.len()
        } else {
            0
        }
    }

    pub fn write_is_shallow(&self, shallow: bool) -> Result<(), OxenError> {
        let shallow_flag_path = util::fs::oxen_hidden_dir(&self.path).join(SHALLOW_FLAG);
        log::debug!("Write is shallow [{shallow}] to path: {shallow_flag_path:?}");
        if shallow {
            AtomicFile::new(&shallow_flag_path).write(b"true")?;
        } else if shallow_flag_path.exists() {
            util::fs::remove_file(&shallow_flag_path)?;
        }
        Ok(())
    }

    /// Tolerance to allow when comparing an on-disk file's mtime against a value recorded
    /// on a merkle-tree node (used by `restore`'s fast-path skip check, and available for
    /// any other caller that needs to do mtime equality against a recorded value). The
    /// working tree is always on the same filesystem as `.oxen/`, so we probe inside
    /// `.oxen/` — same filesystem, guaranteed writable for a local repo, and already
    /// hidden from `oxen status`.
    ///
    /// Returns `Duration::ZERO` if the filesystem round-trips nanosecond-precision
    /// mtimes exactly (ext4 / APFS / NTFS), `Duration::from_secs(2)` if any drift is
    /// detected (conservative upper bound covering FAT/exFAT's 2s rounding, HFS+'s 1s,
    /// coarse NFS mounts, etc. — one probe isn't authoritative about the true max drift,
    /// so we pick a safe ceiling). I/O errors also return `ZERO`.
    ///
    /// Probed at most once per repo per process; the result is memoized in a module-level
    /// `HashMap` keyed by `self.path`.
    pub async fn mtime_tolerance(&self) -> Duration {
        if let Some(&t) = MTIME_TOLERANCE_CACHE
            .lock()
            .expect("mtime tolerance cache poisoned")
            .get(&self.path)
        {
            return t;
        }
        let t = probe_mtime_drift(&self.path.join(constants::OXEN_HIDDEN_DIR)).await;
        MTIME_TOLERANCE_CACHE
            .lock()
            .expect("mtime tolerance cache poisoned")
            .insert(self.path.clone(), t);
        t
    }

    /// Compare two filesystem mtimes for equality, allowing for this repo's filesystem-rounding
    /// tolerance. Used by the merge / restore code paths so the conflict check (`should_restore_*`)
    /// agrees with `restore_file`'s fast-path skip — without this, a file whose mtime drifts
    /// inside the tolerance window on coarse-mtime mounts (FAT/exFAT, HFS+, some NFS) is treated
    /// as a no-op by one and as a unique local edit by the other, which is the bug behind a
    /// spurious `cannot_overwrite_files` from `oxen pull` after `oxen restore .`.
    pub async fn mtime_matches(&self, disk: filetime::FileTime, node: filetime::FileTime) -> bool {
        if disk == node {
            return true;
        }
        let tolerance = self.mtime_tolerance().await;
        if tolerance.is_zero() {
            return false;
        }
        let disk =
            SystemTime::UNIX_EPOCH + Duration::new(disk.unix_seconds() as u64, disk.nanoseconds());
        let node =
            SystemTime::UNIX_EPOCH + Duration::new(node.unix_seconds() as u64, node.nanoseconds());
        let diff = if disk >= node {
            disk.duration_since(node).unwrap_or_default()
        } else {
            node.duration_since(disk).unwrap_or_default()
        };
        diff <= tolerance
    }

    /// Async, tolerance-aware modification check. Routes the mtime comparison through
    /// [`Self::mtime_matches`] so callers (`oxen status`, `oxen restore`, `oxen checkout`,
    /// `oxen pull`'s overwrite check, etc.) agree with `restore_file`'s fast-path skip on
    /// coarse-mtime mounts (FAT/exFAT, HFS+, some NFS). The previous strict-mtime free
    /// functions in `util::fs` were retired once `oxen status`'s walker went async.
    pub async fn is_modified_from_node(
        &self,
        path: &Path,
        node: &FileNode,
    ) -> Result<bool, OxenError> {
        let metadata = match tokio::fs::symlink_metadata(path).await {
            Ok(m) => m,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                log::debug!("is_modified_from_node: missing path {path:?}, returning false");
                return Ok(false);
            }
            Err(err) => return Err(OxenError::file_metadata_error(path, err)),
        };
        self.is_modified_from_node_with_metadata(path, node, &metadata)
            .await
    }

    /// Pre-fetched-metadata variant of [`Self::is_modified_from_node`]. Callers that
    /// don't already have the metadata in hand should use the no-arg variant instead;
    /// this one trusts that the metadata corresponds to `path` and is up-to-date.
    pub async fn is_modified_from_node_with_metadata(
        &self,
        path: &Path,
        node: &FileNode,
        metadata: &std::fs::Metadata,
    ) -> Result<bool, OxenError> {
        let file_last_modified = filetime::FileTime::from_last_modification_time(metadata);
        let node_last_modified = util::fs::last_modified_time(
            node.last_modified_seconds(),
            node.last_modified_nanoseconds(),
        );
        let mtime_matched = self
            .mtime_matches(file_last_modified, node_last_modified)
            .await;
        util::fs::classify_modified_from_node_with_metadata(path, node, metadata, mtime_matched)
    }

    /// Override the mtime tolerance for this repo path in the per-process cache. Test-only
    /// hook for simulating coarse-mtime filesystems (FAT/HFS+/NFS) on hosts whose real FS
    /// round-trips nanosecond mtimes exactly.
    #[cfg(test)]
    pub fn set_mtime_tolerance_for_test(&self, tolerance: Duration) {
        MTIME_TOLERANCE_CACHE
            .lock()
            .expect("mtime tolerance cache poisoned")
            .insert(self.path.clone(), tolerance);
    }
}

/// Write a probe file inside `probe_dir`, set its mtime to a non-zero-nanosecond value,
/// read the mtime back, and return a conservative tolerance. See `LocalRepository::mtime_tolerance`
/// for the policy and rationale.
async fn probe_mtime_drift(probe_dir: &Path) -> Duration {
    let probe_path = probe_dir.join(".oxen-mtime-probe");
    if tokio::fs::write(&probe_path, b"").await.is_err() {
        return Duration::ZERO;
    }
    // 1970-01-01 00:00:01.123456789 — non-zero nanoseconds so any rounding is measurable.
    let target = SystemTime::UNIX_EPOCH + Duration::new(1, 123_456_789);
    let set_ok =
        filetime::set_file_mtime(&probe_path, filetime::FileTime::from_system_time(target)).is_ok();
    let drift_detected = if set_ok {
        match tokio::fs::metadata(&probe_path).await {
            Ok(meta) => meta
                .modified()
                .map(|actual| actual != target)
                .unwrap_or(false),
            Err(_) => false,
        }
    } else {
        false
    };
    let _ = tokio::fs::remove_file(&probe_path).await;
    if drift_detected {
        Duration::from_secs(2)
    } else {
        Duration::ZERO
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use filetime::FileTime;

    use crate::api::requests::RepoNew;
    use crate::config::RepositoryConfig;
    use crate::error::OxenError;
    use crate::model::LocalRepository;
    use crate::storage::{ContentFormat, StorageConfig};
    use crate::test;
    use tempfile::TempDir;

    #[test]
    fn test_new_rejects_unsupported_repo_version() {
        // A repo whose config is still pinned to a dropped format (e.g. 0.19.0) must surface a
        // clean UnsupportedRepoVersion error at load, not panic when the version is later read.
        let config = RepositoryConfig {
            min_version: Some("0.19.0".to_string()),
            ..Default::default()
        };
        let result = LocalRepository::new("unused/path", config);
        assert!(matches!(result, Err(OxenError::UnsupportedRepoVersion(_))));
    }

    #[test]
    fn test_set_block_v1_content_format_raises_min_version_fence() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|mut repo| {
            repo.set_content_format(ContentFormat::BlockV1);
            assert_eq!(repo.min_version.as_deref(), Some("0.52.0"));
            repo.save()?;

            let config = RepositoryConfig::from_repo(&repo)?;
            assert_eq!(config.min_version.as_deref(), Some("0.52.0"));
            Ok(())
        })
    }

    #[test]
    fn test_new_rejects_unfenced_block_v1_config() {
        let config = RepositoryConfig {
            storage: Some(StorageConfig {
                content_format: ContentFormat::BlockV1,
                ..Default::default()
            }),
            ..Default::default()
        };

        let result = LocalRepository::new("unused/path", config);
        let err = result.expect_err("unfenced block-v1 config was accepted");
        assert!(err.to_string().contains("block-v1 repositories require"));
    }

    #[tokio::test]
    async fn test_mtime_matches_honors_tolerance() -> Result<(), OxenError> {
        // Regression for ENG-94X: on coarse-mtime mounts (FAT/exFAT, HFS+, some NFS) a file's
        // disk mtime can drift inside the FS-rounding window relative to the value recorded on
        // a merkle node. `restore_file`'s fast path skips inside that window; the merge-side
        // conflict check (`should_restore_*`) must agree, or `oxen pull` after `oxen restore .`
        // surfaces a spurious `cannot_overwrite_files`. `mtime_matches` is the single point
        // they both consult, so unit-test it directly here.
        test::run_empty_local_repo_test_async(|repo| async move {
            let a = FileTime::from_unix_time(1_000_000, 500_000_000);
            let b = FileTime::from_unix_time(1_000_000, 500_000_000);
            let one_sec_off = FileTime::from_unix_time(1_000_001, 500_000_000);
            let three_sec_off = FileTime::from_unix_time(1_000_003, 500_000_000);

            // Default tolerance on the test host's FS is ZERO (APFS/ext4 round-trip nanos), so
            // anything but exact equality is rejected.
            repo.set_mtime_tolerance_for_test(Duration::ZERO);
            assert!(
                repo.mtime_matches(a, b).await,
                "exact equality always matches"
            );
            assert!(
                !repo.mtime_matches(a, one_sec_off).await,
                "1 s drift must not match when tolerance is zero",
            );

            // Simulate a coarse-mtime mount.
            repo.set_mtime_tolerance_for_test(Duration::from_secs(2));
            assert!(
                repo.mtime_matches(a, one_sec_off).await,
                "1 s drift is inside the 2 s tolerance window",
            );
            assert!(
                !repo.mtime_matches(a, three_sec_off).await,
                "3 s drift is outside the 2 s tolerance window",
            );

            Ok(())
        })
        .await
    }

    #[test]
    fn test_get_dirname_from_url() -> Result<(), OxenError> {
        let url = "http://0.0.0.0:3000/repositories/OxenData";
        let repo = RepoNew::from_url(url)?;
        assert_eq!(repo.name, "OxenData");
        assert_eq!(repo.namespace, "repositories");
        Ok(())
    }

    #[test]
    fn test_get_set_has_remote() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|mut local_repo| {
            let url = "http://0.0.0.0:3000/repositories/OxenData";
            let remote_name = "origin";
            local_repo.set_remote(remote_name, url);
            let remote = local_repo.get_remote(remote_name).unwrap();
            assert_eq!(remote.name, remote_name);
            assert_eq!(remote.url, url);

            Ok(())
        })
    }

    #[test]
    fn test_delete_remote() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|mut local_repo| {
            let origin_url = "http://0.0.0.0:3000/repositories/OxenData";
            let origin_name = "origin";

            let other_url = "http://0.0.0.0:4000/repositories/OxenData";
            let other_name = "other";
            local_repo.set_remote(origin_name, origin_url);
            local_repo.set_remote(other_name, other_url);

            // Remove and make sure we cannot get again
            local_repo.delete_remote(origin_name);
            let remote = local_repo.get_remote(origin_name);
            assert!(remote.is_none());

            Ok(())
        })
    }

    // Note: Adding/Setting/Deleting workspaces does not currently require the repo to be in remote mode
    // Do we want to require that?
    #[test]
    fn test_add_workspace() -> Result<(), OxenError> {
        let temp_dir = TempDir::new()?;
        let repo_path = temp_dir.path().to_path_buf();
        let mut repo = LocalRepository::new(repo_path, RepositoryConfig::default())?;

        let sample_name = "sample";
        repo.add_workspace(sample_name);

        let result = repo.has_workspace(sample_name);
        assert!(result);

        repo.set_workspace(sample_name)?;
        assert_eq!(repo.workspace_name, Some(sample_name.to_string()));

        Ok(())
    }

    #[test]
    fn test_cannot_add_repeat_workspace() -> Result<(), OxenError> {
        let temp_dir = TempDir::new()?;
        let repo_path = temp_dir.path().to_path_buf();
        let mut repo = LocalRepository::new(repo_path, RepositoryConfig::default())?;

        let sample_name = "sample";
        repo.add_workspace(sample_name);
        assert_eq!(repo.num_workspaces(), 1);

        Ok(())
    }

    #[test]
    fn test_delete_workspace() -> Result<(), OxenError> {
        let temp_dir = TempDir::new()?;
        let repo_path = temp_dir.path().to_path_buf();
        let mut repo = LocalRepository::new(repo_path, RepositoryConfig::default())?;

        let sample_name = "sample";
        repo.add_workspace(sample_name);
        repo.set_workspace(sample_name)?;

        // Cannot delete current workspace_name
        let result = repo.delete_workspace(sample_name);
        assert!(result.is_err());

        let sample_2 = "second";
        repo.add_workspace(sample_2);
        repo.set_workspace(sample_2)?;

        // Can delete previous workspace_name
        repo.delete_workspace(sample_name)?;

        Ok(())
    }

    #[test]
    fn test_storage_config_custom_path_round_trip() -> Result<(), OxenError> {
        // A custom `versions_path` survives save → from_dir. The `.oxen`-prefix join semantics
        // (relative paths get joined with the repo dir at construction time) preserve the
        // *configured* value verbatim — only the runtime path passed to `LocalVersionStore`
        // is joined.
        use crate::storage::{StorageConfig, StorageKind};
        let temp_dir = TempDir::new()?;
        let repo_path = temp_dir.path().to_path_buf();
        let custom = StorageConfig {
            kind: StorageKind::Local,
            versions_path: Some(PathBuf::from("/mnt/nfs/customer/.oxen/versions/files")),
            content_format: Default::default(),
        };
        let repo = LocalRepository::new(
            &repo_path,
            RepositoryConfig {
                storage: Some(custom.clone()),
                ..Default::default()
            },
        )?;
        repo.save()?;

        let reloaded = LocalRepository::from_dir(&repo_path)?;
        assert_eq!(reloaded.storage_config().kind, StorageKind::Local);
        assert_eq!(
            reloaded.storage_config().versions_path,
            custom.versions_path
        );

        Ok(())
    }

    #[test]
    fn test_merkle_node_backend_persists_to_config() -> Result<(), OxenError> {
        use crate::core::db::merkle_node::MerkleNodeBackend;

        // A repo with no explicit backend still records the resolved one in config.toml on save,
        // so the choice is the authoritative record on the next load.
        let temp_dir = TempDir::new()?;
        let repo_path = temp_dir.path().to_path_buf();
        let repo = LocalRepository::new(&repo_path, RepositoryConfig::default())?;
        repo.save()?;
        let on_disk = RepositoryConfig::from_file(crate::util::fs::config_filepath(&repo_path))?;
        assert!(
            on_disk.merkle_node_backend.is_some(),
            "save must persist the resolved backend to config.toml"
        );

        // An explicit backend round-trips verbatim through save → config.toml.
        let temp_dir = TempDir::new()?;
        let repo_path = temp_dir.path().to_path_buf();
        let repo = LocalRepository::new(
            &repo_path,
            RepositoryConfig {
                merkle_node_backend: Some(MerkleNodeBackend::Lmdb),
                ..Default::default()
            },
        )?;
        repo.save()?;
        let on_disk = RepositoryConfig::from_file(crate::util::fs::config_filepath(&repo_path))?;
        assert_eq!(on_disk.merkle_node_backend, Some(MerkleNodeBackend::Lmdb));

        Ok(())
    }
}
