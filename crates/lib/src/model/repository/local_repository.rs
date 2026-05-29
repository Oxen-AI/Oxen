use crate::config::RepositoryConfig;
use crate::config::repository_config::MerkleStoreKind;
use crate::constants::SHALLOW_FLAG;
use crate::constants::{self, DEFAULT_VNODE_SIZE, MIN_OXEN_VERSION};
use crate::core::db::merkle_node::file_backend::FileBackend;
use crate::core::db::merkle_node::lmdb;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::merkle_tree::node::FileNode;
use crate::model::merkle_tree::{MerkleStore, MerkleTransport, TransportableMerkleStore};
use crate::model::{MetadataEntry, Remote, RemoteRepository};
use crate::storage::{StorageConfig, VersionStore, create_version_store};
use crate::util;
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
    vfs: Option<bool>,      // Flag for repositories stored on virtual file systems
    pub remote_mode: Option<bool>, // Flag for remote repositories
    pub workspace_name: Option<String>, // ID of the associated workspace for remote mode
    workspaces: Option<Vec<String>>, // List of workspaces for remote mode

    /// Storage backend configuration. Set once at construction and never mutated for the life
    /// of this `LocalRepository` — the source of truth for which backend `version_store` is.
    storage_config: StorageConfig,
    /// Built from `storage_config` at construction. Never replaced.
    version_store: Arc<dyn VersionStore>,

    merkle_store: Option<Arc<dyn TransportableMerkleStore>>,
    merkle_store_kind: MerkleStoreKind, // This field can be removed when merkle_store is no longer Option
}

#[derive(Debug, Clone)]
pub struct LocalRepositoryWithEntries {
    pub local_repo: LocalRepository,
    pub entries: Option<Vec<MetadataEntry>>,
}

impl LocalRepository {
    /// Create a LocalRepository from a directory
    pub fn from_dir(path: impl AsRef<Path>) -> Result<Self, OxenError> {
        let path = path.as_ref();
        let config_path = util::fs::config_filepath(path);
        let config = RepositoryConfig::from_file(&config_path)?;

        let merkle_store_kind = config.merkle_store_kind;
        let m_store = Self::load_merkle_store(
            path.to_path_buf(),
            merkle_store_kind,
            config.vfs.unwrap_or(false),
        )?;

        let storage_config = config.storage.unwrap_or_default();
        let version_store = create_version_store(path, &storage_config)?;

        Ok(LocalRepository {
            path: path.to_path_buf(),
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
            version_store,
            merkle_store: Some(m_store),
            merkle_store_kind: config.merkle_store_kind,
        })
    }

    /// Loads the Merkle store for the repository at the specified root path.
    ///
    /// Dispatches on `merkle_store_kind`:
    ///   - [`MerkleStoreKind::File`] → [`FileBackend`], honouring `is_vfs`.
    ///   - [`MerkleStoreKind::Lmdb`] → [`LmdbBackend`] via the process-wide
    ///     cache at [`lmdb::cache::get_or_open`], which serializes opens and
    ///     shares one `Arc<LmdbBackend>` across overlapping `LocalRepository`
    ///     instances for the same canonical path. Rejects `is_vfs == true`
    ///     because LMDB's memory-mapped storage isn't safe on virtual file
    ///     systems (the mmap pages may not back to a real, byte-addressable
    ///     file). VFS-on-LMDB returns [`OxenError::MerkleStoreLmdbNotSupportedOnVfs`].
    fn load_merkle_store(
        repo_path: PathBuf,
        merkle_store_kind: MerkleStoreKind,
        is_vfs: bool,
    ) -> Result<Arc<dyn TransportableMerkleStore>, OxenError> {
        let store: Arc<dyn TransportableMerkleStore> = match merkle_store_kind {
            MerkleStoreKind::File => Arc::new(FileBackend { repo_path }),
            MerkleStoreKind::Lmdb => {
                if is_vfs {
                    return Err(OxenError::MerkleStoreLmdbNotSupportedOnVfs);
                }
                lmdb::cache::get_or_open(&repo_path)?
            }
        };
        Ok(store)
    }

    /// Obtain the Merkle tree store for this repository.
    ///
    /// All operations with the repository's Merkle tree store **MUST** go through its
    /// [`MerkleStore`] implementation.
    pub fn merkle_store(&self) -> Result<&dyn MerkleStore, OxenError> {
        let store = self
            .merkle_store
            .as_ref()
            .ok_or(OxenError::MerkleStoreNotInitialized)?;
        Ok(&**store)
    }

    /// Obtain the Merkle tree node packer & unpacker for this repository's merkle store.
    ///
    /// All operations that transmit Merkle tree nodes between a client and a server **MUST**
    /// go through its [`MerkleTransport`] implementation.
    pub fn merkle_transport(&self) -> Result<&dyn MerkleTransport, OxenError> {
        let store = self
            .merkle_store
            .as_ref()
            .ok_or(OxenError::MerkleStoreNotInitialized)?;
        Ok(&**store)
    }

    /// The type of physical Merkle tree node store that this repository uses.
    pub fn merkle_store_kind(&self) -> MerkleStoreKind {
        self.merkle_store_kind
    }

    /// Get a reference to the storage configuration this repository was constructed with.
    pub fn storage_config(&self) -> &StorageConfig {
        &self.storage_config
    }

    /// Get a reference to the version store.
    pub fn version_store(&self) -> Arc<dyn VersionStore> {
        Arc::clone(&self.version_store)
    }

    /// Load a repository from the current directory
    /// this traverses up the directory tree until it finds a .oxen/ directory
    pub fn from_current_dir() -> Result<LocalRepository, OxenError> {
        let current_dir = std::env::current_dir().map_err(OxenError::from)?;
        let repo_dir = util::fs::get_repo_root_from_current_dir()
            .ok_or_else(|| OxenError::local_repo_not_found(&current_dir))?;

        LocalRepository::from_dir(&repo_dir)
    }

    /// Instantiate a new repository at a given path
    /// Note: Does not create the repository on disk, or read the config file, just instantiates the struct
    /// To load the repository, use `LocalRepository::from_dir` or `LocalRepository::from_current_dir`
    pub fn new(
        path: impl AsRef<Path>,
        storage_config: Option<StorageConfig>,
    ) -> Result<LocalRepository, OxenError> {
        Self::new_with_merkle_store_kind(path, storage_config, MerkleStoreKind::default())
    }

    /// [`Self::new`] but with an explicit [`MerkleStoreKind`] selection.
    pub fn new_with_merkle_store_kind(
        path: impl AsRef<Path>,
        storage_config: Option<StorageConfig>,
        merkle_store_kind: MerkleStoreKind,
    ) -> Result<LocalRepository, OxenError> {
        let path = path.as_ref().to_path_buf();
        let storage_config = storage_config.unwrap_or_default();
        let version_store = create_version_store(&path, &storage_config)?;
        let m_store = Self::load_merkle_store(path.clone(), merkle_store_kind, false)?;
        Ok(LocalRepository {
            path,
            remotes: vec![],
            remote_name: None,
            // New with a path should default to our current MIN_OXEN_VERSION
            min_version: Some(MIN_OXEN_VERSION.to_string()),
            vnode_size: None,
            subtree_paths: None,
            depth: None,
            vfs: None,
            remote_mode: None,
            workspace_name: None,
            workspaces: None,
            storage_config,
            version_store,
            merkle_store: Some(m_store),
            merkle_store_kind,
        })
    }

    /// Load an older version of a repository with older oxen core logic
    pub fn new_from_version(
        path: impl AsRef<Path>,
        min_version: impl AsRef<str>,
        storage_config: Option<StorageConfig>,
        is_vfs: bool,
    ) -> Result<LocalRepository, OxenError> {
        Self::new_from_version_with_merkle_store_kind(
            path,
            min_version,
            storage_config,
            is_vfs,
            MerkleStoreKind::default(),
        )
    }

    /// [`Self::new_from_version`] but with an explicit [`MerkleStoreKind`] selection.
    ///
    /// Errors with [`OxenError::MerkleStoreLmdbNotSupportedOnVfs`] when
    /// `merkle_store_kind == MerkleStoreKind::Lmdb && is_vfs`, since the LMDB
    /// backend uses memory-mapped IO that's incompatible with virtual file systems.
    pub fn new_from_version_with_merkle_store_kind(
        path: impl AsRef<Path>,
        min_version: impl AsRef<str>,
        storage_config: Option<StorageConfig>,
        is_vfs: bool,
        merkle_store_kind: MerkleStoreKind,
    ) -> Result<LocalRepository, OxenError> {
        let path = path.as_ref().to_path_buf();
        let storage_config = storage_config.unwrap_or_default();
        let version_store = create_version_store(&path, &storage_config)?;
        let m_store = Self::load_merkle_store(path.clone(), merkle_store_kind, is_vfs)?;
        Ok(LocalRepository {
            path,
            remotes: vec![],
            remote_name: None,
            min_version: Some(min_version.as_ref().to_string()),
            vnode_size: None,
            subtree_paths: None,
            depth: None,
            vfs: if is_vfs { Some(true) } else { None },
            remote_mode: None,
            workspace_name: None,
            workspaces: None,
            storage_config,
            version_store,
            merkle_store: Some(m_store),
            merkle_store_kind,
        })
    }

    pub fn from_view(view: RepositoryView) -> Result<LocalRepository, OxenError> {
        let path = std::env::current_dir()?.join(view.name);
        let storage_config = StorageConfig::default();
        let version_store = create_version_store(&path, &storage_config)?;
        let merkle_store_kind = MerkleStoreKind::default();
        let m_store = Self::load_merkle_store(path.clone(), merkle_store_kind, false)?;
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
            version_store,
            merkle_store: Some(m_store),
            merkle_store_kind,
        })
    }

    pub fn from_remote(
        repo: RemoteRepository,
        path: &Path,
        is_vfs: bool,
    ) -> Result<LocalRepository, OxenError> {
        let path = path.to_owned();
        let storage_config = StorageConfig::default();
        let version_store = create_version_store(&path, &storage_config)?;
        let merkle_store_kind = MerkleStoreKind::default();
        let m_store = Self::load_merkle_store(path.clone(), merkle_store_kind, is_vfs)?;
        Ok(LocalRepository {
            path,
            remotes: vec![repo.remote],
            remote_name: Some(String::from(constants::DEFAULT_REMOTE_NAME)),
            min_version: None,
            vnode_size: None,
            subtree_paths: None,
            depth: None,
            vfs: if is_vfs { Some(true) } else { None },
            remote_mode: None,
            workspace_name: None,
            workspaces: None,
            storage_config,
            version_store,
            merkle_store: Some(m_store),
            merkle_store_kind,
        })
    }

    pub fn min_version(&self) -> MinOxenVersion {
        match MinOxenVersion::or_earliest(self.min_version.clone()) {
            Ok(version) => version,
            Err(err) => {
                panic!("Invalid repo version\n{err}")
            }
        }
    }

    pub fn set_remote_name(&mut self, name: impl AsRef<str>) {
        self.remote_name = Some(name.as_ref().to_string());
    }

    pub fn set_min_version(&mut self, version: MinOxenVersion) {
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

    /// Save the repository configuration to disk
    pub fn save(&self) -> Result<(), OxenError> {
        let config = self.as_config();
        let config_path = util::fs::config_filepath(&self.path);
        config.save(&config_path)?;
        Ok(())
    }

    /// Re-create the repository's configuration.
    pub fn as_config(&self) -> RepositoryConfig {
        RepositoryConfig {
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
            merkle_store_kind: self.merkle_store_kind,
        }
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
            util::fs::write_to_path(&shallow_flag_path, "true")?;
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
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use filetime::FileTime;

    use crate::config::repository_config::MerkleStoreKind;
    use crate::error::OxenError;
    use crate::model::{LocalRepository, RepoNew};
    use crate::test;
    use crate::test::repo_prep::{
        init_test_repo_merkle_init_version_store_async, init_test_repo_with_merkle_store,
    };
    use tempfile::TempDir;

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
        let mut repo = LocalRepository::new(repo_path, None)?;

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
        let mut repo = LocalRepository::new(repo_path, None)?;

        let sample_name = "sample";
        repo.add_workspace(sample_name);
        assert_eq!(repo.num_workspaces(), 1);

        Ok(())
    }

    #[test]
    fn test_delete_workspace() -> Result<(), OxenError> {
        let temp_dir = TempDir::new()?;
        let repo_path = temp_dir.path().to_path_buf();
        let mut repo = LocalRepository::new(repo_path, None)?;

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
        };
        let repo = LocalRepository::new(&repo_path, Some(custom.clone()))?;
        repo.save()?;

        let reloaded = LocalRepository::from_dir(&repo_path)?;
        assert_eq!(reloaded.storage_config().kind, StorageKind::Local);
        assert_eq!(
            reloaded.storage_config().versions_path,
            custom.versions_path
        );

        Ok(())
    }

    // ────────────────────────────────────────────────────────────────────────
    // MerkleStoreKind plumbing — verify that a repo initialized with an
    // explicit kind picks up the right backend and round-trips through
    // `config.toml` on save+reload.
    // ────────────────────────────────────────────────────────────────────────

    /// Default — `LocalRepository::new` selects [`MerkleStoreKind::File`] so
    /// the existing CLI/test behaviour is unchanged.
    #[test]
    fn test_new_defaults_to_file_merkle_store() -> Result<(), OxenError> {
        let temp_dir = TempDir::new()?;
        let repo = LocalRepository::new(temp_dir.path(), None)?;
        assert_eq!(repo.merkle_store_kind(), MerkleStoreKind::File);
        Ok(())
    }

    /// Explicit constructor — passing [`MerkleStoreKind::Lmdb`] flips the
    /// in-memory field over.
    #[test]
    fn test_new_with_merkle_store_kind_picks_lmdb() -> Result<(), OxenError> {
        let temp_dir = TempDir::new()?;
        let repo = LocalRepository::new_with_merkle_store_kind(
            temp_dir.path(),
            None,
            MerkleStoreKind::Lmdb,
        )?;
        assert_eq!(repo.merkle_store_kind(), MerkleStoreKind::Lmdb);
        Ok(())
    }

    /// VFS + LMDB is rejected up-front. LMDB's memory mapping is not safe to
    /// run on virtual file systems and silently degrading would be worse than
    /// failing fast.
    #[test]
    fn test_new_with_merkle_store_kind_rejects_lmdb_on_vfs() -> Result<(), OxenError> {
        let temp_dir = TempDir::new()?;
        let result = LocalRepository::new_from_version_with_merkle_store_kind(
            temp_dir.path(),
            "0.25.0",
            None,
            true, // is_vfs
            MerkleStoreKind::Lmdb,
        );
        assert!(
            matches!(result, Err(OxenError::MerkleStoreLmdbNotSupportedOnVfs)),
            "expected MerkleStoreLmdbNotSupportedOnVfs, got {result:?}"
        );
        Ok(())
    }

    /// File backend has no such restriction.
    #[test]
    fn test_new_with_file_kind_accepts_vfs() -> Result<(), OxenError> {
        let temp_dir = TempDir::new()?;
        let repo = LocalRepository::new_from_version_with_merkle_store_kind(
            temp_dir.path(),
            "0.25.0",
            None,
            true,
            MerkleStoreKind::File,
        )?;
        assert!(repo.is_vfs());
        assert_eq!(repo.merkle_store_kind(), MerkleStoreKind::File);
        Ok(())
    }

    /// Round-trip through `config.toml`: a repo initialized with LMDB and
    /// saved must reload with the same kind via [`LocalRepository::from_dir`].
    ///
    /// LMDB enforces "one [`heed::Env`] per process per path", so we record
    /// the path, drop the original repo to close its env, then reload.
    #[test]
    fn test_lmdb_merkle_store_kind_round_trips_through_config_toml() -> Result<(), OxenError> {
        let repo = init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
        assert_eq!(repo.merkle_store_kind(), MerkleStoreKind::Lmdb);
        // Release the inner LocalRepository (closing its LMDB env) without
        // tearing down the on-disk dir, so we can reload from the same path.
        let repo_path = repo.drop_local_repo();

        let reloaded = LocalRepository::from_dir(&repo_path as &Path)?;
        assert_eq!(reloaded.merkle_store_kind(), MerkleStoreKind::Lmdb);
        Ok(())
    }

    /// LMDB backend's on-disk env subdir is created when the repo is opened
    /// with the LMDB merkle store — proves the dispatch hits the LMDB load arm.
    #[test]
    fn test_lmdb_init_creates_env_directory_under_oxen_hidden() -> Result<(), OxenError> {
        let repo = init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
        let env_dir = crate::core::db::merkle_node::lmdb::lmdb_dir_location(&repo.path);
        assert!(
            env_dir.exists(),
            "expected LMDB env dir to exist at {env_dir:?}"
        );
        Ok(())
    }

    /// End-to-end: add a file, commit, status — all the way through an
    /// LMDB-backed [`LocalRepository`]. This is the smoke test the task
    /// description calls out: "actions running on an LMDB-backed Merkle tree
    /// store using `LocalRepository` must work as expected."
    #[tokio::test]
    async fn test_lmdb_add_commit_status_roundtrip() -> Result<(), OxenError> {
        use crate::repositories;
        use crate::util;
        let repo = init_test_repo_merkle_init_version_store_async(MerkleStoreKind::Lmdb).await?;
        let text_path = repo.path.join("hello.txt");
        util::fs::write_to_path(&text_path, "Hello LMDB")?;

        repositories::add(&repo, &text_path).await?;
        repositories::commit(&repo, "Adding hello.txt")?;

        // The head commit should be queryable through the LMDB store.
        let head = repositories::commits::head_commit(&repo)?;
        let head_hash = head.hash().expect("commit hash");
        assert!(
            repo.merkle_store()?.exists(&head_hash)?,
            "head commit {head_hash} must be readable through LMDB store"
        );
        Ok(())
    }

    /// Equivalent control against [`MerkleStoreKind::File`] — confirms the
    /// add → commit → head-commit chain works the same way regardless of
    /// backend. Same shape as the LMDB test so a future failure mode that
    /// only appears for one backend stands out.
    #[tokio::test]
    async fn test_file_add_commit_status_roundtrip() -> Result<(), OxenError> {
        use crate::repositories;
        use crate::util;
        let repo = init_test_repo_merkle_init_version_store_async(MerkleStoreKind::File).await?;
        let text_path = repo.path.join("hello.txt");
        util::fs::write_to_path(&text_path, "Hello File")?;

        repositories::add(&repo, &text_path).await?;
        repositories::commit(&repo, "Adding hello.txt")?;

        let head = repositories::commits::head_commit(&repo)?;
        let head_hash = head.hash().expect("commit hash");
        assert!(
            repo.merkle_store()?.exists(&head_hash)?,
            "head commit {head_hash} must be readable through file store"
        );
        Ok(())
    }

    /// Bigger smoke test: add 5 files, commit, add another file, commit,
    /// status is clean both times, both commits are reachable from the LMDB
    /// store, and history shows both commits.
    #[tokio::test]
    async fn test_lmdb_two_commits_history() -> Result<(), OxenError> {
        use crate::repositories;
        use crate::util;
        let repo = init_test_repo_merkle_init_version_store_async(MerkleStoreKind::Lmdb).await?;
        let dir = repo.path.join("files");
        util::fs::create_dir_all(&dir)?;
        for i in 0..5 {
            util::fs::write_to_path(dir.join(format!("f{i}.txt")), format!("file {i}"))?;
        }
        repositories::add(&repo, &dir).await?;
        repositories::commit(&repo, "Add 5 files")?;

        // Status clean after first commit.
        let status1 = repositories::status(&repo).await?;
        assert!(
            status1.is_clean(),
            "expected clean status after first commit, got {status1:?}"
        );

        // Second commit on top.
        let extra = repo.path.join("README.md");
        util::fs::write_to_path(&extra, "readme")?;
        repositories::add(&repo, &extra).await?;
        repositories::commit(&repo, "Add README")?;

        let status2 = repositories::status(&repo).await?;
        assert!(
            status2.is_clean(),
            "expected clean status after second commit, got {status2:?}"
        );

        // Two commits visible in history.
        let all = repositories::commits::list_all(&repo)?;
        assert_eq!(all.len(), 2, "expected two commits, got {}", all.len());

        Ok(())
    }
}
