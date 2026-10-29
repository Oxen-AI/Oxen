use bytes::{Bytes, BytesMut};
use filetime::FileTime;
use futures::StreamExt;
use parking_lot::{Mutex, RwLockWriteGuard};
use reqwest::Client;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::header::HeaderValue;
use reqwest::redirect;
use rocksdb::DB;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Read, Write};
use std::iter::successors;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task::spawn_blocking;
use url::{Host, Url};
use zip::ZipArchive;

use crate::core;
use crate::core::staged::staged_db_manager::{
    StagedWriteBatch, get_staged_db_manager, read_from_db, staged_entries_under,
};
use crate::core::v_latest::add::{
    get_file_node, process_add_file_with_staged_db_manager, stage_version_file,
};
use crate::core::v_latest::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::file::TempFilePathNew;
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode};
use crate::model::merkle_tree::node::{MerkleTreeNode, StagedMerkleTreeNode};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::user::User;
use crate::model::workspace::Workspace;
use crate::model::{Branch, Commit, EntryDataType, MerkleHash, StagedEntryStatus};
use crate::model::{LocalRepository, NewCommitBody};
use crate::repositories;
use crate::repositories::metadata::{StoredVersion, inspect_bytes, inspect_file, inspect_stored};
use crate::storage::VersionLocation;
use crate::util;
use crate::util::fs::{LineCounts, metadata as fs_metadata};
use crate::view::ErrorFileInfo;

const BUFFER_SIZE_THRESHOLD: usize = 262144; // 256kb
const MAX_CONTENT_LENGTH: u64 = 1024 * 1024 * 1024; // 1GB limit
const MAX_DECOMPRESSED_SIZE: u64 = 1024 * 1024 * 1024; // 1GB limit
const MAX_COMPRESSION_RATIO: u64 = 100; // Maximum allowed

// TODO: Do we depreciate this, if we always upload to version store?
pub async fn add(workspace: &Workspace, filepath: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    let filepath = filepath.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let base_repo = &workspace.base_repo;

    // Stage the file using the repositories::add method
    let commit = workspace.commit.clone();
    p_add_file(base_repo, workspace_repo, &Some(commit), filepath).await?;

    // Return the relative path of the file in the workspace
    let relative_path = util::fs::path_relative_to_dir(filepath, &workspace_repo.path)?;
    Ok(relative_path)
}

/// Stage each of `paths` for removal and return the ones that could not be staged. An error on one
/// path stops the rest, leaving the paths before it staged.
pub async fn rm(workspace: &Workspace, paths: &[PathBuf]) -> Result<Vec<ErrorFileInfo>, OxenError> {
    let workspace = workspace.clone();
    let paths = paths.to_vec();
    // Staging a removal reads the committed tree (a directory's whole subtree) and writes the
    // staged db, all synchronously, so every path goes through one blocking hop off the worker.
    tokio::task::spawn_blocking(move || {
        let mut err_files = vec![];
        for path in &paths {
            err_files.extend(p_rm(&workspace, path)?);
        }
        Ok(err_files)
    })
    .await?
}

pub async fn add_version_file(
    workspace: &Workspace,
    dst_path: impl AsRef<Path>,
    file_hash: &str,
) -> Result<PathBuf, OxenError> {
    let size = workspace
        .base_repo
        .version_store()
        .get_version_size(file_hash)
        .await?;
    add_stored_version_file(workspace, dst_path.as_ref(), file_hash.parse()?, size, None).await
}

/// Stage the version file `file_hash`, `size` bytes long, which the store has, at `dst_path`,
/// reading what its metadata needs from the store unless the workspace's commit has it there.
/// `lines` are its line counts, when they were taken as it arrived.
pub(crate) async fn add_stored_version_file(
    workspace: &Workspace,
    dst_path: &Path,
    file_hash: MerkleHash,
    size: u64,
    lines: Option<LineCounts>,
) -> Result<PathBuf, OxenError> {
    let store = workspace.base_repo.version_store();
    let inspect = |dst_path| async move {
        match store.version_location(&file_hash.to_string()).await? {
            VersionLocation::Local(data_path) => {
                spawn_blocking(move || {
                    InspectedVersionFile::from_file(dst_path, file_hash, &data_path)
                })
                .await?
            }
            VersionLocation::S3 {
                url,
                region,
                endpoint_url,
            } => {
                let version = StoredVersion::new(
                    store,
                    file_hash,
                    size,
                    url,
                    &region,
                    endpoint_url.as_deref(),
                )
                .await?;
                InspectedVersionFile::from_stored(dst_path, file_hash, version, lines).await
            }
        }
    };
    let committed = CommittedFiles::load(workspace).await?;
    let file =
        VersionFileToStage::prepare_stored(&committed, dst_path.to_path_buf(), file_hash, inspect)
            .await?;
    let staged_db_manager = get_staged_db_manager(&workspace.workspace_repo)?;
    stage_version_file(
        workspace,
        file,
        &staged_db_manager,
        &Arc::new(Mutex::new(HashSet::new())),
    )
    .await?;

    Ok(dst_path.to_path_buf())
}

/// The files a workspace's commit has, for looking them up by path.
pub struct CommittedFiles {
    repo: LocalRepository,
    dir_hashes: HashMap<PathBuf, MerkleHash>,
}

impl CommittedFiles {
    /// Read the directories of `workspace`'s commit.
    pub async fn load(workspace: &Workspace) -> Result<Arc<Self>, OxenError> {
        let (repo, commit) = (workspace.base_repo.clone(), workspace.commit.clone());
        spawn_blocking(move || {
            let dir_hashes = CommitMerkleTree::dir_hashes(&repo, &commit)?;
            Ok(Arc::new(Self { repo, dir_hashes }))
        })
        .await?
    }

    /// The file the commit has at `dst_path` in the workspace.
    async fn get(files: &Arc<Self>, dst_path: &Path) -> Result<Option<FileNode>, OxenError> {
        let path = util::fs::path_relative_to_dir(dst_path, &files.repo.path)?;
        let files = Arc::clone(files);
        spawn_blocking(move || {
            let node = CommitMerkleTree::read_file(&files.repo, &files.dir_hashes, &path)?;
            Ok(node.and_then(|node| match node.node {
                EMerkleTreeNode::File(file_node) => Some(file_node),
                _ => None,
            }))
        })
        .await?
    }
}

/// A version file to stage at a path in a workspace, checked against the file the workspace's
/// commit has there.
pub enum VersionFileToStage {
    /// The commit has this content at the path.
    AlreadyCommitted { dst_path: PathBuf, hash: MerkleHash },
    /// Content the commit does not have at the path, with `committed`, the file it has there.
    Changed {
        file: InspectedVersionFile,
        committed: Option<Box<FileNode>>,
    },
}

impl VersionFileToStage {
    /// The version file `hash`, to stage at `dst_path` in the workspace whose commit `committed`
    /// holds, stored and inspected by `store_and_inspect(dst_path)` only when the commit does not
    /// have it at that path or the store has lost the commit's copy.
    pub async fn prepare<F>(
        committed: &Arc<CommittedFiles>,
        dst_path: PathBuf,
        hash: MerkleHash,
        store_and_inspect: impl FnOnce(PathBuf) -> F,
    ) -> Result<Self, OxenError>
    where
        F: Future<Output = Result<InspectedVersionFile, OxenError>>,
    {
        let node = CommittedFiles::get(committed, &dst_path).await?;
        if node.as_ref().is_some_and(|node| *node.hash() == hash) {
            // The upload restores a committed version file the store has lost.
            if !committed
                .repo
                .version_store()
                .version_exists(&hash.to_string())
                .await?
            {
                store_and_inspect(dst_path.clone()).await?;
            }
            return Ok(Self::AlreadyCommitted { dst_path, hash });
        }
        Ok(Self::Changed {
            file: store_and_inspect(dst_path).await?,
            committed: node.map(Box::new),
        })
    }

    /// The version file `hash`, which the store has, to stage at `dst_path` in the workspace whose
    /// commit `committed` holds, inspected by `inspect(dst_path)` only when the commit does not
    /// have it at that path.
    pub(crate) async fn prepare_stored<F>(
        committed: &Arc<CommittedFiles>,
        dst_path: PathBuf,
        hash: MerkleHash,
        inspect: impl FnOnce(PathBuf) -> F,
    ) -> Result<Self, OxenError>
    where
        F: Future<Output = Result<InspectedVersionFile, OxenError>>,
    {
        let node = CommittedFiles::get(committed, &dst_path).await?;
        if node.as_ref().is_some_and(|node| *node.hash() == hash) {
            return Ok(Self::AlreadyCommitted { dst_path, hash });
        }
        Ok(Self::Changed {
            file: inspect(dst_path).await?,
            committed: node.map(Box::new),
        })
    }

    /// The path in the workspace to stage it at.
    pub(crate) fn dst_path(&self) -> &Path {
        match self {
            Self::AlreadyCommitted { dst_path, .. } => dst_path,
            Self::Changed { file, .. } => &file.dst_path,
        }
    }

    /// The hash of its content.
    pub(crate) fn hash(&self) -> MerkleHash {
        match self {
            Self::AlreadyCommitted { hash, .. } => *hash,
            Self::Changed { file, .. } => file.hash,
        }
    }
}

/// A version file to stage at its destination path, with what staging records about its contents.
pub struct InspectedVersionFile {
    pub(crate) dst_path: PathBuf,
    pub(crate) hash: MerkleHash,
    pub(crate) num_bytes: u64,
    pub(crate) mtime: FileTime,
    pub(crate) mime_type: String,
    pub(crate) data_type: EntryDataType,
    pub(crate) metadata: Option<GenericMetadata>,
}

impl InspectedVersionFile {
    /// Inspect `data`, the bytes of the version file `hash`, to stage at `dst_path`. Blocks while
    /// it reads them.
    pub fn from_bytes(dst_path: PathBuf, hash: MerkleHash, data: Bytes) -> Result<Self, OxenError> {
        let num_bytes = data.len() as u64;
        let (mime_type, data_type, metadata) = inspect_bytes(data, &dst_path)?;
        Ok(Self {
            dst_path,
            hash,
            num_bytes,
            mtime: FileTime::now(),
            mime_type,
            data_type,
            metadata,
        })
    }

    /// Inspect `version`, the version file `hash` in an S3 store, to stage at `dst_path`, reading
    /// what the metadata needs from the store and its line counts from `lines` when they were taken
    /// as it arrived.
    pub(crate) async fn from_stored(
        dst_path: PathBuf,
        hash: MerkleHash,
        version: StoredVersion,
        lines: Option<LineCounts>,
    ) -> Result<Self, OxenError> {
        let num_bytes = version.size();
        let (mime_type, data_type, metadata) = inspect_stored(version, &dst_path, lines).await?;
        Ok(Self {
            dst_path,
            hash,
            num_bytes,
            mtime: FileTime::now(),
            mime_type,
            data_type,
            metadata,
        })
    }

    /// Inspect `data_path`, a local copy of the version file `hash`, to stage at `dst_path`.
    /// Blocks while it reads the file.
    pub fn from_file(
        dst_path: PathBuf,
        hash: MerkleHash,
        data_path: &Path,
    ) -> Result<Self, OxenError> {
        let file_metadata = fs_metadata(data_path)?;
        let (mime_type, data_type, metadata) = inspect_file(data_path, &dst_path)?;
        Ok(Self {
            dst_path,
            hash,
            num_bytes: file_metadata.len(),
            mtime: FileTime::from_last_modification_time(&file_metadata),
            mime_type,
            data_type,
            metadata,
        })
    }
}

/// Stage a batch of version files into the workspace. The version store must already hold every
/// file.
///
/// Returns the destination paths that staged, plus a per-file error for each that did not.
pub async fn add_version_files(
    workspace: &Workspace,
    files: Vec<VersionFileToStage>,
) -> Result<(Vec<PathBuf>, Vec<ErrorFileInfo>), OxenError> {
    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
    let staged_db_manager = get_staged_db_manager(&workspace.workspace_repo)?;

    let mut staged_paths = Vec::with_capacity(files.len());
    let mut err_files = vec![];
    for file in files {
        let (dst_path, hash) = (file.dst_path().to_path_buf(), file.hash());
        match stage_version_file(workspace, file, &staged_db_manager, &seen_dirs).await {
            Ok(_) => staged_paths.push(dst_path),
            Err(e) => {
                let error = format!("Failed to add file to staged db: {e}");
                log::error!("{error}");
                err_files.push(ErrorFileInfo {
                    hash: hash.to_string(),
                    path: Some(dst_path),
                    error,
                });
            }
        }
    }
    Ok((staged_paths, err_files))
}

pub fn track_modified_data_frame(
    workspace: &Workspace,
    filepath: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let filepath = filepath.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let base_repo = &workspace.base_repo;

    // Stage the file using the repositories::add method
    let commit = workspace.commit.clone();
    p_modify_file(base_repo, workspace_repo, &Some(commit), filepath)?;

    // Return the relative path of the file in the workspace
    let relative_path = util::fs::path_relative_to_dir(filepath, &workspace_repo.path)?;
    Ok(relative_path)
}

pub async fn remove_files_from_staged_db(
    workspace: &Workspace,
    paths: Vec<PathBuf>,
) -> Result<Vec<PathBuf>, OxenError> {
    let mut err_files = vec![];

    for path in paths {
        match unstage(workspace, &path) {
            Ok(_) => {}
            Err(e) => {
                log::debug!("Error removing file path {path:?}: {e:?}");
                err_files.push(path);
            }
        }
    }

    Ok(err_files)
}

/// Discard the workspace's staged changes for `path`, including the staged edits held in a data
/// frame's DuckDB index. Editing the path again re-indexes it from the committed file.
pub fn unstage(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let workspace_repo = &workspace.workspace_repo;
    let path = util::fs::path_relative_to_dir(path.as_ref(), &workspace_repo.path)?;
    // Drop the index first: a failure here leaves the path staged and indexed, which is coherent,
    // where dropping the entry first would leave the discarded edits waiting to be re-staged.
    unindex_if_staged_table(workspace, &path)?;
    get_staged_db_manager(workspace_repo)?.delete_entry(&path)
}

pub fn exists(workspace: &Workspace, path: impl AsRef<Path>) -> Result<bool, OxenError> {
    let workspace_repo = &workspace.workspace_repo;
    let path = util::fs::path_relative_to_dir(path.as_ref(), &workspace_repo.path)?;
    get_staged_db_manager(workspace_repo)?.exists(&path)
}

/// SSRF protection: checks whether an IP is non-globally-routable. Covers private,
/// loopback, link-local, and cloud-internal ranges (e.g. CGN used by AWS). Also handles
/// IPv6 encodings that embed IPv4 addresses (mapped, compatible, NAT64) to prevent
/// bypassing the check by encoding a private IPv4 inside an IPv6 address.
fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()               // 127.0.0.0/8
                || v4.is_private()         // 10/8, 172.16/12, 192.168/16
                || v4.is_link_local()      // 169.254/16
                || v4.is_multicast()       // 224.0.0.0/4
                || v4.is_documentation()   // 192.0.2/24, 198.51.100/24, 203.0.113/24
                || is_cgn_or_reserved_v4(v4.octets())
        }
        IpAddr::V6(v6) => {
            if v6.is_loopback() || v6.is_unspecified() || v6.is_multicast() {
                return true;
            }
            let segments = v6.segments();
            // fc00::/7 (unique local)
            if segments[0] & 0xfe00 == 0xfc00 {
                return true;
            }
            // fe80::/10 (link-local) and fec0::/10 (deprecated site-local)
            if segments[0] & 0xff80 == 0xfe80 {
                return true;
            }
            // 2001:db8::/32 (documentation)
            if segments[0] == 0x2001 && segments[1] == 0x0db8 {
                return true;
            }
            // 64:ff9b::/96 (well-known NAT64 prefix) embeds an IPv4 address in its last 32 bits,
            // which is checked like any other IPv4 address. The rest of 64:ff9b::/32, including
            // the local-use 64:ff9b:1::/48, is not globally routable.
            if segments[0] == 0x0064 && segments[1] == 0xff9b {
                return segments[2..6] != [0; 4]
                    || is_private_ip(&IpAddr::V4(embedded_v4(segments[6], segments[7])));
            }
            // 2002::/16 (6to4) embeds an IPv4 address in its second and third segments.
            if segments[0] == 0x2002 {
                return is_private_ip(&IpAddr::V4(embedded_v4(segments[1], segments[2])));
            }
            // IPv4-mapped (::ffff:x.x.x.x) and IPv4-compatible (::x.x.x.x)
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_private_ip(&IpAddr::V4(v4));
            }
            if let Some(v4) = v6.to_ipv4() {
                return is_private_ip(&IpAddr::V4(v4));
            }
            false
        }
    }
}

/// The IPv4 address carried in two segments of an IPv6 address.
fn embedded_v4(high: u16, low: u16) -> Ipv4Addr {
    Ipv4Addr::from((u32::from(high) << 16) | u32::from(low))
}

/// Additional reserved IPv4 ranges not covered by std methods
fn is_cgn_or_reserved_v4(octets: [u8; 4]) -> bool {
    // 0.0.0.0/8 ("this network", RFC 791), including the unspecified address 0.0.0.0
    if octets[0] == 0 {
        return true;
    }
    // 240.0.0.0/4 (reserved, RFC 1112), including the broadcast address 255.255.255.255
    if octets[0] >= 240 {
        return true;
    }
    // 100.64.0.0/10 — Shared/CGN (RFC 6598), used internally by cloud providers
    if octets[0] == 100 && (octets[1] & 0xC0) == 64 {
        return true;
    }
    // 192.0.0.0/24 — IETF protocol assignments (RFC 6890)
    if octets[0] == 192 && octets[1] == 0 && octets[2] == 0 {
        return true;
    }
    // 198.18.0.0/15 — Benchmarking (RFC 2544)
    if octets[0] == 198 && (octets[1] & 0xFE) == 18 {
        return true;
    }
    false
}

#[derive(Debug, thiserror::Error)]
enum InvalidUrlError {
    #[error("URL has no host")]
    NoHost,

    #[error("DNS resolution failed for {host}: {source}")]
    DnsResolutionFailed {
        host: String,
        source: std::io::Error,
    },

    #[error("URL resolves to a private/reserved IP address: {0}")]
    PrivateIp(std::net::IpAddr),
}

/// Rejects an address `is_private_ip` reports, except a loopback one when `allow_loopback` is set.
fn check_target_ip(ip: IpAddr, allow_loopback: bool) -> Result<(), InvalidUrlError> {
    if (allow_loopback && ip.is_loopback()) || !is_private_ip(&ip) {
        Ok(())
    } else {
        Err(InvalidUrlError::PrivateIp(ip))
    }
}

/// Rejects a URL with no host, or whose host is a private or reserved IP literal, which the
/// import client connects to without consulting `PublicAddrResolver`.
fn validate_url_host(url: &Url, allow_loopback: bool) -> Result<(), InvalidUrlError> {
    match url.host().ok_or(InvalidUrlError::NoHost)? {
        Host::Domain(_) => Ok(()),
        Host::Ipv4(v4) => check_target_ip(IpAddr::V4(v4), allow_loopback),
        Host::Ipv6(v6) => check_target_ip(IpAddr::V6(v6), allow_loopback),
    }
}

/// Resolves host names for the import client, refusing a name with any private or reserved address.
struct PublicAddrResolver {
    allow_loopback: bool,
}

impl Resolve for PublicAddrResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let allow_loopback = self.allow_loopback;
        Box::pin(async move {
            let host = name.as_str();
            // Port 0 is replaced with the URL's port by the client.
            let addrs: Vec<_> = tokio::net::lookup_host((host, 0))
                .await
                .map_err(|source| InvalidUrlError::DnsResolutionFailed {
                    host: host.to_string(),
                    source,
                })?
                .collect();
            for addr in &addrs {
                check_target_ip(addr.ip(), allow_loopback)?;
            }
            let addrs: Addrs = Box::new(addrs.into_iter());
            Ok(addrs)
        })
    }
}

fn parse_content_disposition_filename(header: &str) -> Option<String> {
    // Look for filename="..." or filename=...
    let lower = header.to_lowercase();
    if let Some(pos) = lower.find("filename=") {
        let rest = &header[pos + 9..];
        if let Some(rest) = rest.strip_prefix('"') {
            // filename="..."
            rest.find('"').map(|end| rest[..end].to_string())
        } else {
            // filename=... (unquoted, until semicolon or end)
            let end = rest.find(';').unwrap_or(rest.len());
            let name = rest[..end].trim();
            if name.is_empty() {
                None
            } else {
                Some(name.to_string())
            }
        }
    } else {
        None
    }
}

fn filename_from_url(url: &Url) -> Option<String> {
    url.path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|s| !s.is_empty())
        .and_then(|s| urlencoding::decode(s).ok())
        .map(|s| s.into_owned())
}

fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_whitespace() { '_' } else { c })
        .filter(|&c| c.is_alphanumeric() || c == '.' || c == '-' || c == '_')
        .collect()
}

/// Downloads a file from a user-supplied URL into a workspace directory.
/// Validates the URL scheme, and connects only to addresses that are not private or reserved.
///
/// `allow_loopback` relaxes the SSRF guard for loopback targets for tests.
pub async fn import(
    url: &str,
    auth: &str,
    directory: PathBuf,
    filename: Option<String>,
    workspace: &Workspace,
    allow_loopback: bool,
) -> Result<(), OxenError> {
    let parsed_url =
        Url::parse(url).map_err(|_| OxenError::file_import_error(format!("Invalid URL: {url}")))?;

    let auth_header_value = HeaderValue::from_str(auth)
        .map_err(|_e| OxenError::file_import_error(format!("Invalid header auth value {auth}")))?;

    fetch_file(
        &parsed_url,
        auth_header_value,
        directory,
        filename,
        workspace,
        allow_loopback,
    )
    .await?;

    Ok(())
}

pub async fn upload_zip(
    commit_message: &str,
    user: &User,
    temp_files: Vec<TempFilePathNew>,
    workspace: &Workspace,
    branch: &Branch,
) -> Result<Commit, OxenError> {
    // Unzip the files and add
    for temp_file in temp_files {
        let files = decompress_zip(&temp_file.temp_file_path)?;

        for file in files.iter() {
            // Skip files in __MACOSX directories
            if file
                .components()
                .any(|component| component.as_os_str().to_string_lossy() == "__MACOSX")
            {
                log::debug!("Skipping __MACOSX file: {file:?}");
                continue;
            }

            repositories::workspaces::files::add(workspace, file).await?;
        }
    }

    let data = NewCommitBody {
        message: commit_message.to_string(),
        author: user.name.clone(),
        email: user.email.clone(),
    };

    let res = repositories::workspaces::commit(workspace, &data, &branch.name).await;
    match res {
        Ok(commit) => {
            log::debug!("workspace::commit ✅ success! commit {commit:?}");
            Ok(commit)
        }
        workspace_behind_error @ Err(OxenError::WorkspaceBehind(_)) => {
            log::error!(
                "unable to commit branch {:?}. Workspace behind",
                branch.name
            );
            workspace_behind_error
        }
        Err(err) => {
            log::error!("unable to commit branch {:?}. Err: {}", branch.name, err);
            Err(err)
        }
    }
}

const MAX_REDIRECTS: usize = 10;

/// Fetches a file from the given URL, handling redirects manually for two reasons:
/// 1. Auth credentials are only sent on the first request, not leaked to redirect targets
///    (e.g. HuggingFace redirects to a CDN — we shouldn't send the HF token there)
/// 2. Each redirect target is validated against private/reserved IPs to prevent SSRF
///    via open redirects (an attacker's server could 302 to http://169.254.169.254/)
async fn fetch_file(
    url: &Url,
    auth_header_value: HeaderValue,
    directory: PathBuf,
    caller_filename: Option<String>,
    workspace: &Workspace,
    allow_loopback: bool,
) -> Result<(), OxenError> {
    // Every hop uses this client, which resolves each host itself rather than through a proxy.
    let client = Client::builder()
        .redirect(redirect::Policy::none())
        .dns_resolver(PublicAddrResolver { allow_loopback })
        .no_proxy()
        .build()
        .map_err(|e| OxenError::file_import_error(format!("Failed to build HTTP client: {e}")))?;

    let mut current_url = url.clone();
    let mut response = None;

    for hop in 0..=MAX_REDIRECTS {
        let scheme = current_url.scheme();
        if scheme != "http" && scheme != "https" {
            return Err(OxenError::file_import_error(
                "Only http and https URLs are allowed",
            ));
        }
        validate_url_host(&current_url, allow_loopback)
            .map_err(|e| OxenError::file_import_error(e.to_string()))?;

        let mut req = client.get(current_url.as_str());
        if hop == 0 {
            req = req.header("Authorization", auth_header_value.clone());
        }

        let resp = req.send().await.map_err(|e| {
            // A host `PublicAddrResolver` refused reaches here as the cause of a connect error.
            let refused = successors(e.source(), |&err| err.source())
                .find_map(|err| err.downcast_ref::<InvalidUrlError>());
            OxenError::file_import_error(match refused {
                Some(refused) => refused.to_string(),
                None => format!("Fetch file request failed: {e}"),
            })
        })?;

        let status = resp.status();
        if status.is_redirection() {
            if hop == MAX_REDIRECTS {
                return Err(OxenError::file_import_error("Too many redirects (max 10)"));
            }
            let location = resp
                .headers()
                .get("location")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| {
                    OxenError::file_import_error("Redirect response missing Location header")
                })?;

            // Resolve relative redirects
            let next_url = current_url
                .join(location)
                .map_err(|e| OxenError::file_import_error(format!("Invalid redirect URL: {e}")))?;

            current_url = next_url;
            continue;
        }

        if !status.is_success() {
            return Err(OxenError::file_import_error(format!(
                "HTTP request failed with status {status}"
            )));
        }

        response = Some(resp);
        break;
    }

    let response = response
        .ok_or_else(|| OxenError::file_import_error("Failed to get a successful response"))?;

    let resp_headers = response.headers();

    let content_type = resp_headers
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("application/octet-stream");

    let content_length = response.content_length();
    if let Some(content_length) = content_length
        && content_length > MAX_CONTENT_LENGTH
    {
        return Err(OxenError::file_import_error(format!(
            "Content length {content_length} exceeds maximum allowed size of 1GB"
        )));
    }

    // Resolve filename: caller-specified > Content-Disposition > URL path > UUID
    let raw_filename = caller_filename.clone().unwrap_or_else(|| {
        // caller specified
        resp_headers
            .get("content-disposition")
            .and_then(|h| h.to_str().ok())
            .and_then(parse_content_disposition_filename) // Content-Disposition
            .or_else(|| filename_from_url(&current_url)) // URL path
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()) // UUID
    });

    let filename = sanitize_filename(&raw_filename);
    if filename.is_empty() {
        return Err(OxenError::file_import_error(format!(
            "Could not determine a valid filename for {url}"
        )));
    }
    if util::fs::is_oxen_hidden_dir_name(OsStr::new(&filename)) {
        return Err(OxenError::file_import_error(format!(
            "Cannot import {url} as {filename}, which names a .oxen directory"
        )));
    }

    let is_zip = content_type.contains("zip");

    log::debug!("files::import_file Got filename : {filename:?}");

    let filepath = directory.join(&filename);
    log::debug!("files::import_file got download filepath: {filepath:?}");

    // handle download stream
    let mut stream = response.bytes_stream();
    let mut buffer = BytesMut::new();
    let mut save_path = PathBuf::new();
    let mut bytes_downloaded: u64 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| OxenError::file_import_error("Error reading file stream"))?;
        let processed_chunk = chunk.to_vec();
        buffer.extend_from_slice(&processed_chunk);
        bytes_downloaded += processed_chunk.len() as u64;

        if bytes_downloaded > MAX_CONTENT_LENGTH {
            delete_file(workspace, &filepath)?;
            return Err(OxenError::file_import_error(
                "Content length exceeds maximum allowed size of 1GB",
            ));
        }
        if buffer.len() > BUFFER_SIZE_THRESHOLD {
            save_path = save_stream(workspace, &filepath, buffer.split().freeze().to_vec())
                .await
                .map_err(|e| {
                    OxenError::file_import_error(format!(
                        "Error occurred when saving file stream: {e}"
                    ))
                })?;
        }
    }

    if !buffer.is_empty() {
        save_path = save_stream(workspace, &filepath, buffer.freeze().to_vec())
            .await
            .map_err(|e| {
                OxenError::file_import_error(format!("Error occurred when saving file stream: {e}"))
            })?;
    }
    log::debug!("workspace::files::import_file save_path is {save_path:?}");

    // check if the file size matches
    if let Some(content_length) = content_length {
        let bytes_written = if save_path.exists() {
            util::fs::metadata(&save_path)?.len()
        } else {
            0
        };

        log::debug!(
            "workspace::files::import_file has written {bytes_written:?} bytes. It's expecting {content_length:?} bytes"
        );

        if bytes_written != content_length {
            return Err(OxenError::file_import_error(
                "Content length does not match. File incomplete.",
            ));
        }
    }

    // decompress and stage file
    if is_zip {
        let files = decompress_zip(&save_path)?;
        log::debug!("workspace::files::import_file unzipped file");

        for file in files.iter() {
            log::debug!("file::import add file {file:?}");
            let path = repositories::workspaces::files::add(workspace, file).await?;
            log::debug!("file::import add file ✅ success! staged file {path:?}");
        }
    } else {
        log::debug!("file::import add file {:?}", filepath);
        let path = repositories::workspaces::files::add(workspace, &save_path).await?;
        log::debug!("file::import add file ✅ success! staged file {path:?}");
    }

    Ok(())
}

fn delete_file(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let relative_path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
    let full_path = workspace_repo.path.join(&relative_path);

    if full_path.exists() {
        std::fs::remove_file(&full_path).map_err(|e| {
            OxenError::file_import_error(format!(
                "Failed to remove file {}: {}",
                full_path.display(),
                e
            ))
        })?;
    }
    Ok(())
}

pub async fn save_stream(
    workspace: &Workspace,
    filepath: &PathBuf,
    chunk: Vec<u8>,
) -> Result<PathBuf, OxenError> {
    // This function append and save file chunk
    log::debug!(
        "liboxen::workspace::files::save_stream writing {} bytes to file",
        chunk.len()
    );

    let workspace_dir = workspace.dir();

    log::debug!("liboxen::workspace::files::save_stream Got workspace dir: {workspace_dir:?}");

    let full_dir = workspace_dir.join(filepath);

    log::debug!("liboxen::workspace::files::save_stream Got full dir: {full_dir:?}");

    if let Some(parent) = full_dir.parent() {
        std::fs::create_dir_all(parent)?;
    }

    log::debug!(
        "liboxen::workspace::files::save_stream successfully created full dir: {full_dir:?}"
    );

    let full_dir_cpy = full_dir.clone();

    let mut file = tokio::task::spawn_blocking(move || {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(full_dir_cpy)
    })
    .await
    .map_err(|e| OxenError::basic_str(format!("spawn_blocking join error: {e}")))??;

    log::debug!("liboxen::workspace::files::save_stream is writing to file: {file:?}");

    tokio::task::spawn_blocking(move || file.write_all(&chunk).map(|_| file))
        .await
        .map_err(|e| OxenError::basic_str(format!("spawn_blocking join error: {e}")))??;

    Ok(full_dir)
}

/// Extracts the archive into the directory that holds it, then removes the archive whether or not
/// extraction succeeded.
pub fn decompress_zip(zip_filepath: &PathBuf) -> Result<Vec<PathBuf>, OxenError> {
    let extracted = extract_zip(zip_filepath);
    log::debug!("files::decompress_zip removing zip file: {zip_filepath:?}");
    let removed = std::fs::remove_file(zip_filepath);
    let files = extracted?;
    removed?;
    Ok(files)
}

fn extract_zip(zip_filepath: &Path) -> Result<Vec<PathBuf>, OxenError> {
    // File unzipped into the same directory
    let mut files: Vec<PathBuf> = vec![];
    let file = File::open(zip_filepath)?;
    let mut archive = ZipArchive::new(file)
        .map_err(|e| OxenError::basic_str(format!("Failed to access zip file: {e}")))?;

    // Calculate total uncompressed size
    let mut total_size: u64 = 0;
    for i in 0..archive.len() {
        let zip_file = archive.by_index(i).map_err(|e| {
            OxenError::basic_str(format!("Failed to access zip file at index {i}: {e}"))
        })?;

        // Refuse the whole archive before anything is written if any entry would be refused.
        util::fs::normalize_relative_path(&zip_file.mangled_name())?;

        let uncompressed_size = zip_file.size();
        let compressed_size = zip_file.compressed_size();

        // Check individual file compression ratio
        if let Some(compression_ratio) = uncompressed_size.checked_div(compressed_size) {
            if compression_ratio > MAX_COMPRESSION_RATIO {
                return Err(OxenError::basic_str(format!(
                    "Suspicious zip compression ratio: {compression_ratio} detected"
                )));
            }
        } else if uncompressed_size > 0 {
            // If compressed size is 0 but uncompressed isn't, that's suspicious
            return Err(OxenError::basic_str(
                "Suspicious zip file: compressed size is 0 but uncompressed size is not",
            ));
        }
        // If both are 0, it's likely a directory entry, which is fine

        total_size += uncompressed_size;

        // Check total size limit
        if total_size > MAX_DECOMPRESSED_SIZE {
            return Err(OxenError::file_import_error(
                "Decompressed size exceeds size limit of 1GB",
            ));
        }
    }

    log::debug!("liboxen::files::decompress_zip zip filepath is {zip_filepath:?}");

    // Get the canonical (absolute) path of the parent directory
    let parent = match zip_filepath.parent() {
        Some(p) => crate::util::fs::canonicalize(p)?,
        None => std::env::current_dir()?,
    };

    // iterate thru zip archive and save the decompressed file
    for i in 0..archive.len() {
        let mut zip_file = archive.by_index(i).map_err(|e| {
            OxenError::basic_str(format!("Failed to access zip file at index {i}: {e}"))
        })?;

        let mut zipfile_name = zip_file.mangled_name();

        // Sanitize filename
        if let Some(zipfile_name_str) = zipfile_name.to_str()
            && zipfile_name_str.chars().any(|c| c.is_whitespace())
        {
            let new_name = zipfile_name_str
                .chars()
                .map(|c| if c.is_whitespace() { '_' } else { c })
                .collect::<String>();
            zipfile_name = PathBuf::from(new_name);
        }

        // Validate path components to prevent directory traversal
        let safe_path = util::fs::normalize_relative_path(&zipfile_name)?;
        let outpath = parent.join(&safe_path);

        // Verify the final path is within the parent directory
        if !outpath.starts_with(&parent) {
            return Err(OxenError::basic_str(format!(
                "Attempted path traversal detected: {outpath:?}"
            )));
        }

        log::debug!("files::decompress_zip unzipping file to: {outpath:?}");

        if let Some(outdir) = outpath.parent() {
            util::fs::create_dir_all(outdir)?;
        }

        if zip_file.is_dir() {
            util::fs::create_dir_all(&outpath)?;
        } else {
            let mut outfile = File::create(&outpath)?;
            let mut buffer = vec![0; BUFFER_SIZE_THRESHOLD];

            loop {
                let n = zip_file.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                outfile.write_all(&buffer[..n])?;
            }
        }

        files.push(outpath.clone());
    }

    Ok(files)
}

async fn p_add_file(
    base_repo: &LocalRepository,
    workspace_repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<(), OxenError> {
    let version_store = base_repo.version_store();
    let mut maybe_dir_node = None;
    if let Some(head_commit) = maybe_head_commit {
        let path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
        let parent_path = path.parent().unwrap_or(Path::new(""));
        maybe_dir_node =
            repositories::tree::get_dir_with_children(base_repo, head_commit, parent_path, None)?;
    }

    // Skip if it's not a file
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    let relative_path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
    let full_path = workspace_repo.path.join(&relative_path);
    if !full_path.is_file() {
        log::debug!("is not a file - skipping add on {full_path:?}");
        return Ok(());
    }

    // See if this is a new file or a modified file
    let mut file_status = core::v_latest::add::determine_file_status(
        base_repo,
        &maybe_dir_node,
        &file_name,
        &full_path,
    )
    .await?;

    // Store the file in the version store using the hash as the key
    let hash_str = file_status.hash.to_string();
    let file = tokio::fs::File::open(&full_path).await?;
    let size = file.metadata().await?.len();
    let reader = tokio::io::BufReader::new(file);
    version_store
        .store_version_from_reader(&hash_str, Box::new(reader), size)
        .await?;
    let conflicts: HashSet<PathBuf> = repositories::merge::list_conflicts(workspace_repo)?
        .into_iter()
        .map(|conflict| conflict.merge_entry.path)
        .collect();

    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));

    // One handle for the read and the write below, so the staged db isn't reopened per file.
    let staged_db_manager = get_staged_db_manager(workspace_repo)?;
    if let Some(metadata) =
        core::v_latest::add::staged_file_metadata(&staged_db_manager, &relative_path)?
    {
        file_status.previous_metadata = Some(metadata);
    }

    let (workspace_repo, path) = (workspace_repo.clone(), path.to_path_buf());
    tokio::task::spawn_blocking(move || {
        process_add_file_with_staged_db_manager(
            &workspace_repo,
            &workspace_repo.path,
            &file_status,
            &path,
            &seen_dirs,
            &conflicts,
        )
    })
    .await?
}

fn p_rm(workspace: &Workspace, path: &Path) -> Result<Vec<ErrorFileInfo>, OxenError> {
    log::debug!("p_rm: deleting file {path:?}");
    let base_repo = &workspace.base_repo;
    let workspace_repo = &workspace.workspace_repo;
    let commit = &workspace.commit;
    let relative_path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;

    let parent_path = path.parent().unwrap_or(Path::new(""));
    let maybe_dir_node =
        repositories::tree::get_dir_with_children(base_repo, commit, parent_path, None)?;

    let file_name = util::fs::path_relative_to_dir(path, parent_path)?;
    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
    let mut err_files: Vec<ErrorFileInfo> = vec![];
    if let Some(mut file_node) = get_file_node(&maybe_dir_node, &file_name)? {
        file_node.set_name(&path.to_string_lossy());
        err_files.extend(core::v_latest::rm::remove_file_with_db_manager(
            workspace_repo,
            &relative_path,
            &file_node,
            &seen_dirs,
        )?);
        unindex_if_staged_table(workspace, &relative_path)?;
    } else if has_dir_node(&maybe_dir_node, &file_name)? {
        if let Some(dir_node) = repositories::tree::get_dir_with_children_recursive(
            base_repo,
            commit,
            &relative_path,
            None,
        )? {
            core::v_latest::rm::remove_dir_with_db_manager(
                workspace_repo,
                &dir_node,
                &relative_path,
                &seen_dirs,
            )?;
            // remove_dir_with_db_manager stages every file under the dir for removal, so each of
            // them needs its index dropped too.
            let parent_path = relative_path.parent().unwrap_or(Path::new(""));
            let mut committed = HashSet::new();
            for (child_path, node) in dir_node.list_files_and_dirs()? {
                let child_path = parent_path.join(child_path);
                if let EMerkleTreeNode::File(_) = &node.node {
                    unindex_if_staged_table(workspace, &child_path)?;
                }
                committed.insert(child_path);
            }
            unstage_entries_under(workspace, &relative_path, &committed)?;
        };
    } else if !unstage_entries_under(workspace, &relative_path, &HashSet::new())? {
        err_files.push(ErrorFileInfo {
            hash: "".to_string(),
            path: Some(path.to_path_buf()),
            error: "Nothing is committed or staged at this path".to_string(),
        });
    }

    Ok(err_files)
}

/// Unstage every staged entry at or below `path` that is not in `committed`, then drop the
/// staged-only directory markers above `path` left with nothing under them. Returns whether
/// anything was staged at or below `path`.
fn unstage_entries_under(
    workspace: &Workspace,
    path: &Path,
    committed: &HashSet<PathBuf>,
) -> Result<bool, OxenError> {
    let staged_db_manager = get_staged_db_manager(&workspace.workspace_repo)?;
    // One guard over the reads and the write: `begin_write` doesn't exclude other writers.
    let db = staged_db_manager.lock_for_write();
    let entries = staged_entries_under(path, &db)?;
    if entries.is_empty() {
        return Ok(false);
    }
    let mut batch = StagedWriteBatch::default();
    for (entry_path, entry) in entries {
        if committed.contains(&entry_path) {
            continue;
        }
        if entry.node.is_file() {
            unindex_if_staged_table(workspace, &entry_path)?;
        }
        batch.delete_entry(&entry_path);
    }
    prune_empty_staged_dirs(workspace, &db, &mut batch, path)?;
    batch.write(&db)?;
    Ok(true)
}

/// Add to `batch` deletions of the staged directory markers above `path` that name no committed
/// directory and will have no staged entries left under them once `batch` is written, so a commit
/// does not create them as empty directories.
fn prune_empty_staged_dirs(
    workspace: &Workspace,
    db: &RwLockWriteGuard<DB>,
    batch: &mut StagedWriteBatch,
    path: &Path,
) -> Result<(), OxenError> {
    for dir in path.ancestors().skip(1) {
        if dir == Path::new("")
            || repositories::tree::has_dir(&workspace.base_repo, &workspace.commit, dir)?
            || batch
                .entries_under(db, dir)?
                .iter()
                .any(|(entry_path, _)| entry_path != dir)
        {
            break;
        }
        batch.delete_entry(dir);
    }
    Ok(())
}

/// Drop the staged DuckDB table of the data frame at `path`, if it has one. A path staged for
/// removal must not keep an editable index: a later row or column edit would re-stage the file as
/// `Modified`, which would undo the removal.
fn unindex_if_staged_table(workspace: &Workspace, path: &Path) -> Result<(), OxenError> {
    if repositories::workspaces::data_frames::has_staged_table(workspace, path)? {
        log::debug!("p_rm: dropping staged data frame index for {path:?}");
        repositories::workspaces::data_frames::unindex(workspace, path)?;
    }
    Ok(())
}

fn p_modify_file(
    base_repo: &LocalRepository,
    workspace_repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<(), OxenError> {
    let mut maybe_file_node = None;
    if let Some(head_commit) = maybe_head_commit {
        maybe_file_node = repositories::tree::get_file_by_path(base_repo, head_commit, path)?;
    }
    let Some(mut file_node) = maybe_file_node else {
        return Err(OxenError::basic_str("file not found in head commit"));
    };
    file_node.set_name(path.to_str().unwrap());
    // Stamping each edit's time changes the staged node, so a commit that read it before this edit
    // leaves the path staged for the next commit.
    let edited_at = FileTime::now();
    file_node.set_last_modified(edited_at.unix_seconds(), edited_at.nanoseconds());
    log::debug!("p_modify_file file_node: {file_node}");

    let staged_db_manager = get_staged_db_manager(workspace_repo)?;
    staged_db_manager.edit_staged_node(path, |staged| {
        // The committed node knows nothing of workspace metadata edits. When
        // the staged node holds the same content with different metadata,
        // carry that metadata over so re-staging doesn't discard it.
        if let Some(staged) = staged
            && let Ok(staged_file) = staged.node.file()
            && staged_file.hash() == file_node.hash()
            && let Some(metadata) = staged_file.metadata()
        {
            file_node.set_metadata(Some(metadata));
            file_node.recompute_metadata_hashes()?;
        }
        Ok(StagedMerkleTreeNode {
            status: StagedEntryStatus::Modified,
            node: MerkleTreeNode::from_file(file_node),
        })
    })?;
    Ok(())
}

fn has_dir_node(
    dir_node: &Option<MerkleTreeNode>,
    path: impl AsRef<Path>,
) -> Result<bool, OxenError> {
    if let Some(node) = dir_node {
        if let Some(node) = node.get_by_path(path)? {
            if let EMerkleTreeNode::Directory(_dir_node) = &node.node {
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    } else {
        Ok(false)
    }
}

/// True if `path` is a directory in the workspace: committed as one in the base commit, or holding
/// staged entries below it.
pub async fn is_dir(workspace: &Workspace, path: &Path) -> Result<bool, OxenError> {
    let workspace = workspace.clone();
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        if let Some(node) =
            repositories::tree::get_node_by_path(&workspace.base_repo, &workspace.commit, &path)?
            && node.is_dir()
        {
            return Ok(true);
        }
        Ok(get_staged_db_manager(&workspace.workspace_repo)?
            .read_entries_under(&path)?
            .iter()
            .any(|(entry_path, entry)| entry_path != &path || entry.node.is_dir()))
    })
    .await?
}

/// Move or rename a file or directory within a workspace.
/// Each moved file's old path is staged as "Removed" (or unstaged, if it exists only in the
/// workspace) and its new path as "Added". A directory moves every live file under it.
pub async fn mv(workspace: &Workspace, path: &Path, new_path: &Path) -> Result<(), OxenError> {
    let workspace = workspace.clone();
    let path = path.to_path_buf();
    let new_path = new_path.to_path_buf();
    tokio::task::spawn_blocking(move || mv_sync(&workspace, &path, &new_path)).await?
}

/// The sync core of [`mv`]: Merkle reads and staged-db writes, all under one blocking hop.
fn mv_sync(workspace: &Workspace, path: &Path, new_path: &Path) -> Result<(), OxenError> {
    if new_path.starts_with(path) {
        return Err(OxenError::MoveIntoItself(path.to_path_buf().into()));
    }

    let staged_db_manager = get_staged_db_manager(&workspace.workspace_repo)?;
    // One guard from the first read to the batch write: `begin_write` doesn't exclude other
    // writers, so a stage landing in between would be overwritten or left behind by the move.
    let db = staged_db_manager.lock_for_write();
    refuse_move_under_file(workspace, new_path, |p| read_from_db(&db, p))?;

    let staged_file = match read_from_db(&db, path)? {
        // Moving it would restore content the workspace already deleted, as edits to removed paths
        // also refuse. A committed directory the workspace removed or moved away is staged so too.
        Some(entry) if entry.status == StagedEntryStatus::Removed => {
            return Err(OxenError::PathStagedForRemoval(path.to_path_buf().into()));
        }
        Some(entry) if !entry.node.is_dir() => Some(entry.node.file()?),
        _ => None,
    };
    let source_in_base =
        repositories::tree::get_file_by_path(&workspace.base_repo, &workspace.commit, path)?;
    let source_exists_in_base = source_in_base.is_some();

    if let Some(file_node) = staged_file.or(source_in_base) {
        if read_from_db(&db, new_path)?.is_some() {
            return Err(OxenError::DestinationAlreadyStaged(
                new_path.to_path_buf().into(),
            ));
        }
        let dest_exists_in_base = repositories::tree::get_file_by_path(
            &workspace.base_repo,
            &workspace.commit,
            new_path,
        )?
        .is_some();
        let mut batch = StagedWriteBatch::default();
        mv_file_node(
            &mut batch,
            path,
            new_path,
            &file_node,
            source_exists_in_base,
            dest_exists_in_base,
        );
        return batch.write(&db);
    }

    mv_dir(workspace, &db, path, new_path)
}

/// Refuse a destination beneath a file: one the workspace stages, or one in the base commit that
/// the workspace stages nothing over. `staged` reads the workspace's staged entry at a path.
pub(super) fn refuse_move_under_file(
    workspace: &Workspace,
    new_path: &Path,
    staged: impl Fn(&Path) -> Result<Option<StagedMerkleTreeNode>, OxenError>,
) -> Result<(), OxenError> {
    for ancestor in new_path.ancestors().skip(1) {
        if ancestor.as_os_str().is_empty() {
            break;
        }
        let is_file = match staged(ancestor)? {
            Some(entry) => entry.status != StagedEntryStatus::Removed && !entry.node.is_dir(),
            None => repositories::tree::get_file_by_path(
                &workspace.base_repo,
                &workspace.commit,
                ancestor,
            )?
            .is_some(),
        };
        if is_file {
            return Err(OxenError::NotADirectory(ancestor.to_path_buf()));
        }
    }
    Ok(())
}

/// Stage the move of one file, given its resolved node (staged, or else committed). The caller
/// checks that nothing is staged at `new_path`.
fn mv_file_node(
    batch: &mut StagedWriteBatch,
    path: &Path,
    new_path: &Path,
    file_node: &FileNode,
    source_exists_in_base: bool,
    dest_exists_in_base: bool,
) {
    let mut new_file_node = file_node.clone();
    new_file_node.set_name(new_path.to_str().unwrap());
    let new_status = if dest_exists_in_base {
        StagedEntryStatus::Modified
    } else {
        StagedEntryStatus::Added
    };
    batch.upsert_file_node(new_path, new_status, &new_file_node);

    if source_exists_in_base {
        let mut removed_file_node = file_node.clone();
        removed_file_node.set_name(path.to_str().unwrap());
        batch.upsert_file_node(path, StagedEntryStatus::Removed, &removed_file_node);
        batch.add_parent_directories(path);
    } else {
        batch.delete_entry(path);
    }

    batch.add_parent_directories(new_path);
}

/// Move every live file under the directory `path` (committed in the base commit, staged in the
/// workspace, or both) to the same relative location under `new_path`.
///
/// All or nothing: validation runs before any write, data frame indexes move next (and move back
/// if one fails), and every staged-db change lands in one atomic batch. `db` is held throughout,
/// so nothing called here may take the staged db lock again.
fn mv_dir(
    workspace: &Workspace,
    db: &RwLockWriteGuard<DB>,
    path: &Path,
    new_path: &Path,
) -> Result<(), OxenError> {
    let base_repo = &workspace.base_repo;
    let commit = &workspace.commit;

    let mut committed: HashMap<PathBuf, MerkleTreeNode> = HashMap::new();
    if let Some(dir_node) =
        repositories::tree::get_dir_with_children_recursive(base_repo, commit, path, None)?
    {
        let parent_path = path.parent().unwrap_or(Path::new(""));
        for (child_path, node) in dir_node.list_files_and_dirs()? {
            committed.insert(parent_path.join(child_path), node);
        }
    }
    let staged: HashMap<PathBuf, StagedMerkleTreeNode> =
        staged_entries_under(path, db)?.into_iter().collect();

    let mut files: Vec<(PathBuf, FileNode, bool)> = vec![];
    let mut live_dirs: HashSet<PathBuf> = HashSet::new();
    for (p, node) in &committed {
        if staged
            .get(p)
            .is_some_and(|entry| entry.status == StagedEntryStatus::Removed)
        {
            continue;
        }
        if node.is_dir() {
            live_dirs.insert(p.clone());
        } else if !staged.contains_key(p) {
            files.push((p.clone(), node.file()?, true));
        }
    }
    for (p, entry) in &staged {
        if entry.status == StagedEntryStatus::Removed {
            continue;
        }
        if entry.node.is_dir() {
            live_dirs.insert(p.clone());
        } else {
            let in_base = committed.get(p).is_some_and(|node| !node.is_dir());
            files.push((p.clone(), entry.node.file()?, in_base));
        }
    }
    if files.is_empty() && live_dirs.is_empty() {
        return Err(OxenError::path_does_not_exist(path));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));

    // A committed destination staged for removal still has that staged entry, so this refuses it.
    if repositories::tree::get_node_by_path(base_repo, commit, new_path)?.is_some()
        || !staged_entries_under(new_path, db)?.is_empty()
    {
        return Err(OxenError::DestinationAlreadyStaged(
            new_path.to_path_buf().into(),
        ));
    }

    // Collecting the components drops the trailing separator `join` appends for `path` itself,
    // whose remainder is empty: a staged `new_path/` is a second spelling of the directory.
    let relocate = |p: &Path| -> Result<PathBuf, OxenError> {
        Ok(new_path.join(p.strip_prefix(path)?).components().collect())
    };

    let moves = files
        .iter()
        .map(|(p, file_node, in_base)| Ok((p.as_path(), relocate(p)?, file_node, *in_base)))
        .collect::<Result<Vec<_>, OxenError>>()?;

    let mut batch = StagedWriteBatch::default();
    for (p, dest, file_node, in_base) in &moves {
        mv_file_node(&mut batch, p, dest, file_node, *in_base, false);
    }

    // Empty directories have no file to carry their parents across.
    for dir in &live_dirs {
        batch.add_directory(&relocate(dir)?);
    }
    batch.add_parent_directories(new_path);

    // The moves above stage `Added` markers over the source directories. A committed directory
    // is staged as removed (as `rm` of a directory does), and a staged-only one is unstaged, so
    // the source does not survive the commit as an empty directory.
    for (p, entry) in &staged {
        if entry.node.is_dir() && !committed.contains_key(p) {
            batch.delete_entry(p);
        }
    }
    for (p, node) in &committed {
        if let EMerkleTreeNode::Directory(dir_node) = &node.node {
            let mut dir_node = dir_node.clone();
            dir_node.set_name(p.to_string_lossy());
            batch.upsert_staged_node(
                p,
                StagedMerkleTreeNode {
                    status: StagedEntryStatus::Removed,
                    node: MerkleTreeNode {
                        hash: node.hash,
                        node: EMerkleTreeNode::Directory(dir_node),
                        parent_id: node.parent_id,
                        children: vec![],
                    },
                },
            );
        }
    }
    if committed.is_empty() {
        prune_empty_staged_dirs(workspace, db, &mut batch, path)?;
    } else {
        batch.add_parent_directories(path);
    }

    // A data frame's staged edits live in its DuckDB index, keyed by path.
    let mut moved_indexes: Vec<(&Path, &Path)> = vec![];
    for (p, dest, _, _) in &moves {
        match core::v_latest::workspaces::data_frames::move_index(workspace, p, dest) {
            Ok(true) => moved_indexes.push((p, dest)),
            Ok(false) => {}
            Err(err) => {
                restore_indexes(workspace, &moved_indexes);
                return Err(err);
            }
        }
    }

    if let Err(err) = batch.write(db) {
        restore_indexes(workspace, &moved_indexes);
        return Err(err);
    }
    Ok(())
}

/// Best effort: move each `(source, destination)` index back to its source, newest first.
fn restore_indexes(workspace: &Workspace, moved: &[(&Path, &Path)]) {
    for (path, dest) in moved.iter().rev() {
        if let Err(err) = core::v_latest::workspaces::data_frames::move_index(workspace, dest, path)
        {
            tracing::error!(
                oxen.file_path = %path.display(),
                oxen.new_file_path = %dest.display(),
                exception.message = %err,
                "Could not move a data frame index back after a failed directory move"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use reqwest::dns::Resolve;

    use super::{PublicAddrResolver, is_private_ip};

    #[test]
    fn test_is_private_ip() {
        for ip in [
            "169.254.169.254",
            "169.254.170.2",
            "169.254.170.23",
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "100.64.0.1",
            "0.0.0.0",
            "0.1.2.3",
            "255.255.255.255",
            "240.0.0.1",
            "224.0.0.1",
            "239.255.255.250",
            "198.18.0.1",
            "192.0.0.1",
            "192.0.2.1",
            "::1",
            "::",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            "::ffff:169.254.169.254",
            "::ffff:127.0.0.1",
            "::169.254.169.254",
            "64:ff9b::a9fe:a9fe",
            "64:ff9b::0a00:0001",
            "64:ff9b:1::808:808",
            "2002:a9fe:a9fe::1",
        ] {
            let parsed = ip.parse().expect("test addresses are valid");
            assert!(is_private_ip(&parsed), "{ip} should be private/reserved");
        }
        for ip in [
            "8.8.8.8",
            "1.1.1.1",
            "100.128.0.1",
            "2606:4700:4700::1111",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2002:808:808::1",
        ] {
            let parsed = ip.parse().expect("test addresses are valid");
            assert!(!is_private_ip(&parsed), "{ip} should be public");
        }
    }

    #[tokio::test]
    async fn test_public_addr_resolver_admits_loopback_when_allowed() {
        let addrs: Vec<_> = PublicAddrResolver {
            allow_loopback: true,
        }
        .resolve("localhost".parse().expect("localhost is a valid name"))
        .await
        .expect("allow_loopback admits loopback addresses")
        .collect();
        assert!(!addrs.is_empty());
        assert!(addrs.iter().all(|addr| addr.ip().is_loopback()));
    }
}
