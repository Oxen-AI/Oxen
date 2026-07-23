use actix_web::{HttpRequest, HttpResponse, web};
use bytesize::ByteSize;
use futures_util::stream::{self, StreamExt as _};
use liboxen::core::node_sync_status;
use liboxen::core::repo_locks;
use liboxen::error::OxenError;
use liboxen::model::Commit;
use liboxen::model::LocalRepository;
use liboxen::view::MerkleHashesResponse;
use liboxen::view::StatusMessage;
use liboxen::view::tree::MerkleHashResponse;
use liboxen::view::tree::merkle_hashes::MerkleHashes;
use tokio_util::io::{ReaderStream, SyncIoBridge};

use std::io::Write;
use std::path::PathBuf;

use liboxen::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use liboxen::repositories;
use liboxen::view::tree::nodes::{
    CommitNodeResponse, DirNodeResponse, FileNodeResponse, VNodeResponse,
};

use crate::errors::OxenHttpError;
use crate::helpers::{get_repo, stream_with_heartbeat};
use crate::params::TreeDepthQuery;
use crate::params::parse_resource;
use crate::params::{app_data, path_param};

/// Duplex buffer between the blocking packer and the response body for the full-tree download.
const TREE_DOWNLOAD_BUFFER_SIZE: usize = 2 * 1024 * 1024;
/// Buffer that batches the sync packer's writes before they cross into the duplex.
const TREE_PACK_WRITE_BUFFER_SIZE: usize = 10 * 1024 * 1024;

#[tracing::instrument(skip_all)]
pub async fn get_node_by_id(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repository = get_repo(app_data, namespace, repo_name)?;
    let hash_str = path_param(&req, "hash")?.to_string();

    let node = repositories::tree::get_node_by_id(&repository, &hash_str.parse()?)?
        .ok_or(OxenHttpError::NotFound)?;

    node_to_json(node)
}

#[tracing::instrument(skip_all)]
pub async fn list_missing_node_hashes(
    req: HttpRequest,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repository = get_repo(app_data, namespace, repo_name)?;

    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.map_err(|_| OxenHttpError::FailedToReadRequestPayload)?);
    }

    let request: MerkleHashes = serde_json::from_slice(&bytes)?;
    log::debug!(
        "list_missing_node_hashes checking {} node ids",
        request.hashes.len()
    );
    let hashes = repositories::tree::list_missing_node_hashes(&repository, &request.hashes)?;
    log::debug!(
        "list_missing_node_hashes found {} missing node ids",
        hashes.len()
    );
    Ok(HttpResponse::Ok().json(MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes,
    }))
}

#[tracing::instrument(skip_all)]
pub async fn list_missing_file_hashes_from_commits(
    req: HttpRequest,
    query: web::Query<TreeDepthQuery>,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repository = get_repo(app_data, namespace, repo_name)?;

    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.map_err(|_| OxenHttpError::FailedToReadRequestPayload)?);
    }

    let request: MerkleHashes = serde_json::from_slice(&bytes)?;
    log::debug!(
        "list_missing_file_hashes_from_commits checking {} commit ids",
        request.hashes.len()
    );
    let subtree_paths = get_subtree_paths(&query.subtrees)?;
    let hashes = repositories::tree::list_missing_file_hashes_from_commits(
        &repository,
        &request.hashes,
        &subtree_paths,
        &query.depth,
    )
    .await?;
    log::debug!(
        "list_missing_file_hashes_from_commits found {} missing node ids",
        hashes.len()
    );
    Ok(HttpResponse::Ok().json(MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes,
    }))
}

#[tracing::instrument(skip_all)]
pub async fn list_missing_file_hashes(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repository = get_repo(app_data, namespace, repo_name)?;
    let hash_str = path_param(&req, "hash")?.to_string();
    let hash = hash_str.parse()?;

    let hashes = repositories::tree::list_missing_file_hashes(&repository, &hash).await?;
    log::debug!(
        "list_missing_file_hashes {} got {} hashes",
        hash,
        hashes.len()
    );
    Ok(HttpResponse::Ok().json(MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes,
    }))
}

#[tracing::instrument(skip_all)]
pub async fn mark_nodes_as_synced(
    req: HttpRequest,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repository = get_repo(app_data, namespace, repo_name)?;
    let _write = repo_locks::acquire_write(&repository)?;

    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.map_err(|_| OxenHttpError::FailedToReadRequestPayload)?);
    }

    let request: MerkleHashes = serde_json::from_slice(&bytes)?;
    let hashes = request.hashes;
    log::debug!("mark_nodes_as_synced marking {} node hashes", &hashes.len());

    for hash in &hashes {
        node_sync_status::mark_node_as_synced(&repository, hash)?;
    }

    log::debug!("successfully marked {} commit hashes", &hashes.len());
    Ok(HttpResponse::Ok().json(MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes,
    }))
}

#[tracing::instrument(skip_all)]
pub async fn create_nodes(
    req: HttpRequest,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repository = get_repo(app_data, namespace, repo_name)?;
    // Acquire before streaming so a contended write is rejected with 429 up front; the guard is
    // moved into the work future below to stay held across the deferred unpack.
    let write_guard = repo_locks::acquire_write(&repository)?;

    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.map_err(|_| OxenHttpError::FailedToReadRequestPayload)?);
    }
    let bytes = bytes.freeze();

    log::debug!("create_nodes unpacking {}", ByteSize::b(bytes.len() as u64));

    // Unpacking decompresses the archive and writes every node to the store. That is blocking CPU
    // and disk work, and it takes longer the bigger the tree is. Run it on the blocking pool so a
    // large tree never stalls the async workers that serve every other request this server handles.
    // The connection is silent for the whole unpack, so stream heartbeats to hold idle timers off.
    Ok(stream_with_heartbeat(async move {
        // Hold the write guard across the deferred unpack (the handler has already returned).
        let _write = write_guard;
        tokio::task::spawn_blocking(move || {
            repositories::tree::unpack_nodes(&repository, &bytes[..])
        })
        .await
        .map_err(|e| OxenError::basic_str(format!("create_nodes unpack task panicked: {e}")))??;
        Ok(StatusMessage::resource_found())
    }))
}

#[tracing::instrument(skip_all)]
pub async fn download_tree(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();
    let repository = get_repo(app_data, namespace, name)?;

    // Stream the entire tree tarball straight into the response body so the server never
    // buffers the whole (potentially huge) tree in memory.
    Ok(stream_tarball(move |out| {
        repositories::tree::pack_tree(&repository, out)
    }))
}

/// Stream a tar-gz produced by `pack` straight into the HTTP response body.
///
/// The packer is sync + blocking (`tar` + `flate2`), so it runs on a `spawn_blocking` worker
/// that writes into one end of a `tokio::io::duplex`; the response body reads the other end,
/// so packing and sending progress together with back-pressure and the whole tarball never
/// lives in memory at once. A large `BufWriter` batches the packer's writes across the
/// blocking boundary.
///
/// A pack failure — error or panic — becomes the stream's terminal item rather than being lost:
/// the HTTP 200 is already sent, so it truncates the body instead of changing the status, and the
/// cause is always logged.
fn stream_tarball<F>(pack: F) -> HttpResponse
where
    F: FnOnce(&mut dyn Write) -> Result<(), OxenError> + Send + 'static,
{
    let (writer, reader) = tokio::io::duplex(TREE_DOWNLOAD_BUFFER_SIZE);

    let pack_handle = tokio::task::spawn_blocking(move || {
        let mut buf_writer = std::io::BufWriter::with_capacity(
            TREE_PACK_WRITE_BUFFER_SIZE,
            SyncIoBridge::new(writer),
        );
        let result =
            pack(&mut buf_writer).and_then(|()| buf_writer.flush().map_err(OxenError::from));
        // Dropping `buf_writer` drops the bridged duplex writer, signalling EOF to the reader.
        drop(buf_writer);
        result
    });

    // After the body bytes drain (reader EOF), await the worker and surface its error — or a
    // panic — as the stream's final item. The worker has finished by then, so the await is ready.
    let body = ReaderStream::new(reader)
        .map(|chunk| chunk.map_err(OxenHttpError::from))
        .chain(stream::once(async move {
            match pack_handle.await {
                Ok(Ok(())) => Ok(web::Bytes::new()),
                Ok(Err(e)) => {
                    log::error!("stream_tarball pack failed: {e}");
                    Err(OxenHttpError::from(e))
                }
                Err(join_err) => {
                    log::error!("stream_tarball pack task panicked: {join_err}");
                    Err(OxenHttpError::InternalServerError)
                }
            }
        }));

    HttpResponse::Ok()
        .content_type("application/gzip")
        .streaming(body)
}

#[tracing::instrument(skip_all)]
pub async fn get_node_hash_by_path(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repository = get_repo(app_data, namespace, repo_name)?;
    let resource = parse_resource(&req, &repository)?;
    let commit = resource.commit.ok_or(OxenHttpError::NotFound)?;

    let node = repositories::tree::get_node_by_path(&repository, &commit, &resource.path)?
        .ok_or(OxenHttpError::NotFound)?;

    Ok(HttpResponse::Ok().json(MerkleHashResponse {
        status: StatusMessage::resource_found(),
        hash: node.hash,
    }))
}

#[tracing::instrument(skip_all)]
pub async fn download_tree_nodes(
    req: HttpRequest,
    query: web::Query<TreeDepthQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();
    let repository = get_repo(app_data, namespace, name)?;
    let base_head_str = path_param(&req, "base_head")?.to_string();
    let is_download = query.is_download.unwrap_or(false);

    log::debug!("download_tree_nodes for base_head: {base_head_str}");
    log::debug!(
        "download_tree_nodes subtrees: {:?}, depth: {:?}",
        query.subtrees,
        query.depth
    );

    let (base_commit_id, maybe_head_commit_id) = maybe_parse_base_head(base_head_str)?;
    let base_commit = repositories::commits::get_by_id(&repository, &base_commit_id)?
        .ok_or_else(|| OxenError::RevisionNotFound(base_commit_id.into()))?;

    // Parse the subtrees
    let subtrees = get_subtree_paths(&query.subtrees)?;

    // Could be a single commit or a range of commits
    let commits = get_commit_list(&repository, &base_commit, &maybe_head_commit_id, &subtrees)?;
    log::debug!("download_tree_nodes got {} commits", commits.len());

    let node_hashes = if maybe_head_commit_id.is_some() {
        // Collect the new node hashes between the base and head commits
        repositories::tree::get_node_hashes_between_commits(
            &repository,
            &commits,
            &subtrees,
            &query.depth,
            is_download,
        )?
    } else {
        // Collect all the node hashes for the commits
        repositories::tree::get_all_node_hashes_for_commits(
            &repository,
            &commits,
            &subtrees,
            &query.depth,
            is_download,
        )?
    };

    let buffer = repositories::tree::compress_nodes(&repository, &node_hashes)?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Compressed {} commits size is {}",
        commits.len(),
        ByteSize::b(total_size)
    );

    Ok(HttpResponse::Ok().body(buffer))
}

fn get_commit_list(
    repository: &LocalRepository,
    base_commit: &Commit,
    maybe_head_commit_id: &Option<String>,
    maybe_subtrees: &Option<Vec<PathBuf>>,
) -> Result<Vec<Commit>, OxenError> {
    // If we have a head commit, then we are downloading a range of commits
    // Otherwise, we are downloading all commits from the base commit back to the first commit
    // This is the difference between the first pull and subsequent pulls
    // The first pull doesn't have a head commit, but subsequent pulls do
    let mut commits = if let Some(head_commit_id) = maybe_head_commit_id {
        let head_commit = repositories::commits::get_by_id(repository, head_commit_id)?
            .ok_or_else(|| OxenError::resource_not_found(head_commit_id))?;
        repositories::commits::list_between(repository, base_commit, &head_commit)?
    } else {
        // If the subtree is specified, we only want to get the latest commit
        if maybe_subtrees.is_some() {
            vec![base_commit.clone()]
        } else {
            repositories::commits::list_from(repository, &base_commit.id)?
        }
    };

    // Reverse the list so we get the commits in *chronological* order
    commits.reverse();
    Ok(commits)
}

#[tracing::instrument(skip_all)]
pub async fn download_node(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();
    let hash_str = path_param(&req, "hash")?.to_string();
    let hash = hash_str.parse()?;
    let repository = get_repo(app_data, namespace, name)?;

    let buffer = repositories::tree::compress_node(&repository, &hash)?;

    Ok(HttpResponse::Ok().body(buffer))
}

fn node_to_json(node: MerkleTreeNode) -> actix_web::Result<HttpResponse, OxenHttpError> {
    match node.node {
        EMerkleTreeNode::File(file) => Ok(HttpResponse::Ok().json(FileNodeResponse {
            status: StatusMessage::resource_found(),
            node: file,
        })),
        EMerkleTreeNode::Directory(dir) => Ok(HttpResponse::Ok().json(DirNodeResponse {
            status: StatusMessage::resource_found(),
            node: dir,
        })),
        EMerkleTreeNode::Commit(commit) => Ok(HttpResponse::Ok().json(CommitNodeResponse {
            status: StatusMessage::resource_found(),
            node: commit,
        })),
        EMerkleTreeNode::VNode(vnode) => Ok(HttpResponse::Ok().json(VNodeResponse {
            status: StatusMessage::resource_found(),
            node: vnode,
        })),
        _ => Err(OxenHttpError::NotFound),
    }
}

/// Parses a base..head string into a base and head string
/// If the base..head string does not contain a .., then it returns the base as the base and head as None
fn maybe_parse_base_head(
    base_head: impl AsRef<str>,
) -> Result<(String, Option<String>), OxenError> {
    let base_head_str = base_head.as_ref();
    if base_head_str.contains("..") {
        let mut split = base_head_str.split("..");
        if let (Some(base), Some(head)) = (split.next(), split.next()) {
            Ok((base.to_string(), Some(head.to_string())))
        } else {
            Err(OxenError::basic_str(
                "Could not parse commits. Format should be base..head",
            ))
        }
    } else {
        Ok((base_head_str.to_string(), None))
    }
}

fn get_subtree_paths(subtrees: &Option<String>) -> Result<Option<Vec<PathBuf>>, OxenError> {
    if let Some(subtrees) = subtrees {
        Ok(Some(subtrees.split(',').map(PathBuf::from).collect()))
    } else {
        Ok(None)
    }
}
