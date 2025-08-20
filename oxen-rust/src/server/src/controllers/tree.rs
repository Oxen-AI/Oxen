use actix_web::{web, HttpRequest, HttpResponse};
use bytesize::ByteSize;
use futures_util::stream::StreamExt as _;
use liboxen::core::node_sync_status;
use liboxen::error::OxenError;
use liboxen::model::Commit;
use liboxen::model::LocalRepository;
use liboxen::view::tree::merkle_hashes::MerkleHashes;
use liboxen::view::tree::merkle_hashes::NodeHashes;
use liboxen::view::tree::MerkleHashResponse;
use liboxen::view::MerkleHashesResponse;
use liboxen::view::StatusMessage;

use std::path::PathBuf;
use std::str::FromStr;

use liboxen::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use liboxen::model::MerkleHash;
use liboxen::repositories;
use liboxen::view::tree::nodes::{
    CommitNodeResponse, DirNodeResponse, FileNodeResponse, VNodeResponse,
};

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::parse_resource;
use crate::params::TreeDepthQuery;
use crate::params::{app_data, path_param};

pub async fn get_node_by_id(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;
    let hash_str = path_param(&req, "hash")?;
    let hash = MerkleHash::from_str(&hash_str)?;

    let node =
        repositories::tree::get_node_by_id(&repository, &hash)?.ok_or(OxenHttpError::NotFound)?;

    node_to_json(node)
}

pub async fn list_missing_node_hashes(
    req: HttpRequest,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

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

pub async fn list_missing_file_hashes_from_commits(
    req: HttpRequest,
    query: web::Query<TreeDepthQuery>,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

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
    )?;
    log::debug!(
        "list_missing_file_hashes_from_commits found {} missing node ids",
        hashes.len()
    );
    Ok(HttpResponse::Ok().json(MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes,
    }))
}

pub async fn list_missing_file_hashes_from_nodes(
    req: HttpRequest,
    query: web::Query<TreeDepthQuery>,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.map_err(|_| OxenHttpError::FailedToReadRequestPayload)?);
    }

    let request: NodeHashes = serde_json::from_slice(&bytes)?;
    log::debug!(
        "list_missing_file_hashes_from_nodes checking {} commit ids with {} dirs/vnodes already found",
        request.commit_hashes.len(),
        request.dir_hashes.len(),
    );

    let commit_hashes = request.commit_hashes;
    let mut shared_hashes = request.dir_hashes;

    let subtree_paths = get_subtree_paths(&query.subtrees)?;
    let hashes = repositories::tree::list_missing_file_hashes_from_nodes(
        &repository,
        &commit_hashes,
        &mut shared_hashes,
        &subtree_paths,
        &query.depth,
    )?;
    log::debug!(
        "list_missing_file_hashes_from_nodes found {} missing node ids",
        hashes.len()
    );
    Ok(HttpResponse::Ok().json(MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes,
    }))
}

pub async fn list_missing_file_hashes(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;
    let hash_str = path_param(&req, "hash")?;
    let hash = MerkleHash::from_str(&hash_str)?;

    let hashes = repositories::tree::list_missing_file_hashes(&repository, &hash)?;
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

pub async fn mark_nodes_as_synced(
    req: HttpRequest,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("START");
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

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

pub async fn create_nodes(
    req: HttpRequest,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.map_err(|_| OxenHttpError::FailedToReadRequestPayload)?);
    }

    log::debug!(
        "create_node decompressing {} bytes",
        ByteSize::b(bytes.len() as u64)
    );

    let _hashes = repositories::tree::unpack_nodes(&repository, &bytes[..])?;

    Ok(HttpResponse::Ok().json(StatusMessage::resource_found()))
}

pub async fn download_tree(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Download the entire tree
    let buffer = repositories::tree::compress_tree(&repository)?;

    Ok(HttpResponse::Ok().body(buffer))
}

pub async fn get_node_hash_by_path(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repository)?;
    let commit = resource.commit.ok_or(OxenHttpError::NotFound)?;

    let node = repositories::tree::get_node_by_path(&repository, &commit, &resource.path)?
        .ok_or(OxenHttpError::NotFound)?;

    Ok(HttpResponse::Ok().json(MerkleHashResponse {
        status: StatusMessage::resource_found(),
        hash: node.hash,
    }))
}

pub async fn download_tree_nodes(
    req: HttpRequest,
    query: web::Query<TreeDepthQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;
    let base_head_str = path_param(&req, "base_head")?;
    let is_download = query.is_download.unwrap_or(false);

    log::debug!("download_tree_nodes for base_head: {}", base_head_str);
    log::debug!(
        "download_tree_nodes subtrees: {:?}, depth: {:?}",
        query.subtrees,
        query.depth
    );

    let (base_commit_id, maybe_head_commit_id) = maybe_parse_base_head(base_head_str)?;
    let base_commit = repositories::commits::get_by_id(&repository, &base_commit_id)?
        .ok_or(OxenError::revision_not_found(base_commit_id.into()))?;

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
            .ok_or(OxenError::resource_not_found(head_commit_id))?;
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

pub async fn download_node(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let hash_str = path_param(&req, "hash")?;
    let hash = MerkleHash::from_str(&hash_str)?;
    let repository = get_repo(&app_data.path, namespace, name)?;

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
