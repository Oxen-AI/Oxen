use async_compression::futures::bufread::GzipDecoder;
use async_tar::Archive;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::TryStreamExt;
use std::collections::HashSet;
use std::time;

use crate::api::client;
use crate::constants::{NODES_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::core::v0_19_0::index::merkle_node_db::node_db_path;
use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::{Commit, LocalRepository, MerkleHash, RemoteRepository};
use crate::view::{MerkleHashesResponse, StatusMessage};
use crate::{api, repositories, util};

/// Check if a node exists in the remote repository merkle tree by hash
pub async fn has_node(
    repository: &RemoteRepository,
    node_id: MerkleHash,
) -> Result<bool, OxenError> {
    let uri = format!("/tree/nodes/{node_id}");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("api::client::tree::has_node {}", url);

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    if res.status() == 404 {
        return Ok(false);
    }

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("api::client::tree::get_by_id Got response {}", body);
    let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(_) => Ok(true),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::tree::get_by_id() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

/// Upload a node to the remote repository merkle tree
pub async fn create_node(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    node: &MerkleTreeNode,
) -> Result<(), OxenError> {
    // Compress the node
    let node_dir = node_db_path(local_repo, &node.hash);
    let tree_dir = local_repo
        .path
        .join(OXEN_HIDDEN_DIR)
        .join(TREE_DIR)
        .join(NODES_DIR);
    let sub_dir = util::fs::path_relative_to_dir(&node_dir, &tree_dir)?;
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    log::debug!(
        "create_node appending objects dir {:?} to tar at path {:?}",
        sub_dir,
        node_dir
    );
    tar.append_dir_all(sub_dir, node_dir)?;

    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;

    // Upload the node
    let uri = "/tree/nodes".to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::builder_for_url(&url)?
        .timeout(time::Duration::from_secs(120))
        .build()?;

    let size = buffer.len() as u64;
    log::debug!(
        "uploading node of size {} to {}",
        bytesize::ByteSize::b(size),
        url
    );
    let res = client.post(&url).body(buffer.to_owned()).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    log::debug!("upload node complete {}", body);

    Ok(())
}

/// Download a node from the remote repository merkle tree by hash
pub async fn download_node(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    node_id: &MerkleHash,
) -> Result<MerkleTreeNode, OxenError> {
    let node_hash_str = node_id.to_string();
    let uri = format!("/tree/nodes/{node_hash_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    log::debug!("downloading node {} from {}", node_hash_str, url);

    node_download_request(local_repo, &url).await?;

    log::debug!("unpacked node {}", node_hash_str);

    // We just downloaded, so unwrap is safe
    let node = CommitMerkleTree::read_node(local_repo, node_id, false)?.unwrap();

    log::debug!("read node {}", node);

    Ok(node)
}

/// Download a node and all its children from the remote repository merkle tree by hash
pub async fn download_node_with_children(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    node_id: &MerkleHash,
) -> Result<MerkleTreeNode, OxenError> {
    let node_hash_str = node_id.to_string();
    let uri = format!("/tree/nodes/{node_hash_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    log::debug!(
        "downloading node with children {} from {}",
        node_hash_str,
        url
    );

    node_download_request(local_repo, &url).await?;

    log::debug!("unpacked node {}", node_hash_str);

    // We just downloaded, so unwrap is safe
    let node = CommitMerkleTree::read_node(local_repo, node_id, true)?.unwrap();

    log::debug!("read node {}", node);

    Ok(node)
}

/// Downloads the full merkle tree from the remote repository
pub async fn download_tree(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
) -> Result<(), OxenError> {
    let uri = "/tree/download".to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    log::debug!("downloading tree from {}", url);

    node_download_request(local_repo, &url).await?;

    log::debug!("unpacked tree");

    Ok(())
}

/// Downloads a tree from the remote repository merkle tree by hash
pub async fn download_tree_from(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    hash: &MerkleHash,
) -> Result<MerkleTreeNode, OxenError> {
    let hash_str = hash.to_string();
    let uri = format!("/tree/download/{hash_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    log::debug!("downloading tree from {} {}", hash_str, url);

    node_download_request(local_repo, &url).await?;

    log::debug!("unpacked tree from {}", hash_str);

    // We just downloaded, so unwrap is safe
    let node = CommitMerkleTree::read_node(local_repo, hash, true)?.unwrap();

    log::debug!("read tree root from {}", node);

    Ok(node)
}

pub async fn download_commits_between(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    base_id: impl AsRef<str>,
    head_id: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    let base_id = base_id.as_ref();
    let head_id = head_id.as_ref();
    let base_head = format!("{base_id}..{head_id}");
    let uri = format!("/tree/commits/{base_head}/download");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    log::debug!("downloading commits {} from {}", base_head, url);

    node_download_request(local_repo, &url).await?;

    log::debug!("unpacked commits {}", base_head);

    // Return the commits we downloaded
    let base_commit = repositories::commits::get_by_id(local_repo, base_id)?.unwrap();
    let head_commit = repositories::commits::get_by_id(local_repo, head_id)?.unwrap();
    let commits = repositories::commits::list_between(local_repo, &base_commit, &head_commit)?;

    Ok(commits)
}

pub async fn download_commits_from(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    base_id: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    let base_id = base_id.as_ref();
    let uri = format!("/tree/commits/{base_id}/download");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    log::debug!("downloading commits {} from {}", base_id, url);

    node_download_request(local_repo, &url).await?;

    log::debug!("unpacked commits {}", base_id);

    // Return the commits we downloaded
    let commits = repositories::commits::list_from(local_repo, base_id)?;
    Ok(commits)
}

async fn node_download_request(
    local_repo: &LocalRepository,
    url: impl AsRef<str>,
) -> Result<(), OxenError> {
    let url = url.as_ref();
    let client = client::new_for_url(url)?;
    let res = client.get(url).send().await?;
    let reader = res
        .bytes_stream()
        .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
        .into_async_read();
    let decoder = GzipDecoder::new(futures::io::BufReader::new(reader));
    let archive = Archive::new(decoder);

    // The remote tar packs it in TREE_DIR/NODES_DIR
    // So this will unpack it in OXEN_HIDDEN_DIR/TREE_DIR/NODES_DIR
    let full_unpacked_path = local_repo.path.join(OXEN_HIDDEN_DIR);

    // create the temp path if it doesn't exist
    if !full_unpacked_path.exists() {
        std::fs::create_dir_all(&full_unpacked_path)?;
    }

    archive.unpack(&full_unpacked_path).await?;

    Ok(())
}

pub async fn list_missing_file_hashes(
    remote_repo: &RemoteRepository,
    node_id: &MerkleHash,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let uri = format!("/tree/nodes/{node_id}/missing_file_hashes");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<MerkleHashesResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response.hashes),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::tree::list_missing_file_hashes() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}
