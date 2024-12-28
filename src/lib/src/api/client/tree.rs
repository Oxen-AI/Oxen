use async_compression::futures::bufread::GzipDecoder;
use async_tar::Archive;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::TryStreamExt;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time;

use crate::api::client;
use crate::constants::{NODES_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::core::v0_19_0::index::merkle_node_db::node_db_path;
use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::core::v0_19_0::structs::PushProgress;
use crate::error::OxenError;
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::{Commit, LocalRepository, MerkleHash, RemoteRepository};
use crate::opts::download_tree_opts::DownloadTreeOpts;
use crate::opts::fetch_opts::FetchOpts;
use crate::view::tree::merkle_hashes::MerkleHashes;
use crate::view::tree::MerkleHashResponse;
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
pub async fn create_nodes(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    nodes: HashSet<MerkleTreeNode>,
    progress: &Arc<PushProgress>,
) -> Result<(), OxenError> {
    // Compress the node
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);
    let mut children_count = 0;
    for (i, node) in nodes.iter().enumerate() {
        let node_dir = node_db_path(local_repo, &node.hash);
        let tree_dir = local_repo
            .path
            .join(OXEN_HIDDEN_DIR)
            .join(TREE_DIR)
            .join(NODES_DIR);
        let sub_dir = util::fs::path_relative_to_dir(&node_dir, &tree_dir)?;
        log::debug!(
            "create_nodes appending objects dir {:?} to tar at path {:?}",
            sub_dir,
            node_dir
        );
        progress.set_message(format!(
            "Packing {}/{} nodes with {} children",
            i + 1,
            nodes.len(),
            children_count
        ));

        tar.append_dir_all(sub_dir, node_dir)?;
        children_count += node.children.len();
    }

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

pub async fn get_node_hash_by_path(
    remote_repo: &RemoteRepository,
    commit_id: impl AsRef<str>,
    path: PathBuf,
) -> Result<MerkleHash, OxenError> {
    let commit_id = commit_id.as_ref();
    let path_str = path.to_string_lossy();
    let uri = format!("/tree/nodes/{commit_id}/{path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let hash_response: MerkleHashResponse = serde_json::from_str(&body)?;
    Ok(hash_response.hash)
}

pub async fn download_tree_from_path(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit_id: impl AsRef<str>,
    path: impl AsRef<str>,
    is_dir: bool,
) -> Result<MerkleTreeNode, OxenError> {
    let download_tree_opts = DownloadTreeOpts {
        subtree_paths: path.as_ref().into(),
        depth: if is_dir { -1 } else { 0 },
        is_download: true,
    };
    let path: PathBuf = path.as_ref().into();
    let commit_id = commit_id.as_ref();
    let uri = append_download_tree_opts_to_uri(
        format!("/tree/download/{commit_id}"),
        &download_tree_opts,
    );
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    log::debug!("downloading trees {} from {}", commit_id, url);

    node_download_request(local_repo, &url).await?;

    if is_dir {
        let hash = get_node_hash_by_path(remote_repo, commit_id, path).await?;
        let node = CommitMerkleTree::read_node(local_repo, &hash, true)?.unwrap();
        Ok(node)
    } else {
        let parent_path = path
            .parent()
            .ok_or_else(|| OxenError::basic_str("Parent path not found"))?;
        let hash = get_node_hash_by_path(remote_repo, commit_id, parent_path.to_path_buf()).await?;
        let file_node = CommitMerkleTree::read_node(local_repo, &hash, true)?.unwrap();

        Ok(file_node)
    }
}

pub async fn download_trees_from(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit_id: impl AsRef<str>,
    fetch_opts: &FetchOpts,
) -> Result<Vec<Commit>, OxenError> {
    let commit_id = commit_id.as_ref();
    let uri = append_fetch_opts_to_uri(format!("/tree/download/{commit_id}"), fetch_opts);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    log::debug!("downloading trees {} from {}", commit_id, url);

    node_download_request(local_repo, &url).await?;

    log::debug!("unpacked trees {}", commit_id);

    // Return the commits we downloaded
    let commits = repositories::commits::list_from(local_repo, commit_id)?;

    Ok(commits)
}

fn append_fetch_opts_to_uri(uri: String, fetch_opts: &FetchOpts) -> String {
    append_subtree_paths_and_depth_to_uri(uri, &fetch_opts.subtree_paths, &fetch_opts.depth, false)
}

fn append_download_tree_opts_to_uri(uri: String, download_tree_opts: &DownloadTreeOpts) -> String {
    append_subtree_paths_and_depth_to_uri(
        uri,
        &Some(vec![download_tree_opts.subtree_paths.clone()]),
        &Some(download_tree_opts.depth),
        download_tree_opts.is_download,
    )
}

fn append_subtree_paths_and_depth_to_uri(
    uri: String,
    subtree_paths: &Option<Vec<PathBuf>>,
    depth: &Option<i32>,
    is_download: bool,
) -> String {
    let mut uri = uri;

    // Start building the query string
    let mut query_params = Vec::new();

    // Add depth parameter if it exists
    if let Some(depth_value) = depth {
        query_params.push(format!("depth={}", depth_value));
    }

    // Add is_download parameter
    if is_download {
        query_params.push("is_download=true".to_string());
    }

    // Add subtree_paths parameter if it exists
    if let Some(paths) = subtree_paths {
        let subtree_str = paths
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<String>>()
            .join(",");
        // URI encode the subtree paths
        let encoded_subtree_str = urlencoding::encode(&subtree_str);
        query_params.push(format!("subtrees={}", encoded_subtree_str));
    }

    // Construct the final URI
    if !query_params.is_empty() {
        uri = format!("{}?{}", uri, query_params.join("&"));
    }

    uri
}

pub async fn download_trees_between(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    base_id: impl AsRef<str>,
    head_id: impl AsRef<str>,
    fetch_opts: &FetchOpts,
) -> Result<Vec<Commit>, OxenError> {
    let base_id = base_id.as_ref();
    let head_id = head_id.as_ref();
    let base_head = format!("{base_id}..{head_id}");
    let uri = append_fetch_opts_to_uri(format!("/tree/download/{base_head}"), fetch_opts);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    log::debug!("downloading trees {} from {}", base_head, url);

    node_download_request(local_repo, &url).await?;

    log::debug!("unpacked trees {}", base_head);

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
    log::debug!("node_download_request about to send request {}", url);
    let res = client.get(url).send().await?;
    let reader = res
        .bytes_stream()
        .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
        .into_async_read();
    log::debug!("node_download_request about to iterate over archive");
    let decoder = GzipDecoder::new(futures::io::BufReader::new(reader));
    let archive = Archive::new(decoder);

    // The remote tar packs it in TREE_DIR/NODES_DIR
    // So this will unpack it in OXEN_HIDDEN_DIR/TREE_DIR/NODES_DIR
    let full_unpacked_path = local_repo.path.join(OXEN_HIDDEN_DIR);
    log::debug!(
        "node_download_request unpacking to {:?}",
        full_unpacked_path
    );

    // create the temp path if it doesn't exist
    if !full_unpacked_path.exists() {
        std::fs::create_dir_all(&full_unpacked_path)?;
    }

    archive.unpack(&full_unpacked_path).await?;

    Ok(())
}

pub async fn list_missing_node_hashes(
    remote_repo: &RemoteRepository,
    node_ids: HashSet<MerkleHash>,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let uri = "/tree/nodes/missing_node_hashes".to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::new_for_url(&url)?;
    let node_hashes = MerkleHashes { hashes: node_ids };
    let res = client.post(&url).json(&node_hashes).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<MerkleHashesResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response.hashes),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::tree::list_missing_file_hashes() Could not deserialize response [{err}]\n{body}"
        ))),
    }
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

pub async fn list_missing_file_hashes_from_commits(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit_ids: HashSet<MerkleHash>,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let uri = "/tree/nodes/missing_file_hashes_from_commits".to_string();
    let uri = append_subtree_paths_and_depth_to_uri(
        uri,
        &local_repo.subtree_paths(),
        &local_repo.depth(),
        false,
    );
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::new_for_url(&url)?;
    let commit_hashes = MerkleHashes { hashes: commit_ids };
    let res = client.post(&url).json(&commit_hashes).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<MerkleHashesResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response.hashes),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::tree::list_missing_file_hashes_from_commits() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::error::OxenError;
    use crate::model::MerkleHash;
    use crate::opts::FetchOpts;
    use crate::repositories;
    use crate::test;
    use std::fs;

    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_has_node() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|local_repo, remote_repo| async move {
            let commit = repositories::commits::head_commit(&local_repo)?;
            let commit_hash = MerkleHash::from_str(&commit.id)?;
            let has_node = api::client::tree::has_node(&remote_repo, commit_hash).await?;
            assert!(has_node);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_download_tree_from_path() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let commit = repositories::commits::head_commit(&local_repo)?;
            let remote_repo_clone = remote_repo.clone();
            let download_repo_path_1 = local_repo.path.join("download_repo_test_1");
            let download_repo_path_2 = local_repo.path.join("download_repo_test_2");
            let download_local_repo_1 = repositories::init(&download_repo_path_1)?;
            let download_local_repo_2 = repositories::init(&download_repo_path_2)?;
            api::client::tree::download_tree_from_path(
                &download_local_repo_1,
                &remote_repo_clone,
                &commit.id,
                "",
                true,
            )
            .await?;

            let dir_path = download_local_repo_1.path.join(".oxen/tree/nodes");
            let entries = fs::read_dir(&dir_path)?;
            let dir_count = entries
                .filter_map(|entry| match entry {
                    Ok(e) => {
                        if let Ok(file_type) = e.file_type() {
                            if file_type.is_dir() {
                                return Some(1);
                            }
                        }
                        None
                    }
                    Err(_) => None,
                })
                .count();

            assert!(dir_count > 16);

            api::client::tree::download_tree_from_path(
                &download_local_repo_2,
                &remote_repo_clone,
                &commit.id,
                "annotations/test",
                true,
            )
            .await?;

            let dir_path = download_local_repo_2.path.join(".oxen/tree/nodes");
            let entries = fs::read_dir(&dir_path)?;
            let dir_count = entries
                .filter_map(|entry| match entry {
                    Ok(e) => {
                        if let Ok(file_type) = e.file_type() {
                            if file_type.is_dir() {
                                return Some(1);
                            }
                        }
                        None
                    }
                    Err(_) => None,
                })
                .count();

            // Here we expect very few nodes (but more than on the download_tree_from_path function) because we only download the nodes that actually
            // contain the files in the subtree path, no parents, no siblings.
            assert!(dir_count < 4);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_download_trees_from() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let commit = repositories::commits::head_commit(&local_repo)?;
            let remote_repo_clone = remote_repo.clone();
            let download_repo_path = local_repo.path.join("download_repo_test_1");
            let download_local_repo = repositories::init(&download_repo_path)?;
            let fetch_opts = FetchOpts {
                subtree_paths: None,
                depth: None,
                all: false,
                remote: remote_repo_clone.url().to_string(),
                branch: "main".to_string(),
            };
            api::client::tree::download_trees_from(
                &download_local_repo,
                &remote_repo_clone,
                &commit.id,
                &fetch_opts,
            )
            .await?;

            let dir_path = download_local_repo.path.join(".oxen/tree/nodes");
            let entries = fs::read_dir(&dir_path)?;
            let dir_count = entries
                .filter_map(|entry| match entry {
                    Ok(e) => {
                        if let Ok(file_type) = e.file_type() {
                            if file_type.is_dir() {
                                return Some(1);
                            }
                        }
                        None
                    }
                    Err(_) => None,
                })
                .count();

            assert!(dir_count > 33);

            let download_repo_path_2 = local_repo.path.join("download_repo_test_2");
            let download_local_repo_2 = repositories::init(&download_repo_path_2)?;
            let fetch_opts = FetchOpts {
                subtree_paths: Some(vec![PathBuf::from("annotations/test")]),
                depth: Some(1),
                all: false,
                remote: remote_repo_clone.url().to_string(),
                branch: "main".to_string(),
            };
            api::client::tree::download_trees_from(
                &download_local_repo_2,
                &remote_repo_clone,
                &commit.id,
                &fetch_opts,
            )
            .await?;

            let dir_path = download_local_repo_2.path.join(".oxen/tree/nodes");
            let entries = fs::read_dir(&dir_path)?;
            let dir_count = entries
                .filter_map(|entry| match entry {
                    Ok(e) => {
                        if let Ok(file_type) = e.file_type() {
                            if file_type.is_dir() {
                                return Some(1);
                            }
                        }
                        None
                    }
                    Err(_) => None,
                })
                .count();

            // Here we expect few nodes (but more than on the download_tree_from_path function) because we download the nodes that
            // contain the files in the subtree path, with the parents. This allows us to keep the remote repo intact when we push
            // back the changes applied to this subset of files.
            assert!(dir_count < 8);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_list_missing_node_hashes() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|local_repo, remote_repo| async move {
            let commit = repositories::commits::head_commit(&local_repo)?;
            let commit_hash = MerkleHash::from_str(&commit.id)?;
            let missing_node_hashes = api::client::tree::list_missing_node_hashes(
                &remote_repo,
                HashSet::from([commit_hash]),
            )
            .await?;
            assert_eq!(missing_node_hashes.len(), 0);

            // Add and commit a new file
            let file_path = local_repo.path.join("test.txt");
            let file_path = test::write_txt_file_to_path(file_path, "image,label\n1,2\n3,4\n5,6")?;
            repositories::add(&local_repo, &file_path)?;
            let commit = repositories::commit(&local_repo, "test")?;
            let commit_hash = MerkleHash::from_str(&commit.id)?;

            let missing_node_hashes = api::client::tree::list_missing_node_hashes(
                &remote_repo,
                HashSet::from([commit_hash]),
            )
            .await?;
            assert_eq!(missing_node_hashes.len(), 1);
            assert!(missing_node_hashes.contains(&commit_hash));

            Ok(remote_repo)
        })
        .await
    }
}
