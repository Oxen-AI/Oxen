//! Client side of the block-dedup wire protocol: chunk negotiation, transfer-block
//! upload, and manifest publication (`oxen push` on a block-v1 repository).
//!
//! No chunk is ever re-sent to a store that has it, and block IDs never become
//! durable wire references — the server verifies each transfer block and stores it
//! as its own local block.

use bytes::Bytes;

use crate::api;
use crate::api::client;
use crate::constants;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::storage::chunked::ChunkManifest;
use crate::view::StatusMessage;
use crate::view::chunks::{
    ChunkHashesRequest, MissingChunksResponse, format_chunk_hash, parse_chunk_hash,
};

/// Hashes per negotiation request — bounds both request size and the server's
/// per-request index probing.
pub const NEGOTIATION_BATCH_SIZE: usize = 16_000;

/// A missing route means the server predates block-v1 support; surface the
/// structured upgrade error instead of a bare 404.
fn map_missing_route(status: reqwest::StatusCode) -> Option<OxenError> {
    (status == reqwest::StatusCode::NOT_FOUND).then_some(OxenError::ServerLacksBlockSupport)
}

/// The subset of `hashes` the server does not have (negotiation). Batches
/// internally; returns hashes in server-reported order.
pub async fn missing_chunks(
    remote_repo: &RemoteRepository,
    hashes: &[u128],
) -> Result<Vec<u128>, OxenError> {
    let mut missing = Vec::new();
    for batch in hashes.chunks(NEGOTIATION_BATCH_SIZE) {
        let request = ChunkHashesRequest {
            hashes: batch.iter().copied().map(format_chunk_hash).collect(),
        };
        let url = api::endpoint::url_from_repo(remote_repo, "/chunks/missing")?;
        let client = client::new_for_url(&url)?;
        let res = client.post(&url).json(&request).send().await?;
        if let Some(err) = map_missing_route(res.status()) {
            return Err(err);
        }
        let body = client::parse_json_body(&url, res).await?;
        let response: MissingChunksResponse = serde_json::from_str(&body).map_err(|err| {
            OxenError::basic_str(format!(
                "api::client::chunks::missing_chunks() Could not deserialize response [{err}]\n{body}"
            ))
        })?;
        for hash in response.missing {
            missing.push(parse_chunk_hash(&hash)?);
        }
    }
    Ok(missing)
}

/// Upload one complete transfer block. Idempotent on the server (content-addressed
/// by `block_hash`); retried with backoff on transient failures.
pub async fn upload_block(
    remote_repo: &RemoteRepository,
    block_hash: u128,
    data: Bytes,
) -> Result<(), OxenError> {
    let uri = format!("/blocks/{}", format_chunk_hash(block_hash));
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let mut last_err: Option<OxenError> = None;
    for attempt in 0..constants::max_retries() {
        let client = client::new_for_url(&url)?;
        match client.put(&url).body(data.clone()).send().await {
            Ok(res) => {
                if let Some(err) = map_missing_route(res.status()) {
                    return Err(err);
                }
                let body = client::parse_json_body(&url, res).await?;
                let response: Result<StatusMessage, serde_json::Error> =
                    serde_json::from_str(&body);
                match response {
                    Ok(_) => return Ok(()),
                    Err(err) => {
                        return Err(OxenError::basic_str(format!(
                            "api::client::chunks::upload_block() Could not deserialize response [{err}]\n{body}"
                        )));
                    }
                }
            }
            Err(err) => {
                log::warn!("upload_block attempt {attempt} failed: {err}");
                last_err = Some(err.into());
                tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt as u64 + 1)))
                    .await;
            }
        }
    }
    Err(last_err
        .unwrap_or_else(|| OxenError::basic_str("upload_block failed with no attempts made")))
}

/// Publish a manifest for a pushed version. The server validates it by full
/// streamed reconstruction before publishing, so every referenced chunk must
/// already be durable there (upload blocks first).
pub async fn upload_manifest(
    remote_repo: &RemoteRepository,
    manifest: &ChunkManifest,
) -> Result<(), OxenError> {
    let uri = format!("/manifests/{}", manifest.file_hash);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::new_for_url(&url)?;
    let res = client.put(&url).body(manifest.to_bytes()?).send().await?;
    if let Some(err) = map_missing_route(res.status()) {
        return Err(err);
    }
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(_) => Ok(()),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::chunks::upload_manifest() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}
