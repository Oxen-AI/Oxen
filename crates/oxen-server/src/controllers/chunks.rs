//! Server side of the block-dedup wire protocol: chunk negotiation
//! (`POST /chunks/missing`), verified transfer-block ingest
//! (`PUT /blocks/{block_hash}`), and manifest publication with full
//! reconstruction validation (`PUT /manifests/{file_hash}`).

use actix_web::{HttpRequest, HttpResponse, web};
use futures_util::stream::StreamExt as _;

use liboxen::error::OxenError;
use liboxen::storage::VersionStore;
use liboxen::storage::chunked::{ChunkManifest, ChunkedVersionStore};
use liboxen::view::StatusMessage;
use liboxen::view::chunks::{
    ChunkHashesRequest, MissingChunksResponse, format_chunk_hash, parse_chunk_hash,
};

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

/// The repo's chunked-storage capability, or a clear error for backends that
/// don't support block storage (the route existing means the *server* does).
fn chunked(store: &dyn VersionStore) -> Result<&dyn ChunkedVersionStore, OxenHttpError> {
    store.chunked().ok_or_else(|| {
        OxenHttpError::BadRequest(
            "this repository's storage backend does not support block storage"
                .to_string()
                .into(),
        )
    })
}

/// Drain a request payload into one buffer (blocks are ≤ 64 MiB by format;
/// manifests are a few MB for multi-GB files — the parsers enforce their bounds).
async fn read_payload(body: &mut web::Payload) -> Result<web::Bytes, OxenHttpError> {
    let mut data = web::BytesMut::new();
    while let Some(part) = body.next().await {
        let part = part.map_err(|e| OxenHttpError::BadRequest(e.to_string().into()))?;
        data.extend_from_slice(&part);
    }
    Ok(data.freeze())
}

/// `POST /chunks/missing` — the subset of the posted chunk hashes this repo's
/// store does not have (push negotiation).
pub async fn missing(
    req: HttpRequest,
    body: web::Json<ChunkHashesRequest>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(app_data, namespace, repo_name)?;
    let store = repo.version_store();

    let hashes = body
        .hashes
        .iter()
        .map(|h| parse_chunk_hash(h))
        .collect::<Result<Vec<_>, OxenError>>()?;
    let missing = chunked(&*store)?.missing_chunks(&hashes).await?;

    Ok(HttpResponse::Ok().json(MissingChunksResponse {
        status: StatusMessage::resource_found(),
        missing: missing.into_iter().map(format_chunk_hash).collect(),
    }))
}

/// `PUT /blocks/{block_hash}` — verified transfer-block ingest: the block's bytes
/// must hash to `block_hash`, and every chunk is decoded and checked against the
/// footer's claims before anything is indexed. Idempotent.
pub async fn upload_block(
    req: HttpRequest,
    mut body: web::Payload,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let block_hash = path_param(&req, "block_hash")?;
    let repo = get_repo(app_data, namespace, repo_name)?;
    let store = repo.version_store();

    let data = read_payload(&mut body).await?;
    chunked(&*store)?.store_block(block_hash, data).await?;
    Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
}

/// `PUT /manifests/{file_hash}` — validate and publish a manifest for a pushed
/// version. Validation is full streamed reconstruction against the claimed file
/// hash (every referenced chunk must already be durable here); a manifest already
/// published for this hash is a no-op success and is never overwritten.
pub async fn upload_manifest(
    req: HttpRequest,
    mut body: web::Payload,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let file_hash = path_param(&req, "file_hash")?;
    let repo = get_repo(app_data, namespace, repo_name)?;
    let store = repo.version_store();

    let data = read_payload(&mut body).await?;
    let manifest = ChunkManifest::from_bytes(&data).map_err(OxenError::from)?;
    if manifest.file_hash.to_string() != file_hash {
        return Err(OxenHttpError::BadRequest(
            format!(
                "manifest file hash {} does not match path {file_hash}",
                manifest.file_hash
            )
            .into(),
        ));
    }

    chunked(&*store)?.put_manifest(&manifest).await?;
    Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
}
