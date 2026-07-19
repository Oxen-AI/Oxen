//! Server side of the block-dedup wire protocol: chunk negotiation
//! (`POST /chunks/missing`), verified transfer-block ingest
//! (`PUT /blocks/{block_hash}`), and manifest publication with full
//! reconstruction validation (`PUT /manifests/{file_hash}`).

use actix_web::{HttpRequest, HttpResponse, web};
use futures_util::stream::StreamExt as _;

use liboxen::error::OxenError;
use liboxen::storage::chunked::{ChunkManifest, ChunkedVersionStore, MAX_BLOCK_SIZE};
use liboxen::storage::{BLOCK_V1_MIN_OXEN_VERSION, VersionStore};
use liboxen::view::StatusMessage;
use liboxen::view::chunks::{
    ChunkHashesRequest, MissingChunksResponse, format_chunk_hash, parse_chunk_hash,
};

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

/// Upper bound for a manifest upload. Each chunk entry is ~28 bytes, so this
/// covers ~9.5M chunks — roughly a 600 GB file at the 64 KiB target chunk size —
/// while keeping a hostile payload from exhausting server memory.
const MAX_MANIFEST_PAYLOAD_SIZE: u64 = 256 * 1024 * 1024;

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

    // Drain the payload inline with a hard cap: the block format rejects blocks
    // over MAX_BLOCK_SIZE, but only after the bytes are resident — enforce the
    // bound before buffering.
    let mut data = web::BytesMut::new();
    while let Some(part) = body.next().await {
        let part = part.map_err(|e| OxenHttpError::BadRequest(e.to_string().into()))?;
        if (data.len() + part.len()) as u64 > MAX_BLOCK_SIZE {
            return Err(OxenHttpError::PayloadTooLarge(
                format!("block exceeds the maximum block size of {MAX_BLOCK_SIZE} bytes").into(),
            ));
        }
        data.extend_from_slice(&part);
    }

    chunked(&*store)?
        .store_block(block_hash, data.freeze())
        .await?;
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
    let mut repo = get_repo(app_data, namespace, repo_name)?;
    let store = repo.version_store();

    // Drain the payload inline with a hard cap; the manifest parser enforces its
    // own bounds, but only after the bytes are resident.
    let mut data = web::BytesMut::new();
    while let Some(part) = body.next().await {
        let part = part.map_err(|e| OxenHttpError::BadRequest(e.to_string().into()))?;
        if (data.len() + part.len()) as u64 > MAX_MANIFEST_PAYLOAD_SIZE {
            return Err(OxenHttpError::PayloadTooLarge(
                format!(
                    "manifest exceeds the maximum manifest size of {MAX_MANIFEST_PAYLOAD_SIZE} bytes"
                )
                .into(),
            ));
        }
        data.extend_from_slice(&part);
    }

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

    // Fence the repository durably before making a block-backed version
    // visible. A pre-block server must refuse to open this repository even if
    // the process is interrupted immediately after manifest publication.
    repo.set_min_version_marker(BLOCK_V1_MIN_OXEN_VERSION);
    repo.save()?;
    chunked(&*store)?.put_manifest(&manifest).await?;
    Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
}

#[cfg(test)]
mod tests {
    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;

    use actix_web::{App, web};
    use liboxen::error::OxenError;
    use liboxen::model::EntryDataType;
    use liboxen::storage::chunked::MAX_BLOCK_SIZE;
    use liboxen::util;
    use std::io::Cursor;

    /// Oversized block bodies are rejected with 413 before they are fully
    /// buffered (raw payload streaming is not covered by actix's extractor
    /// limits, so the handler enforces the bound itself).
    #[actix_web::test]
    async fn test_upload_block_rejects_oversized_body() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Oversized-Block";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let payload = vec![0u8; MAX_BLOCK_SIZE as usize + 1];
        let uri = format!("/oxen/{namespace}/{repo_name}/blocks/00000000000000000000000000000000");
        let req = actix_web::test::TestRequest::put()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .set_payload(payload)
            .to_request();
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/blocks/{block_hash}",
                    web::put().to(controllers::chunks::upload_block),
                ),
        )
        .await;
        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            actix_web::http::StatusCode::PAYLOAD_TOO_LARGE
        );

        test::cleanup_repo_and_sync_dir(repo, &sync_dir)?;
        Ok(())
    }

    /// Publishing the first server-side manifest raises the repository format
    /// fence before the manifest becomes visible to older binaries.
    #[actix_web::test]
    async fn test_upload_manifest_persists_block_v1_fence() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Manifest-Fence";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let source = test::create_local_repo(&sync_dir, namespace, "Manifest-Source")?;

        let data = b"manifest fence test bytes".to_vec();
        let hash = util::hasher::hash_buffer(&data);
        let source_store = source.version_store();
        let source_chunked = source_store
            .chunked()
            .expect("source store supports chunks");
        let manifest = source_chunked
            .store_version_chunked(
                &hash,
                None,
                &EntryDataType::Text,
                "txt",
                Box::new(Cursor::new(data)),
            )
            .await?;
        let chunk_hashes = manifest
            .chunks
            .iter()
            .map(|chunk| chunk.hash)
            .collect::<Vec<_>>();

        let store = repo.version_store();
        let destination_chunked = store.chunked().expect("destination store supports chunks");
        for block in source_chunked.pack_chunks(&chunk_hashes).await? {
            destination_chunked
                .store_block(&format!("{:032x}", block.hash), block.data)
                .await?;
        }
        assert!(destination_chunked.get_manifest(&hash).await?.is_none());

        let uri = format!("/oxen/{namespace}/{repo_name}/manifests/{hash}");
        let req = actix_web::test::TestRequest::put()
            .uri(&uri)
            .set_payload(manifest.to_bytes()?)
            .to_request();
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/manifests/{file_hash}",
                    web::put().to(controllers::chunks::upload_manifest),
                ),
        )
        .await;
        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

        let config = std::fs::read_to_string(util::fs::config_filepath(&repo.path))?;
        assert!(
            config.contains("min_version = \"0.53.0\""),
            "manifest was published without the block-v1 fence: {config}"
        );

        drop(source_store);
        drop(store);
        drop(source);
        drop(repo);
        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }
}
