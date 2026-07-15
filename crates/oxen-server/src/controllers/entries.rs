use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{PageNumQuery, app_data, parse_resource, path_param};

use liboxen::constants::stream_segment_size;
use liboxen::error::OxenError;
use liboxen::util::fs::replace_file_name_keep_extension;
use liboxen::util::paginate;
use liboxen::view::StatusMessage;
use liboxen::view::entries::{PaginatedMetadataEntries, PaginatedMetadataEntriesResponse};
use liboxen::{constants, current_function, repositories};

use actix_web::{HttpRequest, HttpResponse, web};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use futures_util::stream::StreamExt as _;
use serde::Deserialize;

use std::io::prelude::*;

#[derive(Deserialize, Debug)]
pub struct ChunkQuery {
    pub chunk_start: Option<u64>,
    pub chunk_size: Option<u64>,
}

// Deprecated. Only kept to support older clients before v0.37.2
pub async fn download_data_from_version_paths(
    req: HttpRequest,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repo = get_repo(app_data, namespace, &repo_name)?;

    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.map_err(|_| OxenHttpError::FailedToReadRequestPayload)?);
    }
    log::debug!(
        "{} got repo [{}] and content_ids size {}",
        current_function!(),
        repo_name,
        bytes.len()
    );

    let mut gz = GzDecoder::new(&bytes[..]);
    let mut line_delimited_files = String::new();
    gz.read_to_string(&mut line_delimited_files)?;

    let content_files: Vec<&str> = line_delimited_files.split('\n').collect();

    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    log::debug!("Got {} content ids", content_files.len());
    for content_file in content_files.iter() {
        if content_file.is_empty() {
            // last line might be empty on split \n
            continue;
        }

        // log::debug!("download_data_from_version_paths pulling {}", content_file);

        // We read from version file as determined by the latest logic (data.extension)
        // but still want to write the tar archive with the original filename so that it
        // unpacks to the location old clients expect.

        // This is annoying but the older client passes in the full path to the version file with the extension
        // ie .oxen/versions/files/71/7783cda74ceeced8d45fae3155382c/data.jpg
        // but the new client passes in the path without the extension
        // ie .oxen/versions/files/71/7783cda74ceeced8d45fae3155382c/data
        // So we need to support both formats.

        // In an ideal world we would just pass in the hash and not the full path to save on bandwidth as well
        let mut path_to_read = repo.path.join(content_file);
        path_to_read = replace_file_name_keep_extension(
            &path_to_read,
            constants::VERSION_FILE_NAME.to_string(),
        );

        if path_to_read.exists() {
            tar.append_path_with_name(path_to_read, content_file)?;
        } else {
            // No whole-file blob on disk: chunked versions publish only a
            // manifest, so read through the version store, which reconstructs
            // the bytes from chunks.
            let Some(hash) = version_hash_from_path(&path_to_read) else {
                log::error!("Could not find content: {content_file:?} -> {path_to_read:?}");
                return Err(OxenError::path_does_not_exist(path_to_read).into());
            };
            let data = match repo.version_store().get_version(&hash).await {
                Ok(data) => data,
                Err(err) => {
                    log::error!(
                        "Could not find content: {content_file:?} -> {path_to_read:?}: {err}"
                    );
                    return Err(OxenError::path_does_not_exist(path_to_read).into());
                }
            };
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append_data(&mut header, content_file, data.as_slice())?;
        }
    }

    tar.finish()?;
    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    Ok(HttpResponse::Ok().body(buffer))
}

/// Extract the version hash from a `.oxen/versions/files/{aa}/{rest}/data[.ext]`
/// path sent by an old client. The hash is the two directory names above the
/// file, concatenated.
fn version_hash_from_path(path: &std::path::Path) -> Option<String> {
    let suffix_dir = path.parent()?;
    let prefix_dir = suffix_dir.parent()?;
    let suffix = suffix_dir.file_name()?.to_str()?;
    let prefix = prefix_dir.file_name()?.to_str()?;
    (prefix.len() == 2 && !suffix.is_empty()).then(|| format!("{prefix}{suffix}"))
}

/// Download a chunk of a larger file
#[tracing::instrument(skip_all)]
pub async fn download_chunk(
    req: HttpRequest,
    query: web::Query<ChunkQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repo = get_repo(app_data, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;
    let path = resource.path.clone();

    log::debug!(
        "{} resource {}/{}",
        current_function!(),
        repo_name,
        resource
    );

    let version_store = repo.version_store();
    let chunk_start: u64 = query.chunk_start.unwrap_or(0);
    let chunk_size: u64 = query.chunk_size.unwrap_or_else(stream_segment_size);

    let file_node = match repositories::entries::get_file(&repo, &commit, &path)? {
        Some(node) => node,
        None => {
            return Err(OxenHttpError::NotFound);
        }
    };

    let chunk = version_store
        .get_version_chunk(&file_node.hash().to_string(), chunk_start, chunk_size)
        .await?;

    Ok(HttpResponse::Ok().body(chunk))
}

pub async fn list_tabular(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let commit_or_branch = path_param(&req, "commit_or_branch")?.to_string();
    let repo = get_repo(app_data, namespace, repo_name)?;
    let commit = repositories::revisions::get(&repo, &commit_or_branch)?
        .ok_or_else(|| OxenError::RevisionNotFound(commit_or_branch.into()))?;

    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    log::debug!(
        "{} page {} page_size {}",
        current_function!(),
        page,
        page_size,
    );

    let entries = repositories::entries::list_tabular_files_in_repo(&repo, &commit)?;
    log::debug!("list_tabular entries: {entries:?}");
    let (paginated_entries, pagination) = paginate(entries, page, page_size);

    Ok(HttpResponse::Ok().json(PaginatedMetadataEntriesResponse {
        status: StatusMessage::resource_found(),
        entries: PaginatedMetadataEntries {
            entries: paginated_entries,
            pagination,
        },
    }))
}

#[cfg(test)]
mod tests {
    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;

    use actix_web::{App, web};
    use flate2::Compression;
    use flate2::read::GzDecoder;
    use flate2::write::GzEncoder;
    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::storage::version_store::ContentFormat;
    use liboxen::util;
    use std::collections::HashMap;
    use std::io::{Read, Write};

    /// Old clients (< v0.37.2) fetch version data by full version-file path via
    /// GET /versions. Chunked versions publish a manifest instead of a `data`
    /// blob, so the endpoint must fall back to the version store and
    /// reconstruct the bytes.
    #[actix_web::test]
    async fn test_download_data_from_version_paths_serves_chunked_versions() -> Result<(), OxenError>
    {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Legacy-Chunked-Download";
        let mut repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        repo.set_content_format(ContentFormat::BlockV1);
        repo.save()?;

        // A small file stays a whole-file blob; a large CSV stores chunked.
        let small_path = repo.path.join("small.txt");
        util::fs::write_to_path(&small_path, "Hello")?;
        let mut csv = String::from("id,name,label\n");
        for i in 0..60_000 {
            csv.push_str(&format!("{i},image_{i}.jpg,label_{}\n", i % 10));
        }
        let csv_path = repo.path.join("big.csv");
        util::fs::write_to_path(&csv_path, &csv)?;
        repositories::add(&repo, &small_path).await?;
        repositories::add(&repo, &csv_path).await?;
        let commit = repositories::commit(&repo, "Add files")?;

        let small_node = repositories::entries::get_file(&repo, &commit, "small.txt")?
            .expect("small.txt committed");
        let csv_node =
            repositories::entries::get_file(&repo, &commit, "big.csv")?.expect("big.csv committed");
        let version_line =
            |hash: &str| format!(".oxen/versions/files/{}/{}/data", &hash[..2], &hash[2..]);
        let small_line = version_line(&small_node.hash().to_string());
        let csv_line = version_line(&csv_node.hash().to_string());
        // The CSV must have no whole-file blob on disk for this test to
        // exercise the version-store fallback.
        assert!(repo.path.join(&small_line).exists());
        assert!(!repo.path.join(&csv_line).exists());

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(format!("{small_line}\n{csv_line}\n").as_bytes())?;
        let body = encoder.finish()?;

        let uri = format!("/oxen/{namespace}/{repo_name}/versions");
        let req = actix_web::test::TestRequest::get()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .set_payload(body)
            .to_request();
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/versions",
                    web::get().to(controllers::entries::download_data_from_version_paths),
                ),
        )
        .await;
        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

        let bytes = actix_web::test::read_body(resp).await;
        let mut archive = tar::Archive::new(GzDecoder::new(&bytes[..]));
        let mut contents: HashMap<String, Vec<u8>> = HashMap::new();
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.to_string_lossy().to_string();
            let mut data = Vec::new();
            entry.read_to_end(&mut data)?;
            contents.insert(path, data);
        }
        assert_eq!(contents[&small_line], b"Hello");
        assert_eq!(contents[&csv_line], csv.as_bytes());

        test::cleanup_repo_and_sync_dir(repo, &sync_dir)?;
        Ok(())
    }
}
