use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param, PageNumQuery};

use liboxen::constants::AVG_CHUNK_SIZE;
use liboxen::error::OxenError;
use liboxen::util::fs::replace_file_name_keep_extension;
use liboxen::util::paginate;
use liboxen::view::entries::{PaginatedMetadataEntries, PaginatedMetadataEntriesResponse};
use liboxen::view::StatusMessage;
use liboxen::{constants, current_function, repositories, util};

use actix_web::{web, HttpRequest, HttpResponse};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::stream::StreamExt as _;
use serde::Deserialize;

use std::fs::File;
use std::io::prelude::*;

#[derive(Deserialize, Debug)]
pub struct ChunkQuery {
    pub chunk_start: Option<u64>,
    pub chunk_size: Option<u64>,
}

pub async fn download_data_from_version_paths(
    req: HttpRequest,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;

    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.unwrap());
    }
    log::debug!(
        "{} got repo [{}] and content_ids size {}",
        current_function!(),
        repo_name,
        bytes.len()
    );

    let mut gz = GzDecoder::new(&bytes[..]);
    let mut line_delimited_files = String::new();
    gz.read_to_string(&mut line_delimited_files).unwrap();

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
            tar.append_path_with_name(path_to_read, content_file)
                .unwrap();
        } else {
            log::error!(
                "Could not find content: {:?} -> {:?}",
                content_file,
                path_to_read
            );
        }
    }

    tar.finish().unwrap();
    let buffer: Vec<u8> = tar.into_inner().unwrap().finish().unwrap();
    Ok(HttpResponse::Ok().body(buffer))
}

/// Download a chunk of a larger file
pub async fn download_chunk(
    req: HttpRequest,
    query: web::Query<ChunkQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;

    log::debug!(
        "{} resource {}/{}",
        current_function!(),
        repo_name,
        resource
    );

    let version_path = util::fs::version_path_for_commit_id(&repo, &commit.id, &resource.path)?;
    let chunk_start: u64 = query.chunk_start.unwrap_or(0);
    let chunk_size: u64 = query.chunk_size.unwrap_or(AVG_CHUNK_SIZE);

    let mut f = File::open(version_path).unwrap();
    f.seek(std::io::SeekFrom::Start(chunk_start)).unwrap();
    let mut buffer = vec![0u8; chunk_size as usize];
    f.read_exact(&mut buffer).unwrap();

    Ok(HttpResponse::Ok().body(buffer))
}

pub async fn list_tabular(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let commit_or_branch = path_param(&req, "commit_or_branch")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let commit = repositories::revisions::get(&repo, &commit_or_branch)?
        .ok_or_else(|| OxenError::revision_not_found(commit_or_branch.into()))?;

    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    log::debug!(
        "{} page {} page_size {}",
        current_function!(),
        page,
        page_size,
    );

    let entries = repositories::entries::list_tabular_files_in_repo(&repo, &commit)?;
    log::debug!("list_tabular entries: {:?}", entries);
    let (paginated_entries, pagination) = paginate(entries, page, page_size);

    Ok(HttpResponse::Ok().json(PaginatedMetadataEntriesResponse {
        status: StatusMessage::resource_found(),
        entries: PaginatedMetadataEntries {
            entries: paginated_entries,
            pagination,
        },
    }))
}
