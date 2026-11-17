use crate::errors::OxenHttpError;
use crate::helpers::get_repo_async;
use crate::params::{PageNumQuery, app_data, parse_resource_async, path_param};

use liboxen::constants::stream_segment_size;
use liboxen::error::OxenError;
use liboxen::util::paginate;
use liboxen::view::StatusMessage;
use liboxen::view::entries::{PaginatedMetadataEntries, PaginatedMetadataEntriesResponse};
use liboxen::{constants, current_function, repositories};

use actix_web::{HttpRequest, HttpResponse, web};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct ChunkQuery {
    pub chunk_start: Option<u64>,
    pub chunk_size: Option<u64>,
}

/// Download a chunk of a larger file
#[tracing::instrument(skip_all)]
pub async fn download_chunk(
    req: HttpRequest,
    query: web::Query<ChunkQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo_async(app_data, namespace, repo_name).await?;
    let resource = parse_resource_async(&req, &repo).await?;
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

    let file_node = repositories::tree::get_file_by_path_async(&repo, &commit, &path)
        .await?
        .ok_or(OxenHttpError::NotFound)?;

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
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let commit_or_branch = path_param(&req, "commit_or_branch")?;
    let repo = get_repo_async(app_data, namespace, repo_name).await?;
    let commit = repositories::revisions::get_async(&repo, commit_or_branch)
        .await?
        .ok_or_else(|| OxenError::RevisionNotFound(commit_or_branch.into()))?;

    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    log::debug!(
        "{} page {} page_size {}",
        current_function!(),
        page,
        page_size,
    );

    let entries = repositories::entries::list_tabular_files_in_repo_async(&repo, &commit).await?;
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
