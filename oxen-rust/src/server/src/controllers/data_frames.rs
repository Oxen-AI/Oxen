use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::df_opts_query::{self, DFOptsQuery};
use crate::params::{app_data, parse_resource, path_param};

use liboxen::constants;
use liboxen::error::PathBufError;
use liboxen::model::{DataFrameSize, NewCommitBody};
use liboxen::opts::df_opts::DFOptsView;
use liboxen::repositories;
use liboxen::view::data_frames::FromDirectoryRequest;
use liboxen::view::entries::ResourceVersion;

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::opts::{DFOpts, PaginateOpts};
use liboxen::view::{
    CommitResponse, JsonDataFrameView, JsonDataFrameViewResponse, JsonDataFrameViews, Pagination,
    StatusMessage,
};

use utoipa;
use uuid::Uuid;

/// Get data frame slice
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/data_frames/{resource}",
    operation_id = "get_data_frame_slice",
    tag = "DataFrames",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("resource" = String, Path, description = "Path to the tabular file (including branch/commit info)", example = "main/data/labels.csv"),
        DFOptsQuery // Assumes DFOptsQuery derives IntoParams
    ),
    responses(
        (status = 200, description = "Data frame slice found", body = JsonDataFrameViewResponse),
        (status = 404, description = "File or resource not found")
    )
)]
pub async fn get(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    let mut page_opts = PaginateOpts {
        page_num: constants::DEFAULT_PAGE_NUM,
        page_size: constants::DEFAULT_PAGE_SIZE,
    };

    if let Some((start, end)) = opts.slice_indices() {
        log::debug!("controllers::data_frames Got slice params {start}..{end}");
    } else {
        let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
        let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

        page_opts.page_num = page;
        page_opts.page_size = page_size;

        let start = if page == 0 { 0 } else { page_size * (page - 1) };
        let end = page_size * page;
        opts.slice = Some(format!("{start}..{end}"));
    }

    let resource_version = ResourceVersion {
        path: resource.path.to_string_lossy().into(),
        version: resource.version.to_string_lossy().into(),
    };

    opts.path = Some(resource.path.clone());
    let data_frame_slice =
        repositories::data_frames::get_slice(&repo, &commit, &resource.path, &opts).await?;

    let mut df = data_frame_slice.slice;
    let view_height = if opts.has_filter_transform() {
        data_frame_slice.total_entries
    } else {
        data_frame_slice.schemas.slice.size.height
    };

    let total_pages = (view_height as f64 / page_opts.page_size as f64).ceil() as usize;

    let opts_view = DFOptsView::from_df_opts(&opts);
    let response = JsonDataFrameViewResponse {
        status: StatusMessage::resource_found(),
        data_frame: JsonDataFrameViews {
            source: data_frame_slice.schemas.source,
            view: JsonDataFrameView {
                schema: data_frame_slice.schemas.slice.schema,
                size: DataFrameSize {
                    height: df.height(),
                    width: df.width(),
                },
                data: JsonDataFrameView::json_from_df(&mut df),
                pagination: Pagination {
                    page_number: page_opts.page_num,
                    page_size: page_opts.page_size,
                    total_pages,
                    total_entries: data_frame_slice.total_entries,
                },
                opts: opts_view,
            },
        },
        commit: Some(commit.clone()),
        resource: Some(resource_version),
        derived_resource: None,
    };
    Ok(HttpResponse::Ok().json(response))
}

/// Start data frame indexing
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/data_frames/{resource}/index",
    operation_id = "start_data_frame_indexing",
    tag = "DataFrames",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "CattleData"),
        ("resource" = String, Path, description = "Path to the tabular file to index (including branch/commit info)", example = "main/data/weights.csv"),
    ),
    responses(
        (status = 200, description = "Indexing process started or completed", body = StatusMessage),
        (status = 409, description = "Dataset already indexed"),
        (status = 404, description = "Resource not found")
    )
)]
pub async fn index(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;

    let path = resource.clone().path;

    // Check if the data frame is already indexed.
    if repositories::workspaces::data_frames::is_queryable_data_frame_indexed(
        &repo,
        &resource.path,
        &commit,
    )? {
        log::debug!("data frame is already indexed");
        // If the data frame is already indexed, return the appropriate error.
        return Err(OxenHttpError::DatasetAlreadyIndexed(PathBufError::from(
            path,
        )));
    } else {
        log::debug!("data frame is not indexed");
        // If not, proceed to create a new workspace and index the data frame.
        let workspace_id = Uuid::new_v4().to_string();
        let workspace = match repositories::workspaces::create(&repo, &commit, workspace_id, false)
        {
            Ok(workspace) => workspace,
            Err(_e) => repositories::workspaces::get_non_editable_by_commit_id(&repo, &commit.id)?,
        };
        repositories::workspaces::data_frames::index(&repo, &workspace, &path)?;
    }

    Ok(HttpResponse::Ok().json(StatusMessage::resource_updated()))
}

/// Create data frame from directory
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/data_frames/from_directory/{resource}",
    operation_id = "create_data_frame_from_directory",
    tag = "DataFrames",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "CattleData"),
        ("resource" = String, Path, description = "Directory path to read from (including branch/commit info)", example = "main/data/images"),
    ),
    request_body(
        content = FromDirectoryRequest,
        description = "Options for creating a data frame from a directory, including output path, columns, and commit message.",
        example = json!({
            "output_path": "data/image_index.csv",
            "extra_columns": ["size", "extension"],
            "commit_message": "Generated image index",
            "user_email": "bessie@oxen.ai",
            "user_name": "Bessie",
            "recursive": true
        })
    ),
    responses(
        (status = 200, description = "Data frame created and committed", body = CommitResponse),
        (status = 400, description = "Invalid request body or resource path"),
        (status = 404, description = "Repository or resource not found")
    )
)]
pub async fn from_directory(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;
    let branch = resource.clone().branch.ok_or(OxenHttpError::NotFound)?;
    let path = resource.path.clone();
    let data: Result<FromDirectoryRequest, serde_json::Error> = serde_json::from_str(&body);
    let data = match data {
        Ok(data) => data,
        Err(err) => {
            log::error!("Unable to parse body. Err: {err}\n{body}");
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())));
        }
    };

    let output_path = data.output_path.unwrap_or("".to_string());
    let extra_columns = data.extra_columns.unwrap_or(vec![]);
    let commit_message = data.commit_message.unwrap_or("".to_string());
    let user_email = data.user_email.unwrap_or("".to_string());
    let recursive = data.recursive.unwrap_or(false);
    let temp_workspace = repositories::workspaces::create_temporary(&repo, &commit)?;
    let user_name = data.user_name.unwrap_or("".to_string());
    let new_commit = NewCommitBody {
        author: user_name,
        email: user_email,
        message: commit_message,
    };
    let commit = repositories::workspaces::data_frames::from_directory(
        &repo,
        &temp_workspace,
        &path,
        &output_path,
        &extra_columns,
        recursive,
        &new_commit,
        &branch,
    )
    .await?;
    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit,
    }))
}
