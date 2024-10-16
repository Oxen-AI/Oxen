use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::df_opts_query::{self, DFOptsQuery};
use crate::params::{app_data, parse_resource, path_param};

use liboxen::constants;
use liboxen::error::PathBufError;
use liboxen::model::DataFrameSize;
use liboxen::opts::df_opts::DFOptsView;
use liboxen::repositories;
use liboxen::view::entries::ResourceVersion;

use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::opts::{DFOpts, PaginateOpts};
use liboxen::view::{
    JsonDataFrameView, JsonDataFrameViewResponse, JsonDataFrameViews, Pagination, StatusMessage,
};

use uuid::Uuid;

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
        log::debug!(
            "controllers::data_frames Got slice params {}..{}",
            start,
            end
        );
    } else {
        let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
        let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

        page_opts.page_num = page;
        page_opts.page_size = page_size;

        let start = if page == 0 { 0 } else { page_size * (page - 1) };
        let end = page_size * page;
        opts.slice = Some(format!("{}..{}", start, end));
    }

    let resource_version = ResourceVersion {
        path: resource.path.to_string_lossy().into(),
        version: resource.version.to_string_lossy().into(),
    };

    let data_frame_slice =
        repositories::data_frames::get_slice(&repo, &commit, &resource.path, &opts)?;

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
        // If the data frame is already indexed, return the appropriate error.
        return Err(OxenHttpError::DatasetAlreadyIndexed(PathBufError::from(
            path,
        )));
    } else {
        // If not, proceed to create a new workspace and index the data frame.
        let workspace_id = Uuid::new_v4().to_string();
        let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, false)?;
        repositories::workspaces::data_frames::index(&repo, &workspace, &path)?;
    }

    Ok(HttpResponse::Ok().json(StatusMessage::resource_updated()))
}
