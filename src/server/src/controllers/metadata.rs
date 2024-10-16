use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param, AggregateQuery};

use liboxen::core;
use liboxen::error::OxenError;
use liboxen::model::data_frame::DataFrameSchemaSize;
use liboxen::model::DataFrameSize;
use liboxen::opts::df_opts::DFOptsView;
use liboxen::opts::DFOpts;
use liboxen::view::entries::ResourceVersion;

use liboxen::view::{
    JsonDataFrame, JsonDataFrameView, JsonDataFrameViewResponse, JsonDataFrameViews,
    MetadataEntryResponse, Pagination, StatusMessage,
};
use liboxen::{current_function, repositories};

use actix_web::{web, HttpRequest, HttpResponse};

pub async fn file(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
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

    let latest_commit = repositories::commits::get_by_id(&repo, &commit.id)?
        .ok_or(OxenError::revision_not_found(commit.id.clone().into()))?;

    log::debug!(
        "{} resolve commit {} -> '{}'",
        current_function!(),
        latest_commit.id,
        latest_commit.message
    );

    let mut entry = repositories::entries::get_meta_entry(&repo, &commit, &resource.path)?;
    entry.resource = Some(resource.clone());
    let meta = MetadataEntryResponse {
        status: StatusMessage::resource_found(),
        entry,
    };
    Ok(HttpResponse::Ok().json(meta))
}

/// TODO: Depreciate this API
pub async fn dir(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
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

    let latest_commit = repositories::commits::get_by_id(&repo, &commit.id)?
        .ok_or(OxenError::revision_not_found(commit.id.clone().into()))?;

    log::debug!(
        "{} resolve commit {} -> '{}'",
        current_function!(),
        latest_commit.id,
        latest_commit.message
    );

    let resource_version = ResourceVersion {
        path: resource.path.to_string_lossy().into(),
        version: resource.version.to_string_lossy().into(),
    };

    let directory = resource.path;
    let offset = 0;
    let limit = 100;
    let mut sliced_df = core::v0_10_0::index::commit_metadata_db::select(
        &repo,
        &latest_commit,
        &directory,
        offset,
        limit,
    )?;
    let (num_rows, num_cols) =
        core::v0_10_0::index::commit_metadata_db::full_size(&repo, &latest_commit, &directory)?;

    let full_size = DataFrameSize {
        width: num_cols,
        height: num_rows,
    };

    let df = JsonDataFrame::from_df(&mut sliced_df);

    let source_df = DataFrameSchemaSize {
        schema: df.schema.clone(),
        size: full_size,
    };

    let view_df = JsonDataFrameView {
        data: df.data,
        schema: df.view_schema.clone(),
        size: df.view_size.clone(),
        pagination: Pagination {
            page_number: 1,
            page_size: 100,
            total_pages: 1,
            total_entries: df.view_size.height,
        },
        opts: DFOptsView::empty(),
    };

    let response = JsonDataFrameViewResponse {
        status: StatusMessage::resource_found(),
        data_frame: {
            JsonDataFrameViews {
                source: source_df,
                view: view_df,
            }
        },
        commit: Some(commit),
        resource: Some(resource_version),
        derived_resource: None,
    };
    Ok(HttpResponse::Ok().json(response))
}

/// TODO: Depreciate this API
pub async fn agg_dir(
    req: HttpRequest,
    query: web::Query<AggregateQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;

    let column = query.column.clone().ok_or(OxenHttpError::BadRequest(
        "Must supply column query parameter".into(),
    ))?;

    log::debug!(
        "{} resource {}/{}",
        current_function!(),
        repo_name,
        resource
    );

    let latest_commit = repositories::commits::get_by_id(&repo, &commit.id)?
        .ok_or(OxenError::revision_not_found(commit.id.clone().into()))?;

    log::debug!(
        "{} resolve commit {} -> '{}'",
        current_function!(),
        latest_commit.id,
        latest_commit.message
    );

    let directory = &resource.path;

    let cached_path = core::v0_10_0::cache::cachers::content_stats::dir_column_path(
        &repo,
        &latest_commit,
        directory,
        &column,
    );
    log::debug!("Reading aggregation from cached path: {:?}", cached_path);

    if cached_path.exists() {
        let mut df = core::df::tabular::read_df(&cached_path, DFOpts::empty())?;

        let resource_version = ResourceVersion {
            path: resource.path.to_string_lossy().into(),
            version: resource.version.to_string_lossy().into(),
        };

        let df = JsonDataFrame::from_df(&mut df);
        let full_df = DataFrameSchemaSize {
            schema: df.schema.clone(),
            size: df.full_size.clone(),
        };

        let view_df = JsonDataFrameView {
            data: df.data,
            schema: df.view_schema.clone(),
            size: df.view_size.clone(),
            pagination: Pagination {
                page_number: 1,
                page_size: df.full_size.height,
                total_pages: 1,
                total_entries: df.full_size.height,
            },
            opts: DFOptsView::empty(),
        };

        let response = JsonDataFrameViewResponse {
            status: StatusMessage::resource_found(),
            data_frame: {
                JsonDataFrameViews {
                    source: full_df,
                    view: view_df,
                }
            },
            commit: Some(commit),
            resource: Some(resource_version),
            derived_resource: None,
        };
        Ok(HttpResponse::Ok().json(response))
    } else {
        log::error!("Metadata cache not computed for column {}", column);
        Ok(HttpResponse::BadRequest().json(StatusMessage::resource_not_found()))
    }
}
