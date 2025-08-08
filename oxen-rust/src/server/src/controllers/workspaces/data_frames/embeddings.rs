use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::{web::Bytes, HttpRequest, HttpResponse};
use liboxen::model::Schema;
use liboxen::opts::{DFOpts, PaginateOpts};
use liboxen::repositories;
use liboxen::view::data_frames::embeddings::{
    EmbeddingColumnsResponse, EmbeddingQuery, IndexEmbeddingRequest,
};
use liboxen::view::entries::ResourceVersion;
use liboxen::view::json_data_frame_view::WorkspaceJsonDataFrameViewResponse;
use liboxen::view::{JsonDataFrameViews, StatusMessage, StatusMessageDescription};

/// Get the embedding status for a data frame
pub async fn get(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let file_path = path_param(&req, "path")?;

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let response = EmbeddingColumnsResponse {
        columns: repositories::workspaces::data_frames::embeddings::list_indexed_columns(
            &workspace, file_path,
        )?,
        status: StatusMessage::resource_found(),
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn neighbors(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let file_path = path_param(&req, "path")?;

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let is_indexed = repositories::workspaces::data_frames::is_indexed(&workspace, &file_path)?;
    log::debug!("neighbors is_indexed: {}", is_indexed);
    if !is_indexed {
        let response = WorkspaceJsonDataFrameViewResponse {
            status: StatusMessage::resource_found(),
            data_frame: None,
            resource: None,
            commit: None, // Not at a committed state
            derived_resource: None,
            is_indexed,
        };

        return Ok(HttpResponse::Ok().json(response));
    }

    log::debug!("neighbors: Embedding query: {:?}", body);
    let request: EmbeddingQuery = serde_json::from_str(&body)?;
    let count = repositories::workspaces::data_frames::count(&workspace, &file_path)?;

    let mut opts = DFOpts::empty();
    opts.page = Some(request.page_num);
    opts.page_size = Some(request.page_size);

    let df = repositories::workspaces::data_frames::embeddings::nearest_neighbors(
        &workspace,
        &file_path,
        &request.column,
        request.embedding,
        &PaginateOpts {
            page_num: request.page_num,
            page_size: request.page_size,
        },
        false,
    )?;

    let Some(mut df_schema) =
        repositories::data_frames::schemas::get_by_path(&repo, &workspace.commit, &file_path)?
    else {
        log::error!("Failed to get schema for data frame {:?}", file_path);
        return Err(OxenHttpError::NotFound);
    };

    let resource = ResourceVersion {
        path: file_path.clone(),
        version: workspace.commit.id.to_string(),
    };

    let og_schema = if let Some(schema) =
        repositories::data_frames::schemas::get_by_path(&repo, &workspace.commit, &resource.path)?
    {
        schema
    } else {
        Schema::from_polars(df.schema())
    };

    df_schema.update_metadata_from_schema(&og_schema);

    let mut df_views =
        JsonDataFrameViews::from_df_and_opts_unpaginated(df, df_schema, count, &opts).await;

    repositories::workspaces::data_frames::columns::decorate_fields_with_column_diffs(
        &workspace,
        &file_path,
        &mut df_views,
    )?;

    let new_schema = repositories::data_frames::schemas::get_staged_schema_with_staged_db_manager(
        &workspace.workspace_repo,
        &file_path,
    )?;
    repositories::workspaces::data_frames::columns::update_column_schemas(
        new_schema,
        &mut df_views,
    )?;

    let response = WorkspaceJsonDataFrameViewResponse {
        status: StatusMessage::resource_found(),
        data_frame: Some(df_views),
        resource: Some(resource),
        commit: None, // Not at a committed state
        derived_resource: None,
        is_indexed: true,
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Index a column to enable nearest neighbors search
pub async fn post(req: HttpRequest, bytes: Bytes) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let file_path = path_param(&req, "path")?;

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };
    let Ok(data) = String::from_utf8(bytes.to_vec()) else {
        return Err(OxenHttpError::BadRequest(
            "Could not parse bytes as utf8".to_string().into(),
        ));
    };

    let request: IndexEmbeddingRequest = serde_json::from_str(&data)?;
    let column = request.column;
    let use_background_thread = request.use_background_thread.unwrap_or(false);

    repositories::workspaces::data_frames::embeddings::index(
        &workspace,
        &file_path,
        &column,
        use_background_thread,
    )?;

    let response = EmbeddingColumnsResponse {
        columns: repositories::workspaces::data_frames::embeddings::list_indexed_columns(
            &workspace, file_path,
        )?,
        status: StatusMessage::resource_found(),
    };

    Ok(HttpResponse::Ok().json(response))
}
