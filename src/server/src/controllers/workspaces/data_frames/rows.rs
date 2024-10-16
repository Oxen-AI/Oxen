use std::path::PathBuf;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::{web::Bytes, HttpRequest, HttpResponse};
use liboxen::model::data_frame::update_result::UpdateResult;
use liboxen::model::data_frame::DataFrameSchemaSize;
use liboxen::model::Schema;
use liboxen::opts::DFOpts;
use liboxen::repositories;
use liboxen::view::json_data_frame_view::{BatchUpdateResponse, JsonDataFrameRowResponse};
use liboxen::view::{JsonDataFrameView, JsonDataFrameViews, StatusMessage};

pub async fn create(req: HttpRequest, bytes: Bytes) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace.clone(), repo_name.clone())?;
    let file_path = PathBuf::from(path_param(&req, "path")?);

    let data = String::from_utf8(bytes.to_vec()).expect("Could not parse bytes as utf8");

    // If the json has an outer property of "data", serialize the inner object
    let json_value: serde_json::Value = serde_json::from_str(&data)?;
    // TODO why do we support both?
    let data = if let Some(data_obj) = json_value.get("data") {
        data_obj
    } else {
        &json_value
    };

    log::info!(
        "create row {namespace}/{repo_name} for file {:?} on in workspace id {}",
        file_path,
        workspace_id
    );
    log::debug!("create row with data {:?}", data);

    // Get the workspace
    let workspace = repositories::workspaces::get(&repo, &workspace_id)?;

    // Make sure the data frame is indexed
    let is_editable = repositories::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_editable {
        return Err(OxenHttpError::DatasetNotIndexed(file_path.into()));
    }

    let row_df =
        repositories::workspaces::data_frames::rows::add(&repo, &workspace, &file_path, data)?;
    let row_id: Option<String> = repositories::workspaces::data_frames::rows::get_row_id(&row_df)?;
    let row_index: Option<usize> =
        repositories::workspaces::data_frames::rows::get_row_idx(&row_df)?;

    let opts = DFOpts::empty();
    let row_schema = Schema::from_polars(&row_df.schema().clone());
    let row_df_source = DataFrameSchemaSize::from_df(&row_df, &row_schema);
    let row_df_view = JsonDataFrameView::from_df_opts(row_df, row_schema, &opts);

    let diff = repositories::workspaces::data_frames::rows::get_row_diff(&workspace, &file_path)?;

    let response = JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: row_df_source,
            view: row_df_view,
        },
        diff: Some(diff),
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_found(),
        resource: None,
        row_id,
        row_index,
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn get(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let file_path = path_param(&req, "path")?;
    let row_id = path_param(&req, "row_id")?;

    let workspace = repositories::workspaces::get(&repo, workspace_id)?;
    let row_df =
        repositories::workspaces::data_frames::rows::get_by_id(&workspace, file_path, row_id)?;

    let row_id = repositories::workspaces::data_frames::rows::get_row_id(&row_df)?;
    let row_index = repositories::workspaces::data_frames::rows::get_row_idx(&row_df)?;

    let opts = DFOpts::empty();
    let row_schema = Schema::from_polars(&row_df.schema().clone());
    let row_df_source = DataFrameSchemaSize::from_df(&row_df, &row_schema);
    let row_df_view = JsonDataFrameView::from_df_opts(row_df, row_schema, &opts);

    let response = JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: row_df_source,
            view: row_df_view,
        },
        diff: None,
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_found(),
        resource: None,
        row_id,
        row_index,
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn update(req: HttpRequest, bytes: Bytes) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let row_id = path_param(&req, "row_id")?;

    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    let file_path = PathBuf::from(path_param(&req, "path")?);
    let Ok(data) = String::from_utf8(bytes.to_vec()) else {
        return Err(OxenHttpError::BadRequest(
            "Could not parse bytes as utf8".to_string().into(),
        ));
    };

    // If the json has an outer property of "data", serialize the inner object
    let json_value: serde_json::Value = serde_json::from_str(&data)?;
    // TODO why do we allow both?
    let data = if let Some(data_obj) = json_value.get("data") {
        data_obj
    } else {
        &json_value
    };

    // Assumes the workspace is already created
    let workspace = repositories::workspaces::get(&repo, &workspace_id)?;
    log::debug!(
        "update row repo {}/{} -> {}/{:?}",
        namespace,
        repo_name,
        workspace_id,
        file_path
    );

    let modified_row = repositories::workspaces::data_frames::rows::update(
        &repo, &workspace, &file_path, &row_id, data,
    )?;

    let row_index = repositories::workspaces::data_frames::rows::get_row_idx(&modified_row)?;
    let row_id = repositories::workspaces::data_frames::rows::get_row_id(&modified_row)?;

    let diff = repositories::workspaces::data_frames::rows::get_row_diff(&workspace, &file_path)?;

    log::debug!("Modified row in controller is {:?}", modified_row);
    let schema = Schema::from_polars(&modified_row.schema());
    Ok(HttpResponse::Ok().json(JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: DataFrameSchemaSize::from_df(&modified_row, &schema),
            view: JsonDataFrameView::from_df_opts(modified_row, schema, &DFOpts::empty()),
        },
        diff: Some(diff),
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_updated(),
        resource: None,
        row_id,
        row_index,
    }))
}

pub async fn delete(req: HttpRequest, _bytes: Bytes) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let row_id = path_param(&req, "row_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let file_path = PathBuf::from(path_param(&req, "path")?);
    let workspace = repositories::workspaces::get(&repo, workspace_id)?;

    let df = repositories::workspaces::data_frames::rows::delete(
        &repo, &workspace, &file_path, &row_id,
    )?;
    let diff = repositories::workspaces::data_frames::rows::get_row_diff(&workspace, &file_path)?;

    let schema = Schema::from_polars(&df.schema());
    Ok(HttpResponse::Ok().json(JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: DataFrameSchemaSize::from_df(&df, &schema),
            view: JsonDataFrameView::from_df_opts(df, schema, &DFOpts::empty()),
        },
        diff: Some(diff),
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_deleted(),
        resource: None,
        row_id: None,
        row_index: None,
    }))
}

pub async fn restore(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let row_id = path_param(&req, "row_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let file_path = PathBuf::from(path_param(&req, "path")?);
    let workspace = repositories::workspaces::get(&repo, workspace_id)?;

    let restored_row = repositories::workspaces::data_frames::rows::restore(
        &repo, &workspace, &file_path, &row_id,
    )?;

    let row_index = repositories::workspaces::data_frames::rows::get_row_idx(&restored_row)?;
    let row_id = repositories::workspaces::data_frames::rows::get_row_id(&restored_row)?;

    let diff = repositories::workspaces::data_frames::rows::get_row_diff(&workspace, &file_path)?;

    log::debug!("Restored row in controller is {:?}", restored_row);
    let schema = Schema::from_polars(&restored_row.schema());
    Ok(HttpResponse::Ok().json(JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: DataFrameSchemaSize::from_df(&restored_row, &schema),
            view: JsonDataFrameView::from_df_opts(restored_row, schema, &DFOpts::empty()),
        },
        diff: Some(diff),
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_updated(),
        resource: None,
        row_id,
        row_index,
    }))
}

pub async fn batch_update(req: HttpRequest, bytes: Bytes) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;

    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    let file_path = PathBuf::from(path_param(&req, "path")?);
    let Ok(data) = String::from_utf8(bytes.to_vec()) else {
        return Err(OxenHttpError::BadRequest(
            "Could not parse bytes as utf8".to_string().into(),
        ));
    };

    let json_value: serde_json::Value = serde_json::from_str(&data)?;
    let data = if let Some(data_obj) = json_value.get("data") {
        data_obj
    } else {
        &json_value
    };

    let workspace = repositories::workspaces::get(&repo, &workspace_id)?;
    log::debug!(
        "update row repo {}/{} -> {}/{:?}",
        namespace,
        repo_name,
        workspace_id,
        file_path
    );

    let modified_rows = repositories::workspaces::data_frames::rows::batch_update(
        &repo, &workspace, &file_path, data,
    )?;

    let mut responses = Vec::new();

    for modified_row in modified_rows {
        let response = match modified_row {
            UpdateResult::Success(row_id, _data_frame) => BatchUpdateResponse {
                row_id,
                code: 200,
                error: None,
            },
            UpdateResult::Error(row_id, error) => BatchUpdateResponse {
                row_id,
                code: 500,
                error: Some(error.to_string()),
            },
        };
        responses.push(response);
    }

    Ok(HttpResponse::Ok().json(responses))
}
