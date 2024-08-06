use std::path::PathBuf;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::{web::Bytes, HttpRequest, HttpResponse};
use liboxen::core::index::workspaces;
use liboxen::error::OxenError;
use liboxen::model::Schema;
use liboxen::opts::DFOpts;
use liboxen::view::json_data_frame_view::{JsonDataFrameRowResponse, JsonDataFrameSource};
use liboxen::view::{JsonDataFrameView, JsonDataFrameViews, StatusMessage};
use liboxen::{api, core::index};

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
    let workspace = index::workspaces::get(&repo, &workspace_id)?;

    // Make sure the data frame is indexed
    let is_editable = index::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_editable {
        return Err(OxenHttpError::DatasetNotIndexed(file_path.into()));
    }

    println!("WE GOT HERE NOW");

    let row_df = index::workspaces::data_frames::rows::add(&workspace, &file_path, data)?;
    let _ = workspaces::stager::add_to_dirs_db(&workspace.workspace_repo, &file_path);
    println!("ITSALLDONE");
    let row_id: Option<String> = index::workspaces::data_frames::rows::get_row_id(&row_df)?;
    let row_index: Option<usize> = index::workspaces::data_frames::rows::get_row_idx(&row_df)?;

    let opts = DFOpts::empty();
    let row_schema = Schema::from_polars(&row_df.schema().clone());
    let row_df_source = JsonDataFrameSource::from_df(&row_df, &row_schema);
    let row_df_view = JsonDataFrameView::from_df_opts(row_df, row_schema, &opts);

    let response = JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: row_df_source,
            view: row_df_view,
        },
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

    let workspace = index::workspaces::get(&repo, workspace_id)?;
    let row_df = index::workspaces::data_frames::rows::get_by_id(&workspace, file_path, row_id)?;

    let row_id = index::workspaces::data_frames::rows::get_row_id(&row_df)?;
    let row_index = index::workspaces::data_frames::rows::get_row_idx(&row_df)?;

    let opts = DFOpts::empty();
    let row_schema = Schema::from_polars(&row_df.schema().clone());
    let row_df_source = JsonDataFrameSource::from_df(&row_df, &row_schema);
    let row_df_view = JsonDataFrameView::from_df_opts(row_df, row_schema, &opts);

    let response = JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: row_df_source,
            view: row_df_view,
        },
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
    let workspace = index::workspaces::get(&repo, &workspace_id)?;
    log::debug!(
        "update row repo {}/{} -> {}/{:?}",
        namespace,
        repo_name,
        workspace_id,
        file_path
    );

    let modified_row =
        index::workspaces::data_frames::rows::update(&workspace, &file_path, &row_id, data)?;

    let row_index = index::workspaces::data_frames::rows::get_row_idx(&modified_row)?;
    let row_id = index::workspaces::data_frames::rows::get_row_id(&modified_row)?;

    log::debug!("Modified row in controller is {:?}", modified_row);
    let schema = Schema::from_polars(&modified_row.schema());
    Ok(HttpResponse::Ok().json(JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: JsonDataFrameSource::from_df(&modified_row, &schema),
            view: JsonDataFrameView::from_df_opts(modified_row, schema, &DFOpts::empty()),
        },
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
    let workspace = index::workspaces::get(&repo, workspace_id)?;

    let df = index::workspaces::data_frames::rows::delete(&workspace, file_path, &row_id)?;

    let schema = Schema::from_polars(&df.schema());
    Ok(HttpResponse::Ok().json(JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: JsonDataFrameSource::from_df(&df, &schema),
            view: JsonDataFrameView::from_df_opts(df, schema, &DFOpts::empty()),
        },
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
    let workspace = index::workspaces::get(&repo, workspace_id)?;

    let entry = api::local::entries::get_commit_entry(&repo, &workspace.commit, &file_path)?
        .ok_or(OxenError::entry_does_not_exist(file_path.clone()))?;

    let restored_row = index::workspaces::data_frames::rows::restore(&workspace, &entry, row_id)?;

    let row_index = index::workspaces::data_frames::rows::get_row_idx(&restored_row)?;
    let row_id = index::workspaces::data_frames::rows::get_row_id(&restored_row)?;

    log::debug!("Restored row in controller is {:?}", restored_row);
    let schema = Schema::from_polars(&restored_row.schema());
    Ok(HttpResponse::Ok().json(JsonDataFrameRowResponse {
        data_frame: JsonDataFrameViews {
            source: JsonDataFrameSource::from_df(&restored_row, &schema),
            view: JsonDataFrameView::from_df_opts(restored_row, schema, &DFOpts::empty()),
        },
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_updated(),
        resource: None,
        row_id,
        row_index,
    }))
}
