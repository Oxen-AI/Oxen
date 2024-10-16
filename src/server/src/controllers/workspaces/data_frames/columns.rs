use std::path::PathBuf;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::{HttpRequest, HttpResponse};
use liboxen::error::StringError;
use liboxen::model::data_frame::DataFrameSchemaSize;
use liboxen::model::Schema;
use liboxen::opts::DFOpts;
use liboxen::repositories;
use liboxen::view::data_frames::columns::{
    ColumnToDelete, ColumnToRestore, ColumnToUpdate, NewColumn,
};
use liboxen::view::json_data_frame_view::JsonDataFrameColumnResponse;
use liboxen::view::{JsonDataFrameView, JsonDataFrameViews, StatusMessage};
use serde_json::{json, Value};

pub async fn create(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace.clone(), repo_name.clone())?;
    let file_path = PathBuf::from(path_param(&req, "path")?);

    let mut body_json: Value = serde_json::from_str(&body).map_err(|_err| {
        OxenHttpError::BadRequest("Failed to parse NewColumn from request body".into())
    })?;

    if let Some(obj) = body_json.as_object_mut() {
        if obj.contains_key("dtype") {
            let dtype_value = obj.remove("dtype").unwrap(); // Safe to unwrap because we just checked it exists
            obj.insert("data_type".to_string(), dtype_value);
        }
    } else {
        return Err(OxenHttpError::BadRequest(
            "Request body is not a valid JSON object".into(),
        ));
    }

    let new_column: NewColumn = serde_json::from_value(body_json)?;

    log::info!(
        "create column {namespace}/{repo_name} for file {:?} on in workspace id {}",
        file_path,
        workspace_id
    );
    log::debug!("create column with data {:?}", new_column);

    // Get the workspace
    let workspace = repositories::workspaces::get(&repo, &workspace_id)?;

    // Make sure the data frame is indexed
    let is_editable = repositories::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_editable {
        return Err(OxenHttpError::DatasetNotIndexed(file_path.into()));
    }

    let column_df = repositories::workspaces::data_frames::columns::add(
        &repo,
        &workspace,
        &file_path,
        &new_column,
    )?;

    let opts = DFOpts::empty();
    let column_schema = Schema::from_polars(&column_df.schema().clone());
    let column_df_source = DataFrameSchemaSize::from_df(&column_df, &column_schema);
    let column_df_view = JsonDataFrameView::from_df_opts(column_df, column_schema, &opts);
    let diff =
        repositories::workspaces::data_frames::columns::get_column_diff(&workspace, &file_path)?;

    let mut df_views = JsonDataFrameViews {
        source: column_df_source,
        view: column_df_view,
    };

    repositories::workspaces::data_frames::columns::decorate_fields_with_column_diffs(
        &workspace,
        &file_path,
        &mut df_views,
    )?;

    let response = JsonDataFrameColumnResponse {
        data_frame: df_views,
        diff: Some(diff),
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_found(),
        resource: None,
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace.clone(), repo_name.clone())?;
    let file_path = PathBuf::from(path_param(&req, "path")?);
    let column_name = path_param(&req, "column_name")
        .map_err(|_| OxenHttpError::BadRequest("Column name missing in path parameters".into()))?;

    let column_to_delete: ColumnToDelete = ColumnToDelete { name: column_name };

    log::info!(
        "Delete column {namespace}/{repo_name} for file {:?} on in workspace id {}",
        file_path,
        workspace_id
    );
    log::debug!("create column with data {:?}", column_to_delete);

    // Get the workspace
    let workspace = repositories::workspaces::get(&repo, &workspace_id)?;

    // Make sure the data frame is indexed
    let is_editable = repositories::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_editable {
        return Err(OxenHttpError::DatasetNotIndexed(file_path.into()));
    }

    let column_df = match repositories::workspaces::data_frames::columns::delete(
        &repo,
        &workspace,
        &file_path,
        &column_to_delete,
    ) {
        Ok(df) => df,
        Err(e) => {
            log::error!("Error deleting column: {:?}", e);
            return Err(OxenHttpError::BasicError(StringError::from(e.to_string())));
        }
    };

    let opts = DFOpts::empty();
    let column_schema = Schema::from_polars(&column_df.schema().clone());
    let column_df_source = DataFrameSchemaSize::from_df(&column_df, &column_schema);
    let column_df_view = JsonDataFrameView::from_df_opts(column_df, column_schema, &opts);

    let mut df_views = JsonDataFrameViews {
        source: column_df_source,
        view: column_df_view,
    };

    let diff =
        repositories::workspaces::data_frames::columns::get_column_diff(&workspace, &file_path)?;

    repositories::workspaces::data_frames::columns::decorate_fields_with_column_diffs(
        &workspace,
        &file_path,
        &mut df_views,
    )?;

    let response = JsonDataFrameColumnResponse {
        data_frame: df_views,
        diff: Some(diff),
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_found(),
        resource: None,
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn update(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace.clone(), repo_name.clone())?;
    let file_path = PathBuf::from(path_param(&req, "path")?);
    let column_name = path_param(&req, "column_name")
        .map_err(|_| OxenHttpError::BadRequest("Column name missing in path parameters".into()))?;

    let mut body_json: Value = serde_json::from_str(&body).map_err(|_err| {
        OxenHttpError::BadRequest("Failed to parse request body into JSON".into())
    })?;

    if let Some(obj) = body_json.as_object_mut() {
        if obj.contains_key("name") {
            let name_value = obj.remove("name").unwrap(); // Safe to unwrap because we just checked it exists
            obj.insert("new_name".to_string(), name_value);
        }
        if obj.contains_key("dtype") {
            let dtype_value = obj.remove("dtype").unwrap(); // Safe to unwrap because we just checked it exists
            obj.insert("new_data_type".to_string(), dtype_value);
        }

        obj.insert("name".to_string(), json!(column_name));
    } else {
        return Err(OxenHttpError::BadRequest(
            "Request body is not a valid JSON object".into(),
        ));
    }

    let column_to_update: ColumnToUpdate = serde_json::from_value(body_json).map_err(|_err| {
        OxenHttpError::BadRequest(
            "Failed to parse ColumnToUpdate from modified request body".into(),
        )
    })?;

    log::info!(
        "Update column {namespace}/{repo_name} for file {:?} on in workspace id {}",
        file_path,
        workspace_id
    );
    log::debug!("update column with data {:?}", column_to_update);

    // Get the workspace
    let workspace = repositories::workspaces::get(&repo, &workspace_id)?;

    // Make sure the data frame is indexed
    let is_editable = repositories::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_editable {
        return Err(OxenHttpError::DatasetNotIndexed(file_path.into()));
    }

    let column_df = repositories::workspaces::data_frames::columns::update(
        &repo,
        &workspace,
        &file_path,
        &column_to_update,
    )?;

    let opts = DFOpts::empty();
    let column_schema = Schema::from_polars(&column_df.schema().clone());
    let column_df_source = DataFrameSchemaSize::from_df(&column_df, &column_schema);
    let column_df_view = JsonDataFrameView::from_df_opts(column_df, column_schema, &opts);

    let mut df_views = JsonDataFrameViews {
        source: column_df_source,
        view: column_df_view,
    };

    repositories::workspaces::data_frames::columns::decorate_fields_with_column_diffs(
        &workspace,
        &file_path,
        &mut df_views,
    )?;

    let diff =
        repositories::workspaces::data_frames::columns::get_column_diff(&workspace, &file_path)?;

    let response = JsonDataFrameColumnResponse {
        data_frame: df_views,
        diff: Some(diff),
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_found(),
        resource: None,
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn restore(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let file_path: PathBuf = PathBuf::from(path_param(&req, "path")?);

    let column_name = path_param(&req, "column_name")
        .map_err(|_| OxenHttpError::BadRequest("Column name missing in path parameters".into()))?;

    let column_to_restore: ColumnToRestore = ColumnToRestore { name: column_name };

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let workspace = repositories::workspaces::get(&repo, workspace_id)?;

    let is_editable = repositories::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_editable {
        return Err(OxenHttpError::DatasetNotIndexed(file_path.into()));
    }

    let restored_column = match repositories::workspaces::data_frames::columns::restore(
        &repo,
        &workspace,
        &file_path,
        &column_to_restore,
    ) {
        Ok(df) => df,
        Err(e) => {
            log::error!("Error restoring column: {:?}", e);
            return Err(OxenHttpError::BasicError(StringError::from(e.to_string())));
        }
    };

    let diff =
        repositories::workspaces::data_frames::columns::get_column_diff(&workspace, &file_path)?;

    let schema = Schema::from_polars(&restored_column.schema());
    log::debug!("Restored column in controller is {:?}", restored_column);

    let mut df_views = JsonDataFrameViews {
        source: DataFrameSchemaSize::from_df(&restored_column, &schema),
        view: JsonDataFrameView::from_df_opts(restored_column, schema, &DFOpts::empty()),
    };

    repositories::workspaces::data_frames::columns::decorate_fields_with_column_diffs(
        &workspace,
        &file_path,
        &mut df_views,
    )?;

    Ok(HttpResponse::Ok().json(JsonDataFrameColumnResponse {
        data_frame: df_views,
        diff: Some(diff),
        commit: None,
        derived_resource: None,
        status: StatusMessage::resource_updated(),
        resource: None,
    }))
}
