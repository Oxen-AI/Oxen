use std::path::PathBuf;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, df_opts_query, path_param, DFOptsQuery, PageNumQuery};

use actix_web::{web, HttpRequest, HttpResponse};

use liboxen::constants::{self, TABLE_NAME};
use liboxen::core::db::data_frames::df_db;
use liboxen::core::db::data_frames::workspace_df_db::schema_without_oxen_cols;
use liboxen::error::OxenError;
use liboxen::model::Schema;
use liboxen::opts::DFOpts;
use liboxen::repositories;
use liboxen::util::paginate;
use liboxen::view::data_frames::DataFramePayload;
use liboxen::view::entries::ResourceVersion;
use liboxen::view::entries::{PaginatedMetadataEntries, PaginatedMetadataEntriesResponse};
use liboxen::view::json_data_frame_view::WorkspaceJsonDataFrameViewResponse;
use liboxen::view::workspaces::RenameRequest;
use liboxen::view::{
    JsonDataFrameViewResponse, JsonDataFrameViews, StatusMessage, StatusMessageDescription,
};

use actix_web::web::Bytes;
use futures_util::stream::Stream;
use std::io::{BufReader, Read};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

pub mod columns;
pub mod embeddings;
pub mod rows;

// Custom file stream that cleans up after completion
struct CleanupFileStream {
    reader: BufReader<std::fs::File>,
    temp_path: PathBuf,
    buffer: [u8; 8192], // 8KB buffer
    tx: Option<mpsc::Sender<()>>,
}

impl CleanupFileStream {
    fn new(path: PathBuf) -> std::io::Result<Self> {
        let file = std::fs::File::open(&path)?;
        let reader = BufReader::new(file);
        let (tx, _) = mpsc::channel(1);

        Ok(Self {
            reader,
            temp_path: path,
            buffer: [0; 8192],
            tx: Some(tx),
        })
    }
}

impl Stream for CleanupFileStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        match this.reader.read(&mut this.buffer) {
            Ok(0) => {
                // EOF reached - clean up the file
                if let Some(tx) = this.tx.take() {
                    let path = this.temp_path.clone();
                    tokio::spawn(async move {
                        log::debug!("removing temporary file {:?}", path);
                        if let Err(e) = std::fs::remove_file(&path) {
                            log::error!("Failed to remove temporary file: {:?}", e);
                        }
                        drop(tx); // Signal completion
                    });
                }
                Poll::Ready(None)
            }
            Ok(n) => {
                let bytes = Bytes::copy_from_slice(&this.buffer[..n]);
                Poll::Ready(Some(Ok(bytes)))
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

pub async fn get(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };
    let file_path = PathBuf::from(path_param(&req, "path")?);

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);
    opts.path = Some(file_path.clone());

    opts.page = Some(query.page.unwrap_or(constants::DEFAULT_PAGE_NUM));
    opts.page_size = Some(query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE));

    let is_indexed = repositories::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

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

    log::debug!("querying data frame {:?}", file_path);
    log::debug!("opts: {:?}", opts);
    let count = repositories::workspaces::data_frames::count(&workspace, &file_path)?;

    // Query the data frame
    let df = repositories::workspaces::data_frames::query(&workspace, &file_path, &opts)?;

    let Some(mut df_schema) =
        repositories::data_frames::schemas::get_by_path(&repo, &workspace.commit, &file_path)?
    else {
        log::error!("Failed to get schema for data frame {:?}", file_path);
        return Err(OxenHttpError::NotFound);
    };

    let resource = ResourceVersion {
        path: file_path.to_string_lossy().to_string(),
        version: workspace.commit.id.to_string(),
    };

    let og_schema = if let Some(schema) =
        repositories::data_frames::schemas::get_by_path(&repo, &workspace.commit, &resource.path)?
    {
        schema
    } else {
        Schema::from_polars(&df.schema())
    };

    df_schema.update_metadata_from_schema(&og_schema);

    let mut df_views =
        JsonDataFrameViews::from_df_and_opts_unpaginated(df, df_schema, count, &opts);

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
        is_indexed,
    };

    Ok(HttpResponse::Ok().json(response))
}

pub async fn get_schema(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };
    let file_path = PathBuf::from(path_param(&req, "path")?);

    let is_indexed = repositories::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_indexed {
        repositories::workspaces::data_frames::index(&repo, &workspace, &file_path)?;
    }

    let db_path = repositories::workspaces::data_frames::duckdb_path(&workspace, &file_path);

    let conn = df_db::get_connection(db_path)?;
    let schema = schema_without_oxen_cols(&conn, TABLE_NAME)?;

    Ok(HttpResponse::Ok().json(schema))
}

pub async fn download(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };
    let file_path = PathBuf::from(path_param(&req, "path")?);

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);
    opts.path = Some(file_path.clone());

    opts.page = Some(query.page.unwrap_or(constants::DEFAULT_PAGE_NUM));
    opts.page_size = Some(query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE));

    let is_indexed = repositories::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

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

    log::debug!("exporting data frame {:?}", file_path);
    log::debug!("opts: {:?}", opts);

    // Create temporary file
    let temp_dir = std::env::temp_dir();
    let mut extension = file_path
        .extension()
        .unwrap_or_default()
        .to_str()
        .unwrap_or_default();
    // If the user specified a format, we'll export to that format
    if let Some(output) = &opts.output {
        extension = output
            .extension()
            .unwrap_or_default()
            .to_str()
            .unwrap_or_default();
    }
    let temp_file = temp_dir.join(format!("{}.{}", uuid::Uuid::new_v4(), extension));

    // Export the data frame
    match repositories::workspaces::data_frames::export(&workspace, &file_path, &opts, &temp_file) {
        Ok(_) => (),
        Err(e) => {
            log::error!("Error exporting data frame {:?}: {:?}", file_path, e);
            let error_str = format!("{:?}", e);
            let response = StatusMessageDescription::bad_request(error_str);
            return Ok(HttpResponse::BadRequest().json(response));
        }
    };

    // Read the entire file into memory
    let mut file = std::fs::File::open(&temp_file)?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)?;

    // Remove the temporary file
    if let Err(e) = std::fs::remove_file(&temp_file) {
        log::error!("Failed to remove temporary file: {:?}", e);
    }

    // Create non-streaming response
    let filename = file_path.file_name().and_then(|n| n.to_str()).unwrap();

    Ok(HttpResponse::Ok()
        .append_header(("Content-Type", "text/csv"))
        .append_header((
            "Content-Disposition",
            format!("attachment; filename=\"{}\"", filename),
        ))
        .body(contents))
}

pub async fn download_streaming(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };
    let file_path = PathBuf::from(path_param(&req, "path")?);

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);
    opts.path = Some(file_path.clone());

    opts.page = Some(query.page.unwrap_or(constants::DEFAULT_PAGE_NUM));
    opts.page_size = Some(query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE));

    let is_indexed = repositories::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

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

    log::debug!("exporting data frame {:?}", file_path);
    log::debug!("opts: {:?}", opts);

    // Create temporary file
    let temp_dir = std::env::temp_dir();
    let extension = file_path
        .extension()
        .unwrap_or_default()
        .to_str()
        .unwrap_or_default();
    let temp_file = temp_dir.join(format!("{}.{}", uuid::Uuid::new_v4(), extension));

    // Export the data frame
    match repositories::workspaces::data_frames::export(&workspace, &file_path, &opts, &temp_file) {
        Ok(_) => (),
        Err(e) => {
            log::error!("Error exporting data frame {:?}: {:?}", file_path, e);
            let error_str = format!("{:?}", e);
            let response = StatusMessageDescription::bad_request(error_str);
            return Ok(HttpResponse::BadRequest().json(response));
        }
    };

    // Create streaming response
    let filename = file_path.file_name().and_then(|n| n.to_str()).unwrap();

    let stream = CleanupFileStream::new(temp_file)?;

    Ok(HttpResponse::Ok()
        .append_header(("Content-Type", "text/csv"))
        .append_header((
            "Content-Disposition",
            format!("attachment; filename=\"{}\"", filename),
        ))
        .streaming(stream))
}

pub async fn get_by_branch(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req).unwrap();

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let branch_name: &str = req.match_info().query("branch");
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    // Staged dataframes must be on a branch.
    let branch = repositories::branches::get_by_name(&repo, branch_name)?
        .ok_or(OxenError::remote_branch_not_found(branch_name))?;

    let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?
        .ok_or(OxenError::resource_not_found(&branch.commit_id))?;

    let entries = repositories::entries::list_tabular_files_in_repo(&repo, &commit)?;
    log::debug!("got {} tabular entries", entries.len());

    let mut editable_entries = vec![];
    for entry in entries {
        log::debug!("considering entry {:?}", entry);
        let path = PathBuf::from(&entry.filename);
        if repositories::workspaces::data_frames::is_indexed(&workspace, &path)? {
            editable_entries.push(entry);
        } else {
            log::debug!("not indexed {:?}", path);
        }
    }

    let (paginated_entries, pagination) = paginate(editable_entries, page, page_size);
    Ok(HttpResponse::Ok().json(PaginatedMetadataEntriesResponse {
        status: StatusMessage::resource_found(),
        entries: PaginatedMetadataEntries {
            entries: paginated_entries,
            pagination,
        },
    }))
}

pub async fn diff(
    req: HttpRequest,
    query: web::Query<DFOptsQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let file_path = PathBuf::from(path_param(&req, "path")?);
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let mut opts = DFOpts::empty();
    opts = df_opts_query::parse_opts(&query, &mut opts);

    opts.page = Some(query.page.unwrap_or(constants::DEFAULT_PAGE_NUM));
    opts.page_size = Some(query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE));

    let df = repositories::workspaces::data_frames::query(&workspace, &file_path, &opts)?;
    let diff_df = repositories::workspaces::data_frames::diff(&workspace, &file_path)?;
    let mut df_schema =
        repositories::workspaces::data_frames::schemas::get_by_path(&workspace, &file_path)?;

    let resource = ResourceVersion {
        path: file_path.to_string_lossy().to_string(),
        version: workspace.commit.id.to_string(),
    };

    let og_schema = if let Some(schema) =
        repositories::data_frames::schemas::get_by_path(&repo, &workspace.commit, resource.path)?
    {
        schema
    } else {
        Schema::from_polars(&df.schema())
    };

    df_schema.update_metadata_from_schema(&og_schema);

    let mut df_views = JsonDataFrameViews::from_df_and_opts(diff_df, df_schema, &opts);

    repositories::workspaces::data_frames::columns::decorate_fields_with_column_diffs(
        &workspace,
        &file_path,
        &mut df_views,
    )?;

    let resource = ResourceVersion {
        path: file_path.to_string_lossy().to_string(),
        version: workspace.commit.id.to_string(),
    };

    let resource = JsonDataFrameViewResponse {
        data_frame: df_views,
        status: StatusMessage::resource_found(),
        resource: Some(resource),
        commit: None,
        derived_resource: None,
    };

    Ok(HttpResponse::Ok().json(resource))
}

pub async fn put(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let file_path = PathBuf::from(path_param(&req, "path")?);

    log::debug!("workspace {} data frame put {:?}", workspace_id, file_path);
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };
    let data: DataFramePayload = serde_json::from_str(&body)?;
    log::debug!("workspace {} data frame put {:?}", workspace_id, data);

    let to_index = data.is_indexed;
    let is_indexed = repositories::workspaces::data_frames::is_indexed(&workspace, &file_path)?;

    if !is_indexed && to_index {
        repositories::workspaces::data_frames::index(&repo, &workspace, &file_path)?;
    } else if is_indexed && !to_index {
        repositories::workspaces::data_frames::unindex(&workspace, &file_path)?;
    }

    Ok(HttpResponse::Ok().json(StatusMessage::resource_updated()))
}

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let file_path = PathBuf::from(path_param(&req, "path")?);
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    repositories::workspaces::data_frames::restore(&repo, &workspace, file_path)?;

    Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
}

pub async fn rename(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let path = PathBuf::from(path_param(&req, "path")?);
    // Attempt to parse the body
    let body: RenameRequest = serde_json::from_str(&body)?; // Use the Json wrapper to get the inner value

    // Check if new_path is valid
    if body.new_path.is_empty() {
        return Err(OxenHttpError::BadRequest("new_path cannot be empty".into()));
    }

    let new_path = PathBuf::from(body.new_path);

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    if repositories::entries::get_file(&repo, &workspace.commit, &new_path)?.is_some() {
        return Err(OxenHttpError::BadRequest("new_path already exists".into()));
    }

    repositories::workspaces::data_frames::rename(&workspace, &path, &new_path).await?;

    Ok(HttpResponse::Ok().json(StatusMessage::resource_updated()))
}
