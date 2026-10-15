use crate::errors::OxenHttpError;
use crate::helpers::get_repo_async;
use crate::params::{PageNumQuery, app_data, path_param};
use crate::tasks;

use liboxen::constants;
use liboxen::core::repo_locks;
use liboxen::core::staged::get_staged_db_manager;
use liboxen::error::OxenError;
use liboxen::model::Workspace;
use liboxen::repositories;
use liboxen::util;
use liboxen::view::remote_staged_status::RemoteStagedStatus;
use liboxen::view::{
    FilePathsResponse, RemoteStagedStatusResponse, StatusMessage, StatusMessageDescription,
};

use actix_web::{HttpRequest, HttpResponse, web};

use std::path::{Path, PathBuf};

/// List staged changes in a workspace
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/changes",
    description = "List the staged changes (added, modified, and removed files) in a workspace. The added, modified, and removed lists are each paginated independently, with the same page and page_size applied to each list.",
    tag = "Workspace Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "The UUID of the workspace", example = "580c0587-c157-417b-9118-8686d63d2745"),
        ("page" = Option<usize>, Query, description = "Page number for pagination (default 1)"),
        ("page_size" = Option<usize>, Query, description = "Number of entries per page (default 100, must be at least 1)")
    ),
    responses(
        (status = 200, description = "Staged changes in the workspace", body = RemoteStagedStatusResponse),
        (status = 400, description = "Invalid page_size"),
        (status = 404, description = "Workspace not found")
    )
)]
pub async fn list_root(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();
    log::debug!("/changes looking up repo: {namespace}/{repo_name}");

    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;
    let page_num = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    if page_size == 0 {
        return Err(OxenHttpError::BadRequest(
            "page_size must be at least 1".into(),
        ));
    }

    log::debug!("/changes looking up workspace_id: {workspace_id}");
    let Some(workspace) = repositories::workspaces::get_async(&repo, &workspace_id).await? else {
        log::debug!("/changes could not find workspace_id: {workspace_id}");
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };
    staged_status_response(workspace, Path::new("."), page_num, page_size).await
}

/// List staged changes under a directory in a workspace
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/changes/{path}",
    description = "List the staged changes (added, modified, and removed files) under a directory in a workspace. The added, modified, and removed lists are each paginated independently, with the same page and page_size applied to each list.",
    tag = "Workspace Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "The UUID of the workspace", example = "580c0587-c157-417b-9118-8686d63d2745"),
        ("path" = String, Path, description = "The directory to list staged changes under", example = "images/train"),
        ("page" = Option<usize>, Query, description = "Page number for pagination (default 1)"),
        ("page_size" = Option<usize>, Query, description = "Number of entries per page (default 100, must be at least 1)")
    ),
    responses(
        (status = 200, description = "Staged changes under the directory", body = RemoteStagedStatusResponse),
        (status = 400, description = "Invalid page_size"),
        (status = 404, description = "Workspace not found")
    )
)]
pub async fn list(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();
    log::debug!("/changes looking up repo: {namespace}/{repo_name}");

    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;
    let path = PathBuf::from(path_param(&req, "path")?);
    let page_num = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    if page_size == 0 {
        return Err(OxenHttpError::BadRequest(
            "page_size must be at least 1".into(),
        ));
    }

    log::debug!("/changes looking up workspace_id: {workspace_id}");
    let Some(workspace) = repositories::workspaces::get_async(&repo, &workspace_id).await? else {
        log::debug!("/changes could not find workspace_id: {workspace_id}");
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };
    staged_status_response(workspace, &path, page_num, page_size).await
}

/// The page of `workspace`'s staged changes under `path`, as the changes endpoints answer it.
async fn staged_status_response(
    workspace: Workspace,
    path: &Path,
    page_num: usize,
    page_size: usize,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let staged = repositories::workspaces::status::status_from_dir_async(&workspace, path).await?;

    staged.print();

    let staged = tasks::spawn_blocking(move || {
        RemoteStagedStatus::from_staged(&workspace.workspace_repo, &staged, page_num, page_size)
    })
    .await
    .map_err(OxenError::from)?;
    let response = RemoteStagedStatusResponse {
        status: StatusMessage::resource_found(),
        staged,
    };
    Ok(HttpResponse::Ok().json(response))
}

/// Unstage a file from the workspace
#[utoipa::path(
    delete,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/changes/{path}",
    description = "Unstage a file from workspace staging",
    tag = "Workspace Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "The UUID of the workspace", example = "580c0587-c157-417b-9118-8686d63d2745"),
        ("path" = String, Path, description = "The path to the file to delete (unstage)", example = "images/train/dog_1.jpg")
    ),
    responses(
        (status = 200, description = "File marked for deletion", body = StatusMessage),
        (status = 404, description = "Workspace or File not found")
    )
)]
pub async fn unstage(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();
    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;
    let _write = repo_locks::begin_write(&repo)?;
    let path = PathBuf::from(path_param(&req, "path")?);

    let Some(workspace) = repositories::workspaces::get_async(&repo, &workspace_id).await? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    // This may not be in the commit if it's added, so have to parse tabular-ness from the path.
    if util::fs::is_tabular(&path) {
        repositories::workspaces::data_frames::restore(&repo, &workspace, &path).await?;
        return Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()));
    }
    let unstaged = tasks::spawn_blocking(move || unstage_file(&workspace, &path))
        .await
        .map_err(OxenError::from)??;
    if unstaged {
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else {
        Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
    }
}

/// Unstage files
#[utoipa::path(
    delete,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/changes",
    description = "Unstage files from a workspace. Accepts both files and directories.",
    tag = "Workspace Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "The UUID of the workspace", example = "580c0587-c157-417b-9118-8686d63d2745")
    ),
    request_body(
        content = Vec<String>,
        description = "List of paths to unstage from the workspace staging area",
        example = json!(["images/train/revert_me.jpg", "data/config.json"])
    ),
    responses(
        (status = 200, description = "Files unstaged from staging", body = StatusMessage),
        (status = 206, description = "Some files could not be unstaged (returns paths of files not found)", body = FilePathsResponse),
        (status = 404, description = "Workspace not found")
    )
)]
pub async fn unstage_many(
    req: HttpRequest,
    payload: web::Json<Vec<PathBuf>>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();
    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;
    let _write = repo_locks::begin_write(&repo)?;
    log::debug!("unstage_many found repo {repo_name}, workspace_id {workspace_id}");

    let Some(workspace) = repositories::workspaces::get_async(&repo, &workspace_id).await? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let paths_to_remove: Vec<PathBuf> = payload.into_inner();

    // Note: we can't delete the version file here because it may be
    // referenced elsewhere. In order to cleanup eagerly here we would
    // need the staged DB to track whether the version was newly added
    // or already existed.
    let pass_workspace = workspace.clone();
    let leftovers = tasks::spawn_blocking(move || -> Result<Vec<Leftover>, OxenError> {
        // One handle for the whole pass, which the unstages below share rather than reopen.
        let staged_db = get_staged_db_manager(&pass_workspace.workspace_repo)?;
        let mut leftovers = vec![];
        for path in paths_to_remove {
            match staged_db.read_from_staged_db(&path) {
                Ok(Some(_)) => {}
                Ok(None) => continue,
                Err(e) => {
                    tracing::error!(oxen.file_path = %path.display(), exception.message = ?e, "Failed to read a staged entry");
                    leftovers.push(Leftover::Failed(path));
                    continue;
                }
            }
            // This may not be in the commit if it's added, so have to parse tabular-ness from
            // the path.
            if util::fs::is_tabular(&path) {
                leftovers.push(Leftover::DataFrame(path));
            } else if let Err(e) = unstage_file(&pass_workspace, &path) {
                log::debug!("Failed to unstage file {path:?}: {e:?}");
                leftovers.push(Leftover::Failed(path));
            }
        }
        Ok(leftovers)
    })
    .await
    .map_err(OxenError::from)??;

    let mut err_paths = vec![];
    for leftover in leftovers {
        match leftover {
            Leftover::Failed(path) => err_paths.push(path),
            Leftover::DataFrame(path) => {
                if let Err(e) =
                    repositories::workspaces::data_frames::restore(&repo, &workspace, &path).await
                {
                    log::debug!("Failed to unstage file {path:?}: {e:?}");
                    err_paths.push(path);
                }
            }
        }
    }

    if err_paths.is_empty() {
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else {
        Ok(HttpResponse::PartialContent().json(FilePathsResponse {
            paths: err_paths,
            status: StatusMessage::resource_not_found(),
        }))
    }
}

/// A staged path the blocking pass of `unstage_many` left unfinished, in request order.
enum Leftover {
    /// A file whose staged entry could not be read, or whose unstage failed.
    Failed(PathBuf),
    /// A data frame, restored on the async side.
    DataFrame(PathBuf),
}

/// Unstage the non-tabular file at `path`, or `false` if the workspace has nothing staged there.
fn unstage_file(workspace: &Workspace, path: &Path) -> Result<bool, OxenError> {
    if !repositories::workspaces::files::exists(workspace, path)? {
        return Ok(false);
    }
    repositories::workspaces::files::unstage(workspace, path)?;
    Ok(true)
}
