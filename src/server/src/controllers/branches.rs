use std::path::PathBuf;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param, PageNumQuery};

use actix_web::{web, HttpRequest, HttpResponse};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::util::{self, paginate};
use liboxen::view::entries::ResourceVersion;
use liboxen::view::{
    BranchLockResponse, BranchNewFromBranchName, BranchNewFromCommitId, BranchRemoteMerge,
    BranchResponse, BranchUpdate, CommitEntryVersion, CommitResponse, ListBranchesResponse,
    PaginatedEntryVersions, PaginatedEntryVersionsResponse, StatusMessage,
};
use liboxen::{constants, repositories};

pub async fn index(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, name)?;

    let branches = repositories::branches::list(&repo)?;

    let view = ListBranchesResponse {
        status: StatusMessage::resource_found(),
        branches,
    };
    Ok(HttpResponse::Ok().json(view))
}

pub async fn show(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let branch = repositories::branches::get_by_name(&repository, &branch_name)?
        .ok_or(OxenError::remote_branch_not_found(&branch_name))?;

    let view = BranchResponse {
        status: StatusMessage::resource_found(),
        branch,
    };

    Ok(HttpResponse::Ok().json(view))
}

pub async fn create(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    // Try to deserialize the body into a BranchNewFromBranchName
    let data: Result<BranchNewFromBranchName, serde_json::Error> = serde_json::from_str(&body);
    if let Ok(data) = data {
        return create_from_branch(&repo, &data);
    }

    // Try to deserialize the body into a BranchNewFromCommitId
    let data: Result<BranchNewFromCommitId, serde_json::Error> = serde_json::from_str(&body);
    if let Ok(data) = data {
        return create_from_commit(&repo, &data);
    }

    Ok(HttpResponse::BadRequest().json(StatusMessage::error("Invalid request body")))
}

fn create_from_branch(
    repo: &LocalRepository,
    data: &BranchNewFromBranchName,
) -> Result<HttpResponse, OxenHttpError> {
    let maybe_new_branch: Option<liboxen::model::Branch> =
        repositories::branches::get_by_name(repo, &data.new_name)?;
    if let Some(branch) = maybe_new_branch {
        let view = BranchResponse {
            status: StatusMessage::resource_found(),
            branch,
        };
        return Ok(HttpResponse::Ok().json(view));
    }

    let from_branch = repositories::branches::get_by_name(repo, &data.from_name)?
        .ok_or(OxenHttpError::NotFound)?;

    let new_branch = repositories::branches::create(repo, &data.new_name, from_branch.commit_id)?;

    Ok(HttpResponse::Ok().json(BranchResponse {
        status: StatusMessage::resource_created(),
        branch: new_branch,
    }))
}

fn create_from_commit(
    repo: &LocalRepository,
    data: &BranchNewFromCommitId,
) -> Result<HttpResponse, OxenHttpError> {
    let new_branch = repositories::branches::create(repo, &data.new_name, &data.commit_id)?;

    Ok(HttpResponse::Ok().json(BranchResponse {
        status: StatusMessage::resource_created(),
        branch: new_branch,
    }))
}

pub async fn delete(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let branch = repositories::branches::get_by_name(&repository, &branch_name)?
        .ok_or(OxenError::remote_branch_not_found(&branch_name))?;

    repositories::branches::force_delete(&repository, &branch.name)?;
    Ok(HttpResponse::Ok().json(BranchResponse {
        status: StatusMessage::resource_deleted(),
        branch,
    }))
}

pub async fn update(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let data: Result<BranchUpdate, serde_json::Error> = serde_json::from_str(&body);
    let data = data.map_err(|err| OxenHttpError::BadRequest(format!("{:?}", err).into()))?;

    let branch = repositories::branches::update(&repository, branch_name, data.commit_id)?;

    Ok(HttpResponse::Ok().json(BranchResponse {
        status: StatusMessage::resource_updated(),
        branch,
    }))
}
pub async fn maybe_create_merge(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;
    let branch_name = path_param(&req, "branch_name")?;
    let branch = repositories::branches::get_by_name(&repository, &branch_name)?
        .ok_or(OxenError::remote_branch_not_found(&branch_name))?;

    let data: Result<BranchRemoteMerge, serde_json::Error> = serde_json::from_str(&body);
    let data = data.map_err(|err| OxenHttpError::BadRequest(format!("{:?}", err).into()))?;
    let incoming_commit_id = data.client_commit_id;
    let incoming_commit = repositories::commits::get_by_id(&repository, &incoming_commit_id)?
        .ok_or(OxenError::resource_not_found(&incoming_commit_id))?;

    let current_commit_id = data.server_commit_id;
    let current_commit = repositories::commits::get_by_id(&repository, &current_commit_id)?
        .ok_or(OxenError::resource_not_found(&current_commit_id))?;

    log::debug!(
        "maybe_create_merge got client head commit {:?}",
        incoming_commit_id
    );

    let maybe_merge_commit = repositories::merge::merge_commit_into_base_on_branch(
        &repository,
        &incoming_commit,
        &current_commit,
        &branch,
    )?;

    // Return what will become the new head of the repo after push is complete.
    if let Some(merge_commit) = maybe_merge_commit {
        log::debug!("returning merge commit {:?}", merge_commit);
        // Update branch head
        Ok(HttpResponse::Ok().json(CommitResponse {
            status: StatusMessage::resource_created(),
            commit: merge_commit,
        }))
    } else {
        // If there are merge conflicts, we can't complete this merge and want to reset the branch to the previous remote head
        // as if this push never happened
        log::debug!("returning current commit {:?}.", current_commit_id);
        Ok(HttpResponse::Ok().json(CommitResponse {
            status: StatusMessage::resource_found(),
            commit: current_commit,
        }))
    }
}
pub async fn latest_synced_commit(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let commit = repositories::branches::latest_synced_commit(&repository, &branch_name)?;

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_found(),
        commit,
    }))
}

pub async fn lock(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    match repositories::branches::lock(&repository, &branch_name) {
        Ok(_) => Ok(HttpResponse::Ok().json(BranchLockResponse {
            status: StatusMessage::resource_updated(),
            branch_name: branch_name.clone(),
            is_locked: true,
        })),
        Err(e) => {
            // Log the error for debugging
            log::error!("Failed to lock branch: {}", e);

            Ok(HttpResponse::Conflict().json(BranchLockResponse {
                status: StatusMessage::error(e.to_string()),
                branch_name: branch_name.clone(),
                is_locked: false,
            }))
        }
    }
}

pub async fn unlock(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    repositories::branches::unlock(&repository, &branch_name)?;

    Ok(HttpResponse::Ok().json(BranchLockResponse {
        status: StatusMessage::resource_updated(),
        branch_name,
        is_locked: false,
    }))
}

pub async fn is_locked(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let is_locked = repositories::branches::is_locked(&repository, &branch_name)?;

    Ok(HttpResponse::Ok().json(BranchLockResponse {
        status: StatusMessage::resource_found(),
        branch_name,
        is_locked,
    }))
}

pub async fn list_entry_versions(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;

    // Get branch
    let repo = get_repo(&app_data.path, namespace.clone(), &repo_name)?;
    let branch = repositories::branches::get_by_name(&repo, &branch_name)?
        .ok_or(OxenError::remote_branch_not_found(&branch_name))?;

    let path = PathBuf::from(path_param(&req, "path")?);
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;

    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    let commits_with_versions =
        repositories::branches::list_entry_versions_on_branch(&repo, &branch.name, &path)?;

    let mut commit_versions: Vec<CommitEntryVersion> = Vec::new();

    for (commit, entry) in commits_with_versions {
        // For each version, get the schema hash if one exists.
        let maybe_schema_hash = if util::fs::is_tabular(&entry.path) {
            let maybe_schema =
                repositories::data_frames::schemas::get_by_path(&repo, &commit, &entry.path)?;
            match maybe_schema {
                Some(schema) => Some(schema.hash),
                None => {
                    log::error!("Could not get schema for tabular file {:?}", entry.path);
                    None
                }
            }
        } else {
            None
        };

        commit_versions.push(CommitEntryVersion {
            commit: commit.clone(),
            resource: ResourceVersion {
                version: commit.id.clone(),
                path: entry.path.to_string_lossy().into(),
            },
            schema_hash: maybe_schema_hash,
        });
    }

    let (paginated_commit_versions, pagination) = paginate(commit_versions, page, page_size);

    let response = PaginatedEntryVersionsResponse {
        status: StatusMessage::resource_found(),
        versions: PaginatedEntryVersions {
            versions: paginated_commit_versions,
            pagination,
        },
        branch,
        path,
    };

    Ok(HttpResponse::Ok().json(response))
}

#[cfg(test)]
mod tests {

    use actix_web::http::{self};

    use actix_web::body::to_bytes;

    use liboxen::constants::DEFAULT_BRANCH_NAME;
    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;
    use liboxen::view::http::STATUS_SUCCESS;
    use liboxen::view::{
        BranchNewFromBranchName, BranchResponse, CommitResponse, ListBranchesResponse,
    };

    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_controllers_branches_index_empty() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();
        let namespace = "Testing-Namespace";
        let name = "Testing-Branches-1";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file)?;
        repositories::commit(&repo, "First commit")?;
        let uri = format!("/oxen/{namespace}/{name}/branches");
        let req = test::repo_request(&sync_dir, queue, &uri, namespace, name);

        let resp = controllers::branches::index(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListBranchesResponse = serde_json::from_str(text)?;
        assert_eq!(list.status.status, STATUS_SUCCESS);
        // Should have main branch initialized
        assert_eq!(list.branches.len(), 1);
        assert_eq!(list.branches.first().unwrap().name, DEFAULT_BRANCH_NAME);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_branches_index_multiple_branches() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();
        let namespace = "Testing-Namespace";
        let name = "Testing-Branches-1";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file)?;
        repositories::commit(&repo, "First commit")?;
        repositories::branches::create_from_head(&repo, "branch-1")?;
        repositories::branches::create_from_head(&repo, "branch-2")?;

        let uri = format!("/oxen/{namespace}/{name}/branches");
        let req = test::repo_request(&sync_dir, queue, &uri, namespace, name);

        let resp = controllers::branches::index(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListBranchesResponse = serde_json::from_str(text)?;
        // main + branch-1 + branch-2
        assert_eq!(list.branches.len(), 3);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_branch_show() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Branches-1";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file)?;
        repositories::commit(&repo, "First commit")?;
        let branch_name = "branch-1";
        repositories::branches::create_from_head(&repo, branch_name)?;

        let uri = format!("/oxen/{namespace}/{repo_name}/branches");
        let req = test::repo_request_with_param(
            &sync_dir,
            queue,
            &uri,
            namespace,
            repo_name,
            "branch_name",
            branch_name,
        );

        let resp = controllers::branches::show(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let branch_resp: BranchResponse = serde_json::from_str(text)?;
        assert_eq!(branch_resp.branch.name, branch_name);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_branch_create() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();
        let namespace = "Testing-Namespace";
        let name = "Testing-Branches-Create";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file)?;
        repositories::commit(&repo, "First commit")?;

        let new_name = "My-Branch-Name";

        let params = BranchNewFromBranchName {
            new_name: new_name.to_string(),
            from_name: DEFAULT_BRANCH_NAME.to_string(),
        };
        let uri = format!("/oxen/{namespace}/{name}/branches");
        let req = test::repo_request(&sync_dir, queue, &uri, namespace, name);

        let resp = controllers::branches::create(req, serde_json::to_string(&params)?)
            .await
            .map_err(|_err| OxenError::basic_str("OxenHttpError - could not create branch"))?;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();

        let repo_response: BranchResponse = serde_json::from_str(text)?;
        assert_eq!(repo_response.status.status, STATUS_SUCCESS);
        assert_eq!(repo_response.branch.name, "My-Branch-Name");

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_branch_get_latest() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Branches-1";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file)?;
        repositories::commit(&repo, "First commit")?;
        let branch_name = "branch-1";
        repositories::branches::create_from_head(&repo, branch_name)?;

        // Get head commit through local API
        let created_branch = repositories::branches::get_by_name(&repo, branch_name)?
            .ok_or(OxenError::remote_branch_not_found(branch_name))?;

        let uri = format!("/oxen/{namespace}/{repo_name}/branches/");
        let req = test::repo_request_with_param(
            &sync_dir,
            queue,
            &uri,
            namespace,
            repo_name,
            "branch_name",
            branch_name,
        );

        let resp = controllers::branches::latest_synced_commit(req)
            .await
            .unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let commit_resp: CommitResponse = serde_json::from_str(text)?;
        assert_eq!(commit_resp.commit.id, created_branch.commit_id);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
