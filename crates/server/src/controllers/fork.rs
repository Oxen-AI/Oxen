use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};
use actix_web::{HttpRequest, HttpResponse, Result, web};
use liboxen::error::OxenError;
use liboxen::repositories;
use liboxen::view::StatusMessage;
use liboxen::view::fork::ForkRequest;

/// Fork a repository
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/fork",
    tag = "Fork",
    description = "Fork a repository to a new namespace, optionally with a new name.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository to fork", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository to fork", example = "yolov7-repo"),
    ),
    request_body(
        content = ForkRequest,
        description = "Fork target details, including the new namespace and optional new repository name.",
    ),
    responses(
        (status = 202, description = "Fork initiated successfully", body = StatusMessage),
        (status = 409, description = "Repository already exists at destination namespace/name", body = StatusMessage),
        (status = 404, description = "Original repository not found")
    )
)]
pub async fn fork(
    req: HttpRequest,
    body: web::Json<ForkRequest>,
) -> Result<HttpResponse, OxenHttpError> {
    log::debug!("Forking repository with request: {req:?}");
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();

    let original_repo = get_repo(app_data, &namespace, &repo_name)?;

    // Update storage path in config.toml to relative path
    original_repo.save()?;

    let new_repo_namespace = body.namespace.clone();

    let new_repo_name = body.new_repo_name.clone().unwrap_or(repo_name.clone());

    let new_repo_path = app_data.path.join(&new_repo_namespace).join(&new_repo_name);

    match repositories::fork::start_fork(original_repo.path, new_repo_path.clone()) {
        Ok(fork_start_response) => {
            log::info!("Successfully forked repository to {:?}", &new_repo_path);
            Ok(HttpResponse::Accepted().json(fork_start_response))
        }
        Err(err) => {
            log::error!("Failed to fork repository: {err:?}");
            Err(OxenHttpError::from(err))
        }
    }
}

/// Fork Status
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/fork/status",
    tag = "Fork",
    description = "Check the status of an in-progress or completed fork operation.",
    params(
        ("namespace" = String, Path, description = "Namespace of the forked repository", example = "new-user"),
        ("repo_name" = String, Path, description = "Name of the forked repository", example = "yolov7-repo"),
    ),
    responses(
        (status = 200, description = "Fork status returned successfully", body = StatusMessage),
        (status = 404, description = "Fork status not found or repository does not exist")
    )
)]
pub async fn get_status(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();

    log::debug!("Getting fork status for repo: {namespace}/{repo_name}");

    let repo_path = app_data.path.join(&namespace).join(&repo_name);

    match repositories::fork::get_fork_status(&repo_path) {
        Ok(status) => Ok(HttpResponse::Ok().json(status)),
        Err(OxenError::ForkStatusNotFound) => {
            Ok(HttpResponse::NotFound().json(StatusMessage::error("Fork status not found")))
        }
        Err(e) => {
            log::error!("Failed to get fork status: {e}");
            Err(OxenHttpError::from(e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_data::OxenAppData;

    use actix_web::http::StatusCode;
    use actix_web::test::TestRequest;
    use liboxen::config::repository_config::MerkleStoreKind;
    use liboxen::constants::MIN_OXEN_VERSION;
    use liboxen::model::LocalRepository;
    use liboxen::repositories;
    use liboxen::test as oxen_test;
    use liboxen::util;
    use liboxen::view::fork::{ForkRequest, ForkStartResponse, ForkStatus, ForkStatusResponse};
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    /// Build a sync dir on the OS temp filesystem (not the per-test `OXEN_TEST_RUN_DIR`,
    /// which is a Windows ImDisk RAMDisk on CI). LMDB depends on NT memory-section APIs
    /// that the RAMDisk does not implement — see the long-form rationale in
    /// `crates/lib/src/test/repo_prep.rs::lmdb_test_base`.
    fn lmdb_safe_sync_dir() -> Result<PathBuf, OxenError> {
        let sync_dir = std::env::temp_dir()
            .join("oxen-server-lmdb-fork-tests")
            .join(uuid::Uuid::new_v4().to_string());
        util::fs::create_dir_all(&sync_dir)?;
        Ok(sync_dir)
    }

    /// Build a fork `HttpRequest` keyed on the source `{namespace}/{repo_name}`
    /// path params, with our `OxenAppData` attached so `app_data(&req)` works.
    fn fork_request(sync_dir: &Path, source_ns: &str, source_repo: &str) -> HttpRequest {
        TestRequest::with_uri("/")
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", source_ns.to_string())
            .param("repo_name", source_repo.to_string())
            .to_http_request()
    }

    /// Build a fork-status `HttpRequest` keyed on the *destination* `{namespace}/{repo_name}`.
    fn fork_status_request(sync_dir: &Path, dst_ns: &str, dst_repo: &str) -> HttpRequest {
        TestRequest::with_uri("/")
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", dst_ns.to_string())
            .param("repo_name", dst_repo.to_string())
            .to_http_request()
    }

    /// Drive the fork controller against an LMDB-backed source repo and confirm the
    /// forked repo created under the sync dir is also LMDB-backed and exposes the
    /// same commit set as the source. This is the end-to-end version of the lib-level
    /// test in `crates/lib/src/repositories/fork.rs`, exercised through the HTTP
    /// handler and `get_repo`/`app_data` plumbing rather than the raw `start_fork`.
    #[actix_web::test]
    async fn test_fork_endpoint_preserves_lmdb_backend_and_commits() -> Result<(), OxenError> {
        oxen_test::init_test_env();

        let sync_dir = lmdb_safe_sync_dir()?;
        let source_ns = "src-ns";
        let source_repo_name = "src-repo";
        let dst_ns = "fork-ns";
        let dst_repo_name = "fork-repo";

        // 1. Create an LMDB-backed source repo under `<sync>/src-ns/src-repo`.
        let source_repo_dir = sync_dir.join(source_ns).join(source_repo_name);
        util::fs::create_dir_all(&source_repo_dir)?;
        let source_repo = repositories::init::init_with_version_and_merkle_store(
            &source_repo_dir,
            MIN_OXEN_VERSION,
            MerkleStoreKind::Lmdb,
        )?;
        assert_eq!(source_repo.merkle_store_kind(), MerkleStoreKind::Lmdb);

        // Add and commit a file so the LMDB merkle store has nodes to snapshot.
        let file_path = source_repo_dir.join("readme.txt");
        std::fs::write(&file_path, "hello from the lmdb fork test")?;
        repositories::add(&source_repo, &file_path).await?;
        let commit = repositories::commit(&source_repo, "seed commit")?;
        let source_commit_ids = vec![commit.id.clone()];

        // 2. POST /fork via the handler.
        let req = fork_request(&sync_dir, source_ns, source_repo_name);
        let body = web::Json(ForkRequest {
            namespace: dst_ns.to_string(),
            new_repo_name: Some(dst_repo_name.to_string()),
        });
        let resp = fork(req, body)
            .await
            .expect("fork handler should return Ok");
        assert_eq!(
            resp.status(),
            StatusCode::ACCEPTED,
            "fork must accept the request and start a background copy"
        );
        // Sanity-check the JSON shape so we'd notice if `start_fork` changed its
        // initial-response contract from underneath this endpoint.
        let start_body: ForkStartResponse = serde_json::from_slice(
            &actix_web::body::to_bytes(resp.into_body())
                .await
                .expect("body collected"),
        )
        .expect("response body deserializes as ForkStartResponse");
        assert_eq!(start_body.fork_status, ForkStatus::Started.to_string());

        // 3. Poll GET /fork/status until the background copy reaches a terminal
        //    state (`complete` or `failed`), tolerating the transient
        //    `started`/`counting`/`in_progress` states and a not-yet-created
        //    status file (404).
        let mut status: Option<ForkStatusResponse> = None;
        let mut attempts = 0;
        const MAX_ATTEMPTS: u32 = 30;
        while !matches!(
            status.as_ref().map(|s| s.status.as_str()),
            Some("complete" | "failed")
        ) && attempts < MAX_ATTEMPTS
        {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let status_req = fork_status_request(&sync_dir, dst_ns, dst_repo_name);
            let status_resp = get_status(status_req)
                .await
                .expect("get_status handler should return Ok");
            attempts += 1;
            // The fork-status route never returns 5xx for an in-progress fork — only
            // 200 with a body, or 404 if the destination dir hasn't been created yet.
            if status_resp.status() == StatusCode::NOT_FOUND {
                continue;
            }
            assert_eq!(status_resp.status(), StatusCode::OK);
            status = Some(
                serde_json::from_slice(
                    &actix_web::body::to_bytes(status_resp.into_body())
                        .await
                        .expect("status body collected"),
                )
                .expect("status body deserializes as ForkStatusResponse"),
            );
        }
        let status = status.expect("fork status should be readable after polling");
        // On failure, surface the background thread's error (carried in the status
        // `error` field) instead of just the opaque status string.
        assert_eq!(
            status.status, "complete",
            "fork endpoint should reach 'complete'; got {:?} (error: {:?})",
            status.status, status.error,
        );

        // 4. Open the forked repo at its destination path and verify:
        //    (a) it reports MerkleStoreKind::Lmdb, i.e. the source config.toml was copied,
        //    (b) listing commits succeeds, which only works if the snapshotted LMDB env
        //        is readable end-to-end.
        let dst_repo_path = sync_dir.join(dst_ns).join(dst_repo_name);
        let forked = LocalRepository::from_dir(&dst_repo_path)?;
        assert_eq!(
            forked.merkle_store_kind(),
            MerkleStoreKind::Lmdb,
            "forked repo must inherit MerkleStoreKind::Lmdb from the source"
        );
        let forked_commit_ids: Vec<String> = repositories::commits::list_all(&forked)?
            .into_iter()
            .map(|c| c.id)
            .collect();
        assert_eq!(
            forked_commit_ids, source_commit_ids,
            "forked repo must expose the same commits as the source"
        );

        // Release LMDB envs (mmap handles) before unlinking the dir so cleanup
        // succeeds on Windows.
        drop(forked);
        drop(source_repo);
        let _ = std::fs::remove_dir_all(&sync_dir);
        Ok(())
    }
}
