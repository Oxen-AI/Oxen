use actix_web::{HttpRequest, HttpResponse, web};
use liboxen::{
    command::migrate::{self, Direction, try_apply_migration},
    core::repo_locks,
    error::OxenError,
    migrations,
    view::{ListRepositoryResponse, StatusMessage},
};
use serde::{Deserialize, Serialize};

use crate::{
    errors::OxenHttpError,
    helpers::get_repo,
    params::{app_data, path_param},
};

pub async fn list_unmigrated(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    log::debug!("in the list_unmigrated controller");
    let app_data = app_data(&req)?;
    let migration_tstamp = path_param(&req, "migration_tstamp")?.to_string();

    let unmigrated_repos =
        migrations::list_unmigrated(&app_data.path, migration_tstamp.to_string())?;

    let view = ListRepositoryResponse {
        status: StatusMessage::resource_found(),
        repositories: unmigrated_repos,
    };

    Ok(HttpResponse::Ok().json(view))
}

/// Request body for `POST /api/repos/:namespace/:repo_name/migrations/:migration_name`.
///
/// `deny_unknown_fields` makes serde reject bodies with extra keys, so a typo
/// like `{"directiom": "up"}` becomes a 400 instead of silently running with
/// defaults.
#[derive(Deserialize, Serialize, Default, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RunMigrationRequest {
    /// `"up"` (default) or `"down"`.
    #[serde(default)]
    pub direction: Direction,

    /// If true, then run the migration if it is applicable but not required.
    /// If false, then only required up migrations are run. Down migrations are
    /// always run. Defaults to false if unspecified.
    #[serde(default)]
    pub run_optional: bool,
}

/// Runs a named migration's `up` or `down` on a single repository.
///
/// The Hub is the expected caller: it enqueues per-repo Oban jobs that POST
/// here and mirrors the per-repo status in its own `repository_migrations`
/// table. OSS users can still invoke the same migrations via the existing
/// `oxen migrate up <name> <path>` CLI.
#[tracing::instrument(skip_all)]
pub async fn run(req: HttpRequest, body: web::Bytes) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    // parse path params
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let migration_name = path_param(&req, "migration_name")?.to_string();

    // Parse the body ourselves rather than via `web::Json<_>` so we can tell
    // "no body at all" (use defaults) apart from "body is present but bad"
    // (return 400). `Option<web::Json<_>>` collapses both cases to `None`.
    let RunMigrationRequest {
        direction,
        run_optional,
    } = if body.is_empty() {
        RunMigrationRequest::default()
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| OxenHttpError::BadRequest(format!("Invalid request body: {e}").into()))?
    };

    let migration = migrate::all_migrations(&migration_name).ok_or_else(|| {
        OxenHttpError::BadRequest(format!("Unknown migration: {migration_name}").into())
    })?;

    let repo = get_repo(app_data, &namespace, &repo_name)?;

    // Run the migration with the repo to itself: the exclusive lock blocks new writers and drains
    // in-flight ones before `up`/`down` runs, and serializes two concurrent migration POSTs for the
    // same repo. Returns HTTP 429 if in-flight writes don't drain in time. The synchronous transcode
    // runs on the blocking pool so it doesn't starve other requests on the actix worker.
    let migration_repo = repo.clone();
    repo_locks::with_repo_exclusive(&repo, async move {
        tokio::task::spawn_blocking(move || {
            try_apply_migration(migration, direction, run_optional, migration_repo)
        })
        .await
        .map_err(OxenError::from)?
    })
    .await?;

    log::info!(
        "Ran migration {migration_name} {direction} on {namespace}/{repo_name}",
        migration_name = migration_name,
        direction = direction,
        namespace = namespace,
        repo_name = repo_name,
    );

    Ok(HttpResponse::Ok().json(StatusMessage::resource_updated()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_data::OxenAppData;
    use crate::test;
    use actix_web::{App, http, web};
    use liboxen::core::workspaces::workspace_name_index;
    use liboxen::error::OxenError;
    use std::time::Duration;

    #[actix_web::test]
    async fn test_run_up_on_single_repo() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Repo";

        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        // Create a workspace so the index migration has something to rebuild.
        let workspaces_dir = liboxen::model::Workspace::workspaces_dir(&repo);
        std::fs::create_dir_all(&workspaces_dir)?;

        let req = test::repo_request_with_param(
            &sync_dir,
            "/",
            namespace,
            repo_name,
            "migration_name",
            "add_workspace_name_index",
        );

        let body = web::Bytes::from(
            serde_json::to_vec(&RunMigrationRequest {
                direction: Direction::Up,
                run_optional: false,
            })
            .expect("RunMigrationRequest is always serializable"),
        );

        let resp = run(req, body).await.expect("run handler should succeed");
        assert_eq!(resp.status(), http::StatusCode::OK);

        // Index should exist after migration.
        assert!(workspace_name_index::index_exists(&repo));

        test::cleanup_repo_and_sync_dir(repo, &sync_dir)?;
        Ok(())
    }

    /// The migration runs under the repo's exclusive lock: while a write reservation is
    /// outstanding it waits for that write to drain before running, rather than racing it. Guards
    /// the `with_repo_exclusive` wiring — the happy-path test above passes with or without the
    /// wrap, so this is what would fail if the lock were dropped.
    #[actix_web::test]
    async fn test_run_waits_for_in_flight_writes_to_drain() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Repo";

        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let workspaces_dir = liboxen::model::Workspace::workspaces_dir(&repo);
        std::fs::create_dir_all(&workspaces_dir)?;

        // Reserve a write on the same lock gate the handler targets (gates are keyed by repo path,
        // so resolve the repo exactly as `get_repo` does), giving the exclusive acquire something
        // to drain.
        let handler_repo = liboxen::repositories::get_by_namespace_and_name(
            &sync_dir, namespace, repo_name, None,
        )?
        .expect("repo should exist");
        let guard = repo_locks::acquire_write(&handler_repo)?;

        let req = test::repo_request_with_param(
            &sync_dir,
            "/",
            namespace,
            repo_name,
            "migration_name",
            "add_workspace_name_index",
        );
        let migration = run(req, web::Bytes::new());
        tokio::pin!(migration);

        // The migration must not complete while the write is in flight.
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut migration)
                .await
                .is_err(),
            "migration ran while a write was in flight — exclusive lock not wired"
        );

        // Draining the write lets the migration proceed.
        drop(guard);
        let resp = migration
            .await
            .expect("run handler should succeed after drain");
        assert_eq!(resp.status(), http::StatusCode::OK);
        assert!(workspace_name_index::index_exists(&repo));

        test::cleanup_repo_and_sync_dir(repo, &sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_run_rejects_unknown_migration() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Repo";

        let _repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let req = test::repo_request_with_param(
            &sync_dir,
            "/",
            namespace,
            repo_name,
            "migration_name",
            "does_not_exist",
        );

        let result = run(req, web::Bytes::new()).await;
        match result {
            Err(OxenHttpError::BadRequest(msg)) => {
                assert!(msg.to_string().contains("Unknown migration"));
            }
            other => panic!("expected BadRequest, got {other:?}"),
        }

        test::cleanup_repo_and_sync_dir(_repo, &sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_run_rejects_unknown_direction() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Repo";

        let _repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        // Drive a real App so the `web::Json<RunMigrationRequest>` extractor
        // runs serde on the body — `Direction` rejects unknown variants, so
        // the handler should never be reached and the response should be 400.
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/api/repos/{namespace}/{repo_name}/migrations/{migration_name}",
                    web::post().to(run),
                ),
        )
        .await;

        let uri = format!("/api/repos/{namespace}/{repo_name}/migrations/add_workspace_name_index");
        let req = actix_web::test::TestRequest::post()
            .uri(&uri)
            .set_json(serde_json::json!({ "direction": "sideways" }))
            .to_request();

        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);

        test::cleanup_repo_and_sync_dir(_repo, &sync_dir)?;
        Ok(())
    }

    /// Body with an extra/unknown field must be rejected (not silently
    /// ignored). Relies on `#[serde(deny_unknown_fields)]` on
    /// `RunMigrationRequest`.
    #[actix_web::test]
    async fn test_run_rejects_unknown_field() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Repo";
        let _repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let body = web::Bytes::from(r#"{"direction":"up","not_a_real_field":42}"#);
        let req = test::repo_request_with_param(
            &sync_dir,
            "/",
            namespace,
            repo_name,
            "migration_name",
            "add_workspace_name_index",
        );

        match run(req, body).await {
            Err(OxenHttpError::BadRequest(msg)) => {
                let msg = msg.to_string();
                assert!(
                    msg.contains("Invalid request body") && msg.contains("not_a_real_field"),
                    "expected error to mention the unknown field, got: {msg}"
                );
            }
            other => panic!("expected BadRequest for unknown field, got {other:?}"),
        }

        test::cleanup_repo_and_sync_dir(_repo, &sync_dir)?;
        Ok(())
    }

    /// Body with a correctly-named field but the wrong JSON type
    /// (e.g. a string where a bool is expected) must be rejected.
    #[actix_web::test]
    async fn test_run_rejects_wrong_field_type() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Repo";
        let _repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let body = web::Bytes::from(r#"{"run_optional":"yes"}"#);
        let req = test::repo_request_with_param(
            &sync_dir,
            "/",
            namespace,
            repo_name,
            "migration_name",
            "add_workspace_name_index",
        );

        match run(req, body).await {
            Err(OxenHttpError::BadRequest(msg)) => {
                assert!(
                    msg.to_string().contains("Invalid request body"),
                    "expected \"Invalid request body\" prefix, got: {msg}"
                );
            }
            other => panic!("expected BadRequest for wrong type, got {other:?}"),
        }

        test::cleanup_repo_and_sync_dir(_repo, &sync_dir)?;
        Ok(())
    }

    /// Malformed JSON (not parseable at all) must be rejected. Empty body
    /// is allowed and falls back to defaults — see
    /// `test_run_rejects_unknown_migration` for the empty-body path.
    #[actix_web::test]
    async fn test_run_rejects_malformed_json() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Repo";
        let _repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let body = web::Bytes::from_static(b"{not json");
        let req = test::repo_request_with_param(
            &sync_dir,
            "/",
            namespace,
            repo_name,
            "migration_name",
            "add_workspace_name_index",
        );

        match run(req, body).await {
            Err(OxenHttpError::BadRequest(msg)) => {
                assert!(
                    msg.to_string().contains("Invalid request body"),
                    "expected \"Invalid request body\" prefix, got: {msg}"
                );
            }
            other => panic!("expected BadRequest for malformed JSON, got {other:?}"),
        }

        test::cleanup_repo_and_sync_dir(_repo, &sync_dir)?;
        Ok(())
    }
}
