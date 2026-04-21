use actix_web::{HttpRequest, HttpResponse, web};
use liboxen::{
    command::migrate,
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
#[derive(Deserialize, Serialize, Default, utoipa::ToSchema)]
pub struct RunMigrationRequest {
    /// `"up"` (default) or `"down"`.
    #[serde(default)]
    pub direction: Option<String>,
}

/// Runs a named migration's `up` or `down` on a single repository.
///
/// The Hub is the expected caller: it enqueues per-repo Oban jobs that POST
/// here and mirrors the per-repo status in its own `repository_migrations`
/// table. OSS users can still invoke the same migrations via the existing
/// `oxen migrate up <name> <path>` CLI.
#[tracing::instrument(skip_all)]
pub async fn run(
    req: HttpRequest,
    body: Option<web::Json<RunMigrationRequest>>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let migration_name = path_param(&req, "migration_name")?.to_string();

    let direction = body
        .and_then(|b| b.into_inner().direction)
        .unwrap_or_else(|| "up".to_string());

    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    let migrations = migrate::all_migrations();
    let migration = migrations.get(&migration_name).ok_or_else(|| {
        OxenHttpError::BadRequest(format!("Unknown migration: {migration_name}").into())
    })?;

    match direction.as_str() {
        "up" => migration.up(&repo.path, false)?,
        "down" => migration.down(&repo.path, false)?,
        other => {
            return Err(OxenHttpError::BadRequest(
                format!("Unknown direction: {other} (expected \"up\" or \"down\")").into(),
            ));
        }
    }

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
    use crate::test;
    use actix_web::http;
    use actix_web::web;
    use liboxen::core::workspaces::workspace_name_index;
    use liboxen::error::OxenError;

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

        let body = web::Json(RunMigrationRequest {
            direction: Some("up".to_string()),
        });

        let resp = run(req, Some(body)).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);

        // Index should exist after migration.
        assert!(workspace_name_index::index_exists(&repo));

        test::cleanup_sync_dir(&sync_dir)?;
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

        let result = run(req, None).await;
        match result {
            Err(OxenHttpError::BadRequest(msg)) => {
                assert!(msg.to_string().contains("Unknown migration"));
            }
            other => panic!("expected BadRequest, got {other:?}"),
        }

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_run_rejects_unknown_direction() -> Result<(), OxenError> {
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
            "add_workspace_name_index",
        );

        let body = web::Json(RunMigrationRequest {
            direction: Some("sideways".to_string()),
        });

        let result = run(req, Some(body)).await;
        match result {
            Err(OxenHttpError::BadRequest(msg)) => {
                assert!(msg.to_string().contains("Unknown direction"));
            }
            other => panic!("expected BadRequest, got {other:?}"),
        }

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }
}
