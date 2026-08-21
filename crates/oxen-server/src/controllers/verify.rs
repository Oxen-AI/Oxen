use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::{HttpRequest, HttpResponse};
use liboxen::repositories;
use liboxen::repositories::verify::VerifyReport;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct VerifyResponse {
    pub status_message: String,
    pub report: VerifyReport,
}

/// POST /verify
///
/// Report the repository's commit, merkle tree, and version file corruption. Reads only, so it is
/// safe to run against a live repository and safe to run in a loop across many of them.
pub async fn verify(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repo = get_repo(app_data, &namespace, &repo_name)?;

    let report = repositories::verify::verify_repo(&repo).await?;

    let status_message = if report.is_healthy() {
        format!("Verified {namespace}/{repo_name}; no problems found")
    } else {
        format!(
            "Verified {namespace}/{repo_name}; {} problem(s) found",
            report.total_findings()
        )
    };
    if !report.is_healthy() {
        log::warn!(
            "verify: {namespace}/{repo_name} found {} problem(s)",
            report.total_findings()
        );
    }

    Ok(HttpResponse::Ok().json(VerifyResponse {
        status_message,
        report,
    }))
}

#[cfg(test)]
mod tests {
    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;
    use actix_web::{App, web};

    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;

    use super::VerifyResponse;

    /// Run the endpoint against `repo` and hand back the deserialized response.
    async fn call_verify(
        sync_dir: &std::path::Path,
        namespace: &str,
        name: &str,
    ) -> Result<VerifyResponse, OxenError> {
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.to_path_buf()))
                .route(
                    "/oxen/{namespace}/{repo_name}/verify",
                    web::post().to(controllers::verify::verify),
                ),
        )
        .await;

        let uri = format!("/oxen/{namespace}/{name}/verify");
        let req = actix_web::test::TestRequest::post().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body())
            .await
            .map_err(|e| OxenError::basic_str(format!("could not read body: {e}")))?;
        let body = std::str::from_utf8(&bytes)?;
        Ok(serde_json::from_str(body)?)
    }

    #[actix_web::test]
    async fn test_controllers_verify_reports_a_sound_repository() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;

        let path = repo.path.join("a.txt");
        util::fs::write_to_path(&path, "alpha")?;
        repositories::add(&repo, &path).await?;
        repositories::commit(&repo, "Add a")?;

        let response = call_verify(&sync_dir, namespace, name).await?;

        assert!(response.report.is_healthy());
        assert_eq!(response.report.versions_checked, 1);

        test::cleanup_repo_and_sync_dir(repo, &sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_verify_reports_a_missing_version_file() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;

        let path = repo.path.join("a.txt");
        util::fs::write_to_path(&path, "alpha")?;
        repositories::add(&repo, &path).await?;
        let commit = repositories::commit(&repo, "Add a")?;

        let entries = repositories::entries::list_for_commit(&repo, &commit)?;
        let hash = entries.first().expect("one entry").hash.clone();
        repo.version_store().delete_version(&hash).await?;

        let response = call_verify(&sync_dir, namespace, name).await?;

        assert!(!response.report.is_healthy());
        assert_eq!(response.report.missing_versions.count, 1);
        assert_eq!(response.report.missing_versions.sample, vec![hash]);

        test::cleanup_repo_and_sync_dir(repo, &sync_dir)?;
        Ok(())
    }
}
