use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use actix_web::{HttpRequest, HttpResponse, Result};

use liboxen::view::{ParseResourceResponse, StatusMessage};

use log;

pub async fn get(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let resource = parse_resource(&req, &repository)?;
    let response = ParseResourceResponse {
        status: StatusMessage::resource_found(),
        resource,
    };

    log::debug!("Response: {:?}", response);
    Ok(HttpResponse::Ok().json(response))
}

#[cfg(test)]
mod tests {

    use actix_web::http::{self};

    use actix_web::body::to_bytes;

    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;

    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_get() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Repo";
        let resource_str = "main/to/resource";

        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let path = liboxen::test::add_txt_file_to_dir(&repo.path, resource_str)?;
        repositories::add(&repo, path)?;
        repositories::commit(&repo, "first commit")?;

        let uri = format!(
            "/oxen/{namespace}/{repo_name}/branches/resolve_resource_attributes/{resource_str}"
        );

        let req = test::repo_request_with_param(
            &sync_dir,
            queue,
            &uri,
            namespace,
            repo_name,
            "resource",
            resource_str,
        );

        let resp = controllers::revisions::get(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);

        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let parse_resp: liboxen::view::ParseResourceResponse =
            serde_json::from_str(text).map_err(OxenError::from)?;

        assert_eq!(parse_resp.resource.branch.unwrap().name, "main");
        // fix windows tests
        let path = parse_resp.resource.path.to_string_lossy();
        let path = path.replace('\\', "/");
        assert_eq!(path, "to/resource");

        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
