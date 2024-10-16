use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param, PageNumVersionQuery};

use liboxen::core::versions::MinOxenVersion;
use liboxen::opts::PaginateOpts;
use liboxen::view::PaginatedDirEntriesResponse;
use liboxen::{constants, repositories};

use actix_web::{web, HttpRequest, HttpResponse};

pub async fn get(
    req: HttpRequest,
    query: web::Query<PageNumVersionQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    let page: usize = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size: usize = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    let api_version = MinOxenVersion::or_latest(query.api_version.clone())?;

    log::debug!(
        "{} resource {namespace}/{repo_name}/{resource}",
        liboxen::current_function!()
    );

    let paginated_entries = repositories::entries::list_directory_w_version(
        &repo,
        &resource.path,
        resource.version.to_str().unwrap_or_default(),
        &PaginateOpts {
            page_num: page,
            page_size,
        },
        api_version,
    )?;

    let view = PaginatedDirEntriesResponse::ok_from(paginated_entries);
    Ok(HttpResponse::Ok().json(view))
}

#[cfg(test)]
mod tests {
    use actix_web::{web, App};
    use std::path::Path;

    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;
    use liboxen::view::PaginatedDirEntries;

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_controllers_dir_list_directory() -> Result<(), OxenError> {
        test::init_test_env();

        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();
        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;

        // write files to dir
        liboxen::test::populate_dir_with_training_data(&repo.path)?;

        // add the full dir
        let train_dir = repo.path.join(Path::new("train"));
        let num_entries = util::fs::rcount_files_in_dir(&train_dir);
        repositories::add(&repo, &train_dir)?;

        // commit the changes
        let commit = repositories::commit(&repo, "adding training dir")?;

        // Use the api list the files from the commit
        let uri = format!("/oxen/{}/{}/dir/{}/train/", namespace, name, commit.id);
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone(), queue))
                .route(
                    "/oxen/{namespace}/{repo_name}/dir/{resource:.*}",
                    web::get().to(controllers::dir::get),
                ),
        )
        .await;

        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        println!("GOT RESP STATUS: {}", resp.response().status());
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        println!("GOT BODY: {body}");
        let entries_resp: PaginatedDirEntries = serde_json::from_str(body)?;

        // Make sure we can fetch all the entries
        assert_eq!(entries_resp.total_entries, num_entries);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
