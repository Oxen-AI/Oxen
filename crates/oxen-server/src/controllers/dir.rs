use crate::errors::OxenHttpError;
use crate::helpers::get_repo_async;
use crate::params::{PageNumVersionQuery, app_data, parse_resource_async, path_param};

use liboxen::opts::{PaginateOpts, SortOpts};
use liboxen::perf_guard;
use liboxen::view::PaginatedDirEntriesResponse;
use liboxen::{constants, repositories};

use actix_web::{HttpRequest, HttpResponse, web};
use utoipa;

/// List directory contents
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/dir/{resource}",
    tag = "Directories",
    description = "List paginated contents of a directory at a specific revision, with optional workspace support.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("resource" = String, Path, description = "Path to the directory (including branch/commit ID)", example = "main/data/train"),
        PageNumVersionQuery
    ),
    responses(
        (status = 200, description = "Paginated list of directory entries", body = PaginatedDirEntriesResponse),
        (status = 404, description = "Directory or repository not found")
    )
)]
pub async fn get(
    req: HttpRequest,
    query: web::Query<PageNumVersionQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let _perf = perf_guard!("dir::get_endpoint");

    let _perf_parse = perf_guard!("dir::get_parse_params");
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;
    let resource = parse_resource_async(&req, &repo).await?;

    let page: usize = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size: usize = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
    // depth: 0 = current only, positive = that many levels, negative = unlimited
    let depth: usize = match query.depth.unwrap_or(0) {
        d if d < 0 => usize::MAX,
        d => d as usize,
    };
    let sort_opts = SortOpts::from_query(query.sort_by.as_deref(), query.reverse.unwrap_or(false))
        .map_err(|_| {
            OxenHttpError::BadRequest(
                "Invalid value for sort_by, valid options include: `name`, `date`.".into(),
            )
        })?
        .unwrap_or_default();
    drop(_perf_parse);

    log::debug!(
        "{} resource {namespace}/{repo_name}/{resource} depth {depth}",
        liboxen::current_function!()
    );

    let revision = if let Some(workspace) = resource.workspace.clone() {
        workspace.commit.id
    } else {
        resource.version.to_str().unwrap_or_default().to_string()
    };

    let _perf_list = perf_guard!("dir::get_list_directory");
    let paginated_entries = repositories::entries::list_directory_w_workspace_depth_async(
        &repo,
        &resource.path,
        revision,
        resource.workspace.clone(),
        &PaginateOpts {
            page_num: page,
            page_size,
        },
        &sort_opts,
        depth,
    )
    .await?;
    drop(_perf_list);

    let _perf_serialize = perf_guard!("dir::get_serialize_response");
    let view = PaginatedDirEntriesResponse::ok_from(paginated_entries);
    Ok(HttpResponse::Ok().json(view))
}

#[cfg(test)]
mod tests {
    use crate::test;
    use actix_web::{App, web};
    use std::path::{Path, PathBuf};
    use uuid::Uuid;

    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;
    use liboxen::view::PaginatedDirEntries;

    use crate::app_data::OxenAppData;
    use crate::controllers;

    #[actix_web::test]
    async fn test_controllers_dir_list_directory() -> Result<(), OxenError> {
        liboxen::test::init_test_env();

        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;

        // write files to dir
        liboxen::test::populate_dir_with_training_data(&repo.path)?;

        // add the full dir
        let train_dir = repo.path.join(Path::new("train"));
        let num_entries = util::fs::rcount_files_in_dir(&train_dir);
        repositories::add(&repo, &train_dir).await?;

        // commit the changes
        let commit = repositories::commit(&repo, "adding training dir")?;

        // Use the api list the files from the commit
        let uri = format!("/oxen/{}/{}/dir/{}/train/", namespace, name, commit.id);
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
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
        test::cleanup_repo_and_sync_dir(repo, &sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_dir_sort_by_date() -> Result<(), OxenError> {
        liboxen::test::init_test_env();

        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;

        // Create files in separate commits so they have different timestamps
        let file_a = repo.path.join("a_file.txt");
        util::fs::write_to_path(&file_a, "content a")?;
        repositories::add(&repo, &file_a).await?;
        repositories::commit(&repo, "adding a_file")?;

        let file_b = repo.path.join("b_file.txt");
        util::fs::write_to_path(&file_b, "content b")?;
        repositories::add(&repo, &file_b).await?;
        let commit = repositories::commit(&repo, "adding b_file")?;

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/dir/{resource:.*}",
                    web::get().to(controllers::dir::get),
                ),
        )
        .await;

        // Default sort (name ascending)
        let uri = format!("/oxen/{}/{}/dir/{}/", namespace, name, commit.id);
        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let entries_resp: PaginatedDirEntries = serde_json::from_str(body)?;
        let filenames: Vec<&str> = entries_resp.entries.iter().map(|e| e.filename()).collect();
        assert_eq!(filenames, vec!["a_file.txt", "b_file.txt"]);

        // Sort by date ascending (a_file committed first, b_file second)
        let uri = format!(
            "/oxen/{}/{}/dir/{}/?sort_by=date",
            namespace, name, commit.id
        );
        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let entries_resp: PaginatedDirEntries = serde_json::from_str(body)?;
        let filenames: Vec<&str> = entries_resp.entries.iter().map(|e| e.filename()).collect();
        assert_eq!(filenames, vec!["a_file.txt", "b_file.txt"]);

        // Sort by date descending (reverse)
        let uri = format!(
            "/oxen/{}/{}/dir/{}/?sort_by=date&reverse=true",
            namespace, name, commit.id
        );
        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let entries_resp: PaginatedDirEntries = serde_json::from_str(body)?;
        let filenames: Vec<&str> = entries_resp.entries.iter().map(|e| e.filename()).collect();
        assert_eq!(filenames, vec!["b_file.txt", "a_file.txt"]);

        // Sort by name reversed
        let uri = format!(
            "/oxen/{}/{}/dir/{}/?sort_by=name&reverse=true",
            namespace, name, commit.id
        );
        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let entries_resp: PaginatedDirEntries = serde_json::from_str(body)?;
        let filenames: Vec<&str> = entries_resp.entries.iter().map(|e| e.filename()).collect();
        assert_eq!(filenames, vec!["b_file.txt", "a_file.txt"]);

        // cleanup
        test::cleanup_repo_and_sync_dir(repo, &sync_dir)?;

        Ok(())
    }

    /// The response a browser gets for `GET /dir/{workspace_id}/{path}`: the commit tree's entries
    /// annotated with what the workspace has staged, the workspace's own additions merged in, and
    /// nothing from another directory leaking in.
    #[actix_web::test]
    async fn test_controllers_dir_list_workspace_directory() -> Result<(), OxenError> {
        liboxen::test::init_test_env();

        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;

        let data_dir = repo.path.join("data");
        util::fs::create_dir_all(&data_dir)?;
        util::fs::write_to_path(repo.path.join("README.md"), "readme")?;
        util::fs::write_to_path(data_dir.join("keep.txt"), "keep")?;
        util::fs::write_to_path(data_dir.join("train.csv"), "a,b\n1,2\n")?;
        repositories::add(&repo, &repo.path).await?;
        let commit = repositories::commit(&repo, "adding the committed tree")?;

        let workspace_id = Uuid::new_v4().to_string();
        let workspace = repositories::workspaces::create(&repo, &commit, &workspace_id, true)?;

        // Three additions, one of them under a directory only the workspace knows about.
        for path in [
            PathBuf::from("new_root.txt"),
            Path::new("data").join("new_data.txt"),
            Path::new("fresh").join("inside.txt"),
        ] {
            let full_path = workspace.workspace_repo.path.join(&path);
            if let Some(parent) = full_path.parent() {
                util::fs::create_dir_all(parent)?;
            }
            util::fs::write_to_path(&full_path, "staged")?;
            repositories::workspaces::files::add(&workspace, &full_path).await?;
        }

        // One modification and one removal, both of committed files.
        let modified = workspace.workspace_repo.path.join("data").join("train.csv");
        util::fs::write_to_path(&modified, "a,b\n1,2\n3,4\n")?;
        repositories::workspaces::files::add(&workspace, &modified).await?;
        repositories::workspaces::files::rm(&workspace, Path::new("README.md")).await?;

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/dir/{resource:.*}",
                    web::get().to(controllers::dir::get),
                ),
        )
        .await;

        let uri = format!("/oxen/{namespace}/{name}/dir/{workspace_id}/");
        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        assert!(resp.status().is_success(), "GET {uri} -> {}", resp.status());
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap().to_string();
        let root: serde_json::Value = serde_json::from_str(&body)?;
        let entries = root["entries"].as_array().expect("entries is an array");

        // Directories first, then names ascending. `fresh` exists only because of a staged file.
        let filenames: Vec<&str> = entries
            .iter()
            .map(|e| e["filename"].as_str().unwrap())
            .collect();
        assert_eq!(
            filenames,
            vec!["data", "fresh", "README.md", "new_root.txt"]
        );
        assert_eq!(root["total_entries"], 4);

        // The `changes` field is present only where the workspace has staged something. Its
        // absence, not a null, is what makes an entry read as a plain `MetadataEntry` on the
        // client, so check the key itself rather than indexing it.
        assert!(entries[0].get("changes").is_none());
        assert_eq!(entries[1]["changes"]["status"], "Added");
        assert_eq!(entries[2]["changes"]["status"], "Removed");
        assert_eq!(entries[3]["changes"]["status"], "Added");

        // A staged addition is addressed through the workspace, so its file URL reaches the
        // staged bytes rather than a commit that never held them.
        assert_eq!(entries[3]["resource"]["version"], workspace_id);
        assert_eq!(entries[3]["resource"]["path"], "new_root.txt");
        assert_eq!(
            entries[3]["resource"]["resource"],
            format!("{workspace_id}/new_root.txt")
        );

        let uri = format!("/oxen/{namespace}/{name}/dir/{workspace_id}/data");
        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        assert!(resp.status().is_success(), "GET {uri} -> {}", resp.status());
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let data: PaginatedDirEntries = serde_json::from_str(body)?;
        let filenames: Vec<&str> = data.entries.iter().map(|e| e.filename()).collect();
        assert_eq!(filenames, vec!["keep.txt", "new_data.txt", "train.csv"]);
        assert_eq!(data.total_entries, 3);

        let data: serde_json::Value = serde_json::from_str(body)?;
        let entries = data["entries"].as_array().expect("entries is an array");
        assert!(entries[0].get("changes").is_none());
        assert_eq!(entries[1]["changes"]["status"], "Added");
        assert_eq!(entries[2]["changes"]["status"], "Modified");

        // cleanup
        drop(workspace);
        test::cleanup_repo_and_sync_dir(repo, &sync_dir)?;

        Ok(())
    }
}
