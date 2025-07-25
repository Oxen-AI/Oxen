#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use actix_multipart::test::create_form_data_payload_and_headers;
    use actix_web::{web, App};
    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;
    use liboxen::view::CommitResponse;
    use mime;
    use serde_json::Value;
    use actix_web::web::Bytes;

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_put_without_oxen_based_on_header_succeeds() -> Result<(), OxenError> {
        test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file)?;
        let _commit = repositories::commit(&repo, "First commit")?;

        // Create multipart request data
        let (body, headers) = create_form_data_payload_and_headers(
            "file",
            Some("hello.txt".to_owned()),
            Some(mime::TEXT_PLAIN_UTF_8),
            Bytes::from_static(b"Updated Content Without Conflict Check!"),
        );

        let uri = format!("/oxen/{namespace}/{repo_name}/file/main/data/hello.txt");
        let req = actix_web::test::TestRequest::put()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("repo_name", repo_name)
            .param("resource", "data/hello.txt");

        let req = headers
            .into_iter()
            .fold(req, |req, hdr| req.insert_header(hdr))
            .set_payload(body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::put().to(controllers::file::put),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let resp: CommitResponse = serde_json::from_str(body)?;
        assert_eq!(resp.status.status, "success");

        // Verify the file was updated
        let entry = repositories::entries::get_file(&repo, &resp.commit, PathBuf::from("data/hello.txt"))?
            .unwrap();
        let version_path = util::fs::version_path_from_hash(&repo, entry.hash().to_string());
        let updated_content = util::fs::read_from_path(&version_path)?;
        assert_eq!(updated_content, "Updated Content Without Conflict Check!");

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_put_with_matching_oxen_based_on_header_succeeds() -> Result<(), OxenError> {
        test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file)?;
        let commit = repositories::commit(&repo, "First commit")?;

        // Get the current commit hash for the file
        let node = repositories::tree::get_node_by_path(&repo, &commit, &PathBuf::from("data/hello.txt"))?
            .unwrap();
        let current_hash = node.latest_commit_id()?.to_string();

        // Create multipart request data
        let (body, headers) = create_form_data_payload_and_headers(
            "file",
            Some("hello.txt".to_owned()),
            Some(mime::TEXT_PLAIN_UTF_8),
            Bytes::from_static(b"Updated Content With Matching Hash!"),
        );

        let uri = format!("/oxen/{namespace}/{repo_name}/file/main/data/hello.txt");
        let req = actix_web::test::TestRequest::put()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("repo_name", repo_name)
            .param("resource", "data/hello.txt")
            .insert_header(("oxen-based-on", current_hash.as_str()));

        let req = headers
            .into_iter()
            .fold(req, |req, hdr| req.insert_header(hdr))
            .set_payload(body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::put().to(controllers::file::put),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let resp: CommitResponse = serde_json::from_str(body)?;
        assert_eq!(resp.status.status, "success");

        // Verify the file was updated
        let entry = repositories::entries::get_file(&repo, &resp.commit, PathBuf::from("data/hello.txt"))?
            .unwrap();
        let version_path = util::fs::version_path_from_hash(&repo, entry.hash().to_string());
        let updated_content = util::fs::read_from_path(&version_path)?;
        assert_eq!(updated_content, "Updated Content With Matching Hash!");

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_put_with_mismatched_oxen_based_on_header_fails() -> Result<(), OxenError> {
        test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file)?;
        let _commit = repositories::commit(&repo, "First commit")?;

        // Use a fake/mismatched hash
        let fake_hash = "fake_commit_hash_that_doesnt_match_current";

        // Create multipart request data
        let (body, headers) = create_form_data_payload_and_headers(
            "file",
            Some("hello.txt".to_owned()),
            Some(mime::TEXT_PLAIN_UTF_8),
            Bytes::from_static(b"This Update Should Fail!"),
        );

        let uri = format!("/oxen/{namespace}/{repo_name}/file/main/data/hello.txt");
        let req = actix_web::test::TestRequest::put()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("repo_name", repo_name)
            .param("resource", "data/hello.txt")
            .insert_header(("oxen-based-on", fake_hash));

        let req = headers
            .into_iter()
            .fold(req, |req, hdr| req.insert_header(hdr))
            .set_payload(body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::put().to(controllers::file::put),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        
        // Should return an error status
        assert!(!resp.status().is_success());
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_put_with_commit_metadata_headers() -> Result<(), OxenError> {
        test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file)?;
        let commit = repositories::commit(&repo, "First commit")?;

        // Get the current commit hash for the file
        let node = repositories::tree::get_node_by_path(&repo, &commit, &PathBuf::from("data/hello.txt"))?
            .unwrap();
        let current_hash = node.latest_commit_id()?.to_string();

        // Create multipart request data
        let (body, headers) = create_form_data_payload_and_headers(
            "file",
            Some("hello.txt".to_owned()),
            Some(mime::TEXT_PLAIN_UTF_8),
            Bytes::from_static(b"Updated With Custom Metadata!"),
        );

        let uri = format!("/oxen/{namespace}/{repo_name}/file/main/data/hello.txt");
        let req = actix_web::test::TestRequest::put()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("repo_name", repo_name)
            .param("resource", "data/hello.txt")
            .insert_header(("oxen-based-on", current_hash.as_str()))
            .insert_header(("oxen-commit-message", "Custom commit message for conflict test"))
            .insert_header(("oxen-commit-author", "Test Author"))
            .insert_header(("oxen-commit-email", "test@example.com"));

        let req = headers
            .into_iter()
            .fold(req, |req, hdr| req.insert_header(hdr))
            .set_payload(body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::put().to(controllers::file::put),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let resp: CommitResponse = serde_json::from_str(body)?;
        assert_eq!(resp.status.status, "success");

        // Verify the commit has the custom message
        assert_eq!(resp.commit.message, "Custom commit message for conflict test");
        assert_eq!(resp.commit.author, "Test Author");
        assert_eq!(resp.commit.email, "test@example.com");

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_put_on_nonexistent_file_ignores_oxen_based_on_header() -> Result<(), OxenError> {
        test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file)?;
        let _commit = repositories::commit(&repo, "First commit")?;

        // Create multipart request data for a NEW file
        let (body, headers) = create_form_data_payload_and_headers(
            "file",
            Some("new_file.txt".to_owned()),
            Some(mime::TEXT_PLAIN_UTF_8),
            Bytes::from_static(b"Content for new file"),
        );

        let uri = format!("/oxen/{namespace}/{repo_name}/file/main/data/new_file.txt");
        let req = actix_web::test::TestRequest::put()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("repo_name", repo_name)
            .param("resource", "data/new_file.txt")
            .insert_header(("oxen-based-on", "some_random_hash")); // This should be ignored

        let req = headers
            .into_iter()
            .fold(req, |req, hdr| req.insert_header(hdr))
            .set_payload(body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::put().to(controllers::file::put),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let resp: CommitResponse = serde_json::from_str(body)?;
        assert_eq!(resp.status.status, "success");

        // Verify the new file was created
        let entry = repositories::entries::get_file(&repo, &resp.commit, PathBuf::from("data/new_file.txt"))?
            .unwrap();
        let version_path = util::fs::version_path_from_hash(&repo, entry.hash().to_string());
        let content = util::fs::read_from_path(&version_path)?;
        assert_eq!(content, "Content for new file");

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }
}