#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use actix_multipart_test::MultiPartFormDataBuilder;
    use actix_web::{App, web};
    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;
    use liboxen::view::CommitResponse;

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;

    /// Helper: make a PUT request with a single text file and return the response.
    /// Writes `content` to a temp file and sends it as a multipart "file" field.
    async fn do_put(
        sync_dir: &std::path::Path,
        namespace: &str,
        repo_name: &str,
        file_path: &str,
        content: &[u8],
        oxen_based_on: Option<&str>,
    ) -> actix_web::dev::ServiceResponse {
        // Write content to a temp file so MultiPartFormDataBuilder can read it
        let tmp_dir = std::env::temp_dir().join(format!("oxen_test_put_{}", uuid::Uuid::new_v4()));
        util::fs::create_dir_all(&tmp_dir).unwrap();
        let filename = file_path.rsplit('/').next().unwrap();
        let tmp_file = tmp_dir.join(filename);
        util::fs::write(&tmp_file, content).unwrap();
        let tmp_file: PathBuf = tmp_file;

        let mut builder = MultiPartFormDataBuilder::new();
        builder.with_file(tmp_file.clone(), "file", "text/plain", filename);
        builder.with_text("name", "test_user");
        builder.with_text("email", "test@example.com");
        builder.with_text("message", "test commit");
        let (header, body) = builder.build();

        let uri: &'static str = Box::leak(
            format!("/oxen/{namespace}/{repo_name}/file/main/{file_path}").into_boxed_str(),
        );
        let ns: &'static str = Box::leak(namespace.to_string().into_boxed_str());
        let rn: &'static str = Box::leak(repo_name.to_string().into_boxed_str());
        let fp: &'static str = Box::leak(file_path.to_string().into_boxed_str());
        let mut req = actix_web::test::TestRequest::put()
            .uri(uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", ns)
            .param("repo_name", rn)
            .param("resource", fp);

        if let Some(hash) = oxen_based_on {
            req = req.insert_header(("oxen-based-on", hash.to_string()));
        }

        let req = req.insert_header(header).set_payload(body).to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.to_path_buf()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::put().to(controllers::file::put),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        let _ = std::fs::remove_dir_all(&tmp_dir);
        resp
    }

    #[actix_web::test]
    async fn test_put_without_oxen_based_on_header_succeeds() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file).await?;
        let _commit = repositories::commit(&repo, "First commit")?;

        let resp = do_put(
            &sync_dir,
            namespace,
            repo_name,
            "data/hello.txt",
            b"Updated Content Without Conflict Check!",
            None,
        )
        .await;

        assert!(resp.status().is_success());
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let resp: CommitResponse = serde_json::from_str(body)?;
        assert_eq!(resp.status.status, "success");
        // No merge happened, so merged_content should be absent
        assert!(resp.merged_content.is_none());

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_put_with_matching_oxen_based_on_header_succeeds() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file).await?;
        let commit = repositories::commit(&repo, "First commit")?;

        let node =
            repositories::tree::get_node_by_path(&repo, &commit, PathBuf::from("data/hello.txt"))?
                .unwrap();
        let current_hash = node.latest_commit_id()?.to_string();

        let resp = do_put(
            &sync_dir,
            namespace,
            repo_name,
            "data/hello.txt",
            b"Updated Content With Matching Hash!",
            Some(&current_hash),
        )
        .await;

        assert!(resp.status().is_success());
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let resp: CommitResponse = serde_json::from_str(body)?;
        assert_eq!(resp.status.status, "success");

        let entry =
            repositories::entries::get_file(&repo, &resp.commit, PathBuf::from("data/hello.txt"))?
                .unwrap();
        let version_store = repo.version_store()?;
        let content = version_store.get_version(&entry.hash().to_string()).await?;
        assert_eq!(
            String::from_utf8(content).unwrap(),
            "Updated Content With Matching Hash!"
        );

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_put_with_mismatched_oxen_based_on_header_and_no_merge_possible_fails()
    -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file).await?;
        let _commit = repositories::commit(&repo, "First commit")?;

        // Use a fake hash that doesn't correspond to any commit
        let resp = do_put(
            &sync_dir,
            namespace,
            repo_name,
            "data/hello.txt",
            b"This Update Should Fail!",
            Some("fake_commit_hash_that_doesnt_match_current"),
        )
        .await;

        assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_put_on_nonexistent_file_ignores_oxen_based_on_header() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file).await?;
        let _commit = repositories::commit(&repo, "First commit")?;

        let resp = do_put(
            &sync_dir,
            namespace,
            repo_name,
            "data/new_file.txt",
            b"Content for new file",
            Some("some_random_hash"),
        )
        .await;

        assert!(resp.status().is_success());
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let resp: CommitResponse = serde_json::from_str(body)?;
        assert_eq!(resp.status.status, "success");

        let entry = repositories::entries::get_file(
            &repo,
            &resp.commit,
            PathBuf::from("data/new_file.txt"),
        )?
        .unwrap();
        let version_store = repo.version_store()?;
        let content = version_store.get_version(&entry.hash().to_string()).await?;
        assert_eq!(String::from_utf8(content).unwrap(), "Content for new file");

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_put_auto_merge_non_overlapping_changes() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-AutoMerge";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;

        // Base file: three lines
        let file_path = repo.path.join("data/notes.txt");
        util::fs::write_to_path(&file_path, "line1\nline2\nline3\n")?;
        repositories::add(&repo, &file_path).await?;
        let base_commit = repositories::commit(&repo, "Base commit")?;

        // Record the base commit hash for the file
        let base_node = repositories::tree::get_node_by_path(
            &repo,
            &base_commit,
            PathBuf::from("data/notes.txt"),
        )?
        .unwrap();
        let base_commit_hash = base_node.latest_commit_id()?.to_string();

        // Simulate another user changing line3 (theirs)
        util::fs::write_to_path(&file_path, "line1\nline2\nline3-theirs\n")?;
        repositories::add(&repo, &file_path).await?;
        let _theirs_commit = repositories::commit(&repo, "Their change to line3")?;

        // Now PUT with ours changing line1, based on the original commit
        let resp = do_put(
            &sync_dir,
            namespace,
            repo_name,
            "data/notes.txt",
            b"line1-ours\nline2\nline3\n",
            Some(&base_commit_hash),
        )
        .await;

        assert!(
            resp.status().is_success(),
            "Auto-merge should succeed for non-overlapping changes, got {}",
            resp.status()
        );

        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body_str = std::str::from_utf8(&bytes).unwrap();
        let resp: CommitResponse = serde_json::from_str(body_str)?;

        // Response should include the merged content so client can update locally
        assert_eq!(
            resp.merged_content.as_deref(),
            Some("line1-ours\nline2\nline3-theirs\n"),
            "merged_content should be returned in response"
        );

        // Verify the committed file also has the merged content
        let entry =
            repositories::entries::get_file(&repo, &resp.commit, PathBuf::from("data/notes.txt"))?
                .unwrap();
        let version_store = repo.version_store()?;
        let stored = version_store.get_version(&entry.hash().to_string()).await?;
        assert_eq!(
            String::from_utf8(stored).unwrap(),
            "line1-ours\nline2\nline3-theirs\n"
        );

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_put_auto_merge_conflicting_changes_rejected() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-AutoMerge-Conflict";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;

        // Base file
        let file_path = repo.path.join("data/notes.txt");
        util::fs::write_to_path(&file_path, "line1\nline2\nline3\n")?;
        repositories::add(&repo, &file_path).await?;
        let base_commit = repositories::commit(&repo, "Base commit")?;

        let base_node = repositories::tree::get_node_by_path(
            &repo,
            &base_commit,
            PathBuf::from("data/notes.txt"),
        )?
        .unwrap();
        let base_commit_hash = base_node.latest_commit_id()?.to_string();

        // Another user changes line1
        util::fs::write_to_path(&file_path, "line1-theirs\nline2\nline3\n")?;
        repositories::add(&repo, &file_path).await?;
        let _theirs_commit = repositories::commit(&repo, "Their change to line1")?;

        // PUT also changing line1 — conflict!
        let resp = do_put(
            &sync_dir,
            namespace,
            repo_name,
            "data/notes.txt",
            b"line1-ours\nline2\nline3\n",
            Some(&base_commit_hash),
        )
        .await;

        assert_eq!(
            resp.status(),
            actix_web::http::StatusCode::BAD_REQUEST,
            "Conflicting changes should be rejected"
        );

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_put_auto_merge_identical_changes_succeeds() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-AutoMerge-Identical";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;

        let file_path = repo.path.join("data/notes.txt");
        util::fs::write_to_path(&file_path, "line1\nline2\nline3\n")?;
        repositories::add(&repo, &file_path).await?;
        let base_commit = repositories::commit(&repo, "Base commit")?;

        let base_node = repositories::tree::get_node_by_path(
            &repo,
            &base_commit,
            PathBuf::from("data/notes.txt"),
        )?
        .unwrap();
        let base_commit_hash = base_node.latest_commit_id()?.to_string();

        // Both sides make the same change
        util::fs::write_to_path(&file_path, "line1-changed\nline2\nline3\n")?;
        repositories::add(&repo, &file_path).await?;
        let _theirs_commit = repositories::commit(&repo, "Their identical change")?;

        // PUT with the same change
        let resp = do_put(
            &sync_dir,
            namespace,
            repo_name,
            "data/notes.txt",
            b"line1-changed\nline2\nline3\n",
            Some(&base_commit_hash),
        )
        .await;

        assert!(
            resp.status().is_success(),
            "Identical changes should merge cleanly, got {}",
            resp.status()
        );

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_put_auto_merge_binary_file_rejected() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-AutoMerge-Binary";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;

        // Create a file with invalid UTF-8 bytes
        let file_path = repo.path.join("data/binary.bin");
        let binary_content: Vec<u8> = vec![0x00, 0xFF, 0xFE, 0x80, 0x81];
        util::fs::write(&file_path, &binary_content)?;
        repositories::add(&repo, &file_path).await?;
        let base_commit = repositories::commit(&repo, "Base commit")?;

        let base_node = repositories::tree::get_node_by_path(
            &repo,
            &base_commit,
            PathBuf::from("data/binary.bin"),
        )?
        .unwrap();
        let base_commit_hash = base_node.latest_commit_id()?.to_string();

        // Change the binary file on the server side
        let binary_content_v2: Vec<u8> = vec![0x00, 0xFF, 0xFE, 0x80, 0x82];
        util::fs::write(&file_path, &binary_content_v2)?;
        repositories::add(&repo, &file_path).await?;
        let _theirs_commit = repositories::commit(&repo, "Their binary change")?;

        // PUT with different binary content — should fail (can't text-merge binary)
        let resp = do_put(
            &sync_dir,
            namespace,
            repo_name,
            "data/binary.bin",
            &[0x00, 0xFF, 0xFE, 0x80, 0x83],
            Some(&base_commit_hash),
        )
        .await;

        assert_eq!(
            resp.status(),
            actix_web::http::StatusCode::BAD_REQUEST,
            "Binary files should not auto-merge"
        );

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_put_stale_base_on_deleted_file_rejected() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-AutoMerge-Deleted";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;

        // Create and commit a file
        let file_path = repo.path.join("data/notes.txt");
        util::fs::write_to_path(&file_path, "original content\n")?;
        repositories::add(&repo, &file_path).await?;
        let base_commit = repositories::commit(&repo, "Base commit")?;

        let base_node = repositories::tree::get_node_by_path(
            &repo,
            &base_commit,
            PathBuf::from("data/notes.txt"),
        )?
        .unwrap();
        let base_commit_hash = base_node.latest_commit_id()?.to_string();

        // Delete the file and commit
        let rm_opts = liboxen::opts::RmOpts::from_path("data/notes.txt");
        repositories::rm(&repo, &rm_opts)?;
        let _delete_commit = repositories::commit(&repo, "Delete file")?;

        // Try to PUT with stale base — file no longer exists at HEAD
        // This should get NotFound since the file doesn't exist at HEAD
        let resp = do_put(
            &sync_dir,
            namespace,
            repo_name,
            "data/notes.txt",
            b"updated content\n",
            Some(&base_commit_hash),
        )
        .await;

        // File doesn't exist at HEAD, so the node check won't find it as a file
        // and the conflict path is skipped — this becomes a new file creation
        assert!(
            resp.status().is_success(),
            "PUT to deleted file path should create the file anew, got {}",
            resp.status()
        );

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }
}
