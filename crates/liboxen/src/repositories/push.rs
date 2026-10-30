//! # oxen push
//!
//! Push data from your local machine to a remote.
//!

/// # Push committed data to a remote
///
/// Requires a reachable remote, so this example is compiled but not run.
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), liboxen::error::OxenError> {
/// use liboxen::{command, repositories, util};
/// use std::path::Path;
///
/// // Initialize the repository
/// let base_dir = Path::new("repo_dir_push");
/// let mut repo = repositories::init(base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World")?;
///
/// // Stage the file
/// repositories::add(&repo, &hello_file).await?;
///
/// // Commit staged
/// repositories::commit(&repo, "My commit message")?;
///
/// // Set the remote server
/// let remote_url = "http://localhost:3000/repositories/hello";
/// command::config::set_remote_by_url(&mut repo, "origin", remote_url).await?;
///
/// // Push the file
/// repositories::push(&repo).await?;
/// # Ok(())
/// # }
/// ```
pub use crate::core::v_latest::push::push;

/// Push to a specific remote branch on the default remote repository
pub use crate::core::v_latest::push::push_remote_branch;

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;

    use crate::constants;
    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME, stream_segment_size};
    use crate::core::progress::push_progress::PushProgress;
    use crate::error::OxenError;
    use crate::model::merkle_tree::node::MerkleTreeNode;
    use crate::opts::RmOpts;
    use crate::opts::{CloneOpts, PushOpts};
    use crate::repositories;
    use crate::test;
    use crate::util;
    use crate::util::fs::AtomicFile;
    use crate::view::entries::EMetadataEntry;
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::Arc;

    // One repository pushes a history of several commits to a new remote, then a commit on top of
    // it, then large files, so each push starts from the remote the push before it left. The
    // repairs that delete blobs come last.
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_push_histories_to_a_new_remote() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            test::run_empty_dir_test_async(|scratch_dir| async move {
                /*
                README.md
                data/
                  0/file.txt ... 4/file.txt, one commit each
                train/
                  train_0.txt, train_1.txt
                LICENSE.md
                */
                let readme_path =
                    test::write_txt_file_to_path(repo.path.join("README.md"), "README")?;
                repositories::add(&repo, &readme_path).await?;
                let readme_commit = repositories::commit(&repo, "Adding README")?;

                let data_dir = repo.path.join("data");
                for i in 0..5 {
                    let dir_path = data_dir.join(format!("{i}"));
                    util::fs::create_dir_all(&dir_path)?;
                    let file_path = test::write_txt_file_to_path(
                        dir_path.join("file.txt"),
                        format!("file -> {i}"),
                    )?;
                    repositories::add(&repo, &file_path).await?;
                    repositories::commit(&repo, &format!("Adding file -> data/{i}/file.txt"))?;
                }
                let file_path =
                    test::write_txt_file_to_path(data_dir.join("2").join("file.txt"), "modified")?;
                repositories::add(&repo, &file_path).await?;
                let data_commit = repositories::commit(&repo, "Modifying file again")?;

                let train_dir = repo.path.join("train");
                util::fs::create_dir_all(&train_dir)?;
                for i in 0..2 {
                    test::write_txt_file_to_path(
                        train_dir.join(format!("train_{i}.txt")),
                        format!("train {i}"),
                    )?;
                }
                let num_train_files = util::fs::rcount_files_in_dir(&train_dir);
                repositories::add(&repo, &train_dir).await?;
                repositories::commit(&repo, "Adding train data")?;

                // Adding "." after committing a directory picks up the loose files at the root
                test::write_txt_file_to_path(repo.path.join("LICENSE.md"), "license")?;
                let num_root_entries = util::fs::count_items_in_dir(&repo.path);
                repositories::add(&repo, &repo.path).await?;
                let commit = repositories::commit(&repo, "Adding rest of data")?;

                let remote_repo = test::connect_remote_repo(&mut repo).await?;
                repositories::push(&repo).await?;

                let entries =
                    api::client::dir::list(&remote_repo, &commit.id, ".", 1, num_root_entries + 10)
                        .await?;
                assert_eq!(entries.total_entries, num_root_entries);
                assert_eq!(entries.entries.len(), num_root_entries);
                let data_entry = entries
                    .entries
                    .iter()
                    .find(|e| e.filename() == "data")
                    .expect("data is at the root");
                assert_eq!(
                    data_entry
                        .latest_commit()
                        .as_ref()
                        .expect("data has a commit")
                        .id,
                    data_commit.id,
                    "a directory's latest commit is the last one that changed a file under it"
                );
                let readme_entry = entries
                    .entries
                    .iter()
                    .find(|e| e.filename() == "README.md")
                    .expect("README.md is at the root");
                assert_eq!(
                    readme_entry
                        .latest_commit()
                        .as_ref()
                        .expect("README has a commit")
                        .id,
                    readme_commit.id
                );

                let entries =
                    api::client::dir::list(&remote_repo, &commit.id, "data/3", 1, 10).await?;
                assert_eq!(entries.total_entries, 1);
                let entry = entries.entries.first().expect("data/3 has a file");
                assert_eq!(entry.filename(), "file.txt");
                assert_eq!(
                    entry
                        .latest_commit()
                        .as_ref()
                        .expect("file has a commit")
                        .message,
                    "Adding file -> data/3/file.txt"
                );

                let download_path = scratch_dir.join("README_2.md");
                api::client::entries::download_entry(
                    &remote_repo,
                    "README.md",
                    &download_path,
                    DEFAULT_BRANCH_NAME,
                )
                .await?;
                assert_eq!(
                    util::fs::read_from_path(&download_path)?,
                    util::fs::read_from_path(&readme_path)?
                );

                // Push a commit to a remote that already has every commit before it
                let test_dir = repo.path.join("test");
                util::fs::create_dir_all(&test_dir)?;
                for i in 0..2 {
                    test::write_txt_file_to_path(
                        test_dir.join(format!("test_{i}.txt")),
                        format!("test {i}"),
                    )?;
                }
                let num_test_files = util::fs::count_files_in_dir(&test_dir);
                repositories::add(&repo, &test_dir).await?;
                let commit = repositories::commit(&repo, "Adding test data")?;
                repositories::push(&repo).await?;

                let page_size = num_train_files + num_test_files + 5;
                let train_entries =
                    api::client::dir::list(&remote_repo, &commit.id, "/train", 1, page_size)
                        .await?;
                let test_entries =
                    api::client::dir::list(&remote_repo, &commit.id, "/test", 1, page_size).await?;
                assert_eq!(train_entries.total_entries, num_train_files);
                assert_eq!(test_entries.total_entries, num_test_files);
                assert_eq!(test_entries.entries.len(), num_test_files);

                // Files just over the streamed-transfer threshold take the chunked path both ways,
                // one at the root and one in a subdirectory, since the chunk download path has to
                // carry the directory prefix for the server to find the file. A clone made before
                // they were pushed pulls them, and nothing else.
                let clone_repo =
                    repositories::clone_url(&remote_repo.remote.url, &scratch_dir.join("clone"))
                        .await?;
                let file_size = (stream_segment_size() + 1024 * 1024) as usize;
                let root_data: Vec<u8> = vec![42; file_size];
                AtomicFile::new(repo.path.join("large_file.bin")).write(&root_data)?;
                let models_dir = repo.path.join("data").join("models");
                util::fs::create_dir_all(&models_dir)?;
                let sub_data: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
                AtomicFile::new(models_dir.join("weights.bin")).write(&sub_data)?;
                repositories::add(&repo, &repo.path).await?;
                let commit = repositories::commit(&repo, "Add large files")?;
                repositories::push(&repo).await?;
                assert!(
                    api::client::commits::get_by_id(&remote_repo, &commit.id)
                        .await?
                        .is_some(),
                    "Remote commit should exist"
                );
                repositories::pull(&clone_repo).await?;
                for (relative_path, expected) in [
                    (PathBuf::from("large_file.bin"), &root_data),
                    (
                        PathBuf::from("data").join("models").join("weights.bin"),
                        &sub_data,
                    ),
                ] {
                    let pulled_file_path = clone_repo.path.join(&relative_path);
                    assert_eq!(
                        util::fs::metadata(&pulled_file_path)?.len(),
                        file_size as u64,
                        "Pulled file size should match original for {relative_path:?}"
                    );
                    assert_eq!(
                        &util::fs::read_bytes_from_path(&pulled_file_path)?,
                        expected,
                        "Pulled file contents should match the original data for {relative_path:?}"
                    );
                }

                // A repository with a history of its own cannot push without a remote, to a
                // remote with no repository behind it, nor to a remote holding another history
                let mut other_repo = repositories::init(scratch_dir.join("other_repo"))?;
                other_repo.version_store().init().await?;
                for i in 0..2 {
                    let path = test::write_txt_file_to_path(
                        other_repo.path.join(format!("other_{i}.txt")),
                        format!("other {i}"),
                    )?;
                    repositories::add(&other_repo, &path).await?;
                    repositories::commit(&other_repo, &format!("Adding other file {i}"))?;
                }
                assert!(
                    repositories::push(&other_repo).await.is_err(),
                    "a push needs a remote"
                );
                let missing_url = format!("{}_missing", remote_repo.remote.url);
                command::config::set_remote(&mut other_repo, DEFAULT_REMOTE_NAME, &missing_url)?;
                assert!(
                    repositories::push(&other_repo).await.is_err(),
                    "a push needs a repository at the remote's URL"
                );
                test::attach_remote_repo(&mut other_repo, &remote_repo)?;
                assert!(
                    repositories::push(&other_repo).await.is_err(),
                    "a history unrelated to the remote's cannot be pushed to it"
                );

                let sync_dir =
                    PathBuf::from(std::env::var("SYNC_DIR").expect("SYNC_DIR set by bin/test"));
                let server = repositories::get_by_namespace_and_name(
                    &sync_dir,
                    constants::DEFAULT_NAMESPACE,
                    repo.dirname(),
                    None,
                )?
                .expect("server repo exists");
                let file_hash = |path: &str| -> Result<String, OxenError> {
                    Ok(repositories::tree::get_node_by_path(&repo, &commit, path)?
                        .expect("the file is in the commit")
                        .file()?
                        .hash()
                        .to_string())
                };

                // `oxen push --revalidate` must re-push a blob the remote is simply MISSING, not
                // only one that is corrupt-but-present. `versions::clean` reports zero corrupted
                // for an absent blob.
                let readme_hash = file_hash("README.md")?;
                server.version_store().delete_version(&readme_hash).await?;
                assert!(
                    !server.version_store().version_exists(&readme_hash).await?,
                    "precondition: the blob should be gone from the server"
                );
                let opts = PushOpts {
                    remote: DEFAULT_REMOTE_NAME.to_string(),
                    branch: DEFAULT_BRANCH_NAME.to_string(),
                    revalidate: true,
                    ..Default::default()
                };
                repositories::push::push_remote_branch(&repo, &opts).await?;
                assert!(
                    server.version_store().version_exists(&readme_hash).await?,
                    "revalidate must re-push the missing blob"
                );

                // `--missing-files` from a clone missing some of the files the remote needs must
                // fail fast with CannotPushShallowClone and upload nothing, not partially upload
                // and then error mid-stream. The server is missing both blobs, and this clone can
                // supply LICENSE.md but not test/test_0.txt.
                let license_hash = file_hash("LICENSE.md")?;
                let test_file_hash = file_hash("test/test_0.txt")?;
                server.version_store().delete_version(&license_hash).await?;
                server
                    .version_store()
                    .delete_version(&test_file_hash)
                    .await?;
                repo.version_store().delete_version(&test_file_hash).await?;
                let opts = PushOpts {
                    remote: DEFAULT_REMOTE_NAME.to_string(),
                    branch: DEFAULT_BRANCH_NAME.to_string(),
                    missing_files: true,
                    ..Default::default()
                };
                let result = repositories::push::push_remote_branch(&repo, &opts).await;
                assert!(
                    matches!(&result, Err(OxenError::CannotPushShallowClone { .. })),
                    "expected CannotPushShallowClone, got {result:?}"
                );
                if let Err(OxenError::CannotPushShallowClone { help, .. }) = &result {
                    assert!(
                        help.contains(&test_file_hash),
                        "error should name the locally-absent hash {test_file_hash}: {help}"
                    );
                }
                assert!(
                    !server.version_store().version_exists(&license_hash).await?,
                    "a failed shallow-clone push must upload nothing"
                );

                api::client::repositories::delete(&remote_repo).await?;
                Ok(())
            })
            .await
        })
        .await
    }

    // Users push to one remote past, into, and over each other, each case starting from the
    // remote the case before it left. User A is the fixture's repository, and each other user is
    // a clone of the remote as the case finds it.
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_push_cases_on_readme_remote() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|user_a_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            test::run_empty_dir_test_async(|clones_dir| async move {
                // A branch with no commits of its own pushes
                let new_branch_name = "my-branch";
                repositories::branches::create_checkout(&user_a_repo, new_branch_name)?;
                let opts = PushOpts {
                    remote: DEFAULT_REMOTE_NAME.to_string(),
                    branch: new_branch_name.to_string(),
                    ..Default::default()
                };
                repositories::push::push_remote_branch(&user_a_repo, &opts).await?;
                let remote_branches = api::client::branches::list(&remote_repo).await?;
                assert_eq!(remote_branches.len(), 2, "main and the pushed branch");
                repositories::checkout(&user_a_repo, DEFAULT_BRANCH_NAME).await?;

                // User B deletes README.md after user A modified and pushed it, and cannot push
                let user_b_repo =
                    repositories::clone_url(&remote_repo.remote.url, &clones_dir.join("deletes"))
                        .await?;
                let readme_a = user_a_repo.path.join("README.md");
                test::write_txt_file_to_path(&readme_a, "fancy new file contents")?;
                repositories::add(&user_a_repo, &readme_a).await?;
                repositories::commit(&user_a_repo, "modifying first file path.")?;
                repositories::push(&user_a_repo).await?;
                let readme_b = user_b_repo.path.join("README.md");
                util::fs::remove_file(&readme_b)?;
                repositories::add(&user_b_repo, &readme_b).await?;
                repositories::commit(&user_b_repo, "user B deleting file path.")?;
                assert!(
                    repositories::push(&user_b_repo).await.is_err(),
                    "the remote is ahead of a modify/delete conflict"
                );

                // User B modifies README.md after user A modified and pushed it, cannot push, pulls
                // a conflict, resolves it, and pushes
                let user_b_repo =
                    repositories::clone_url(&remote_repo.remote.url, &clones_dir.join("modifies"))
                        .await?;
                test::write_txt_file_to_path(&readme_a, "I am the README now")?;
                repositories::add(&user_a_repo, &readme_a).await?;
                repositories::commit(&user_a_repo, "User A modifying the README.")?;
                repositories::push(&user_a_repo).await?;
                let readme_b = user_b_repo.path.join("README.md");
                test::write_txt_file_to_path(&readme_b, "I be the README now.")?;
                repositories::add(&user_b_repo, &readme_b).await?;
                repositories::commit(&user_b_repo, "User B modifying the README.")?;
                assert!(
                    repositories::push(&user_b_repo).await.is_err(),
                    "the remote is ahead"
                );
                assert!(
                    repositories::pull(&user_b_repo).await.is_err(),
                    "the pull stops on the conflict"
                );
                let status = repositories::status(&user_b_repo).await?;
                assert!(status.has_merge_conflicts());
                status.print();
                test::write_txt_file_to_path(&readme_b, "No for real. I be the README now.")?;
                repositories::add(&user_b_repo, &readme_b).await?;
                repositories::commit(&user_b_repo, "User B resolving conflicts.")?;
                repositories::push(&user_b_repo).await?;
                repositories::pull(&user_a_repo).await?;

                // A push succeeds when the server already has the commit's directory nodes
                let new_file = user_a_repo.path.join("new_file.txt");
                util::fs::write(&new_file, "I am a new file")?;
                repositories::add(&user_a_repo, &new_file).await?;
                let commit = repositories::commit(&user_a_repo, "Added a new file")?;
                let progress = Arc::new(PushProgress::new());
                progress.set_message("Collecting missing nodes...");
                let mut candidate_nodes: HashSet<MerkleTreeNode> = HashSet::new();
                let Some(commit_node) =
                    repositories::tree::get_root_with_children(&user_a_repo, &commit)?
                else {
                    return Err(OxenError::basic_str("Err: Root not found"));
                };
                candidate_nodes.insert(commit_node.clone());
                commit_node.walk_tree_without_leaves(|node| {
                    candidate_nodes.insert(node.clone());
                    progress.set_message(format!(
                        "Collecting missing nodes... {}",
                        candidate_nodes.len()
                    ));
                });
                progress.set_message(format!("Pushing {} nodes...", candidate_nodes.len()));
                api::client::tree::create_nodes(
                    &user_a_repo,
                    &remote_repo,
                    candidate_nodes.into_iter().map(|node| node.hash).collect(),
                    &progress,
                )
                .await?;
                repositories::push(&user_a_repo).await?;
                api::client::tree::get_node_hash_by_path(
                    &remote_repo,
                    &commit.id,
                    PathBuf::from("new_file.txt"),
                )
                .await?;

                // A file of exactly stream_segment_size() bytes sits at the boundary between the
                // batched-zip and parallel-segment transfer paths
                let segment_size = stream_segment_size();
                let file_path = user_a_repo.path.join("exact_chunk_size_file.bin");
                let file_data: Vec<u8> = vec![42; segment_size as usize];
                AtomicFile::new(&file_path).write(&file_data)?;
                assert_eq!(util::fs::metadata(&file_path)?.len(), segment_size);
                repositories::add(&user_a_repo, &file_path).await?;
                let commit = repositories::commit(
                    &user_a_repo,
                    "Add file with exactly stream_segment_size() bytes",
                )?;
                let branch = repositories::push(&user_a_repo).await?;
                assert!(
                    api::client::commits::get_by_id(&remote_repo, &commit.id)
                        .await?
                        .is_some(),
                    "Remote commit should exist"
                );
                let download_path = clones_dir.join("downloaded_file.bin");
                repositories::download(
                    &remote_repo,
                    "exact_chunk_size_file.bin",
                    &download_path,
                    &branch.name,
                )
                .await?;
                assert_eq!(
                    util::fs::read_bytes_from_path(&download_path)?,
                    file_data,
                    "Downloaded file contents should match the original data"
                );

                // Moving README.md pushes the new path and drops the old one
                let new_path = user_a_repo.path.join("README2.md");
                util::fs::rename(&readme_a, &new_path)?;
                repositories::add(&user_a_repo, new_path).await?;
                repositories::rm(&user_a_repo, &RmOpts::from_path("README.md")).await?;
                let commit = repositories::commit(&user_a_repo, "Moved the readme")?;
                repositories::push(&user_a_repo).await?;
                let dir_entries =
                    api::client::dir::list(&remote_repo, &commit.id, &PathBuf::from(""), 1, 100)
                        .await?;
                assert!(
                    dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "README2.md")
                );
                assert!(
                    !dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "README.md")
                );

                // User A's pull of a clone's change to dir1/file1.txt stops on a conflict, leaves
                // the head where it was, and leaves user A unable to push
                let dir1_path = user_a_repo.path.join("dir1");
                util::fs::create_dir_all(&dir1_path)?;
                let file1_path = dir1_path.join("file1.txt");
                util::fs::write(&file1_path, "Original content")?;
                repositories::add(&user_a_repo, &file1_path).await?;
                repositories::commit(&user_a_repo, "Add dir1/file1.txt")?;
                repositories::push(&user_a_repo).await?;
                let clone_repo =
                    repositories::clone_url(&remote_repo.remote.url, &clones_dir.join("conflicts"))
                        .await?;
                let clone_file1_path = clone_repo.path.join("dir1").join("file1.txt");
                util::fs::write(&clone_file1_path, "Clone modified content")?;
                repositories::add(&clone_repo, &clone_file1_path).await?;
                repositories::commit(&clone_repo, "Modify file1.txt in clone")?;
                repositories::push(&clone_repo).await?;
                util::fs::write(&file1_path, "Original repo modified content")?;
                repositories::add(&user_a_repo, &file1_path).await?;
                let user_a_commit =
                    repositories::commit(&user_a_repo, "Modify file1.txt in original")?;
                assert!(
                    repositories::pull(&user_a_repo).await.is_err(),
                    "Pull should fail due to merge conflict"
                );
                assert_eq!(
                    repositories::commits::head_commit(&user_a_repo)?.id,
                    user_a_commit.id,
                    "Head commit should not change when pull fails due to merge conflict"
                );
                assert!(
                    repositories::status(&user_a_repo)
                        .await?
                        .has_merge_conflicts(),
                    "Should have merge conflicts"
                );
                assert!(
                    repositories::push(&user_a_repo).await.is_err(),
                    "Push should fail due to merge conflict"
                );

                // User C cannot push over a change the clone pushed after user C cloned, and a
                // force push replaces the remote's history with user C's
                let user_c_repo =
                    repositories::clone_url(&remote_repo.remote.url, &clones_dir.join("forces"))
                        .await?;
                util::fs::write(&clone_file1_path, "Clone's second change")?;
                repositories::add(&clone_repo, &clone_file1_path).await?;
                repositories::commit(&clone_repo, "Modify file1.txt in clone again")?;
                repositories::push(&clone_repo).await?;
                let user_c_file1_path = user_c_repo.path.join("dir1").join("file1.txt");
                util::fs::write(&user_c_file1_path, "User C's changes")?;
                repositories::add(&user_c_repo, &user_c_file1_path).await?;
                let user_c_commit = repositories::commit(&user_c_repo, "User C commit")?;
                assert!(
                    repositories::push(&user_c_repo).await.is_err(),
                    "the remote is ahead"
                );
                let opts = PushOpts {
                    remote: DEFAULT_REMOTE_NAME.to_string(),
                    branch: DEFAULT_BRANCH_NAME.to_string(),
                    force: true,
                    ..Default::default()
                };
                repositories::push::push_remote_branch(&user_c_repo, &opts).await?;
                let remote_branch =
                    api::client::branches::get_by_name(&remote_repo, DEFAULT_BRANCH_NAME)
                        .await?
                        .expect("main is on the remote");
                assert_eq!(remote_branch.commit_id, user_c_commit.id);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_cannot_push_two_separate_empty_roots() -> Result<(), OxenError> {
        test::run_no_commit_remote_repo_test(|remote_repo| async move {
            let ret_repo = remote_repo.clone();

            // Clone the first repo
            test::run_empty_dir_test_async(|first_repo_dir| async move {
                println!("test_cannot_push_two_separate_empty_roots clone first repo");
                let first_cloned_repo = repositories::clone_url(
                    &remote_repo.remote.url,
                    &first_repo_dir.join("first_repo"),
                )
                .await?;

                // Clone the second repo
                test::run_empty_dir_test_async(|second_repo_dir| async move {
                    println!("test_cannot_push_two_separate_empty_roots clone second repo");
                    let second_cloned_repo = repositories::clone_url(
                        &remote_repo.remote.url,
                        &second_repo_dir.join("second_repo"),
                    )
                    .await?;

                    // Add to the first repo, after we have the second repo cloned
                    let new_file = "new_file.txt";
                    let new_file_path = first_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                    repositories::add(&first_cloned_repo, &new_file_path).await?;
                    repositories::commit(&first_cloned_repo, "Adding first file path.")?;
                    repositories::push(&first_cloned_repo).await?;

                    // The push to the second version of the same repo should fail
                    // Adding two commits to have a longer history that also should fail
                    let new_file = "new_file_2.txt";
                    let new_file_path = second_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 2")?;
                    repositories::add(&second_cloned_repo, &new_file_path).await?;
                    repositories::commit(&second_cloned_repo, "Adding second file path.")?;

                    let new_file = "new_file_3.txt";
                    let new_file_path = second_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 3")?;
                    repositories::add(&second_cloned_repo, &new_file_path).await?;
                    repositories::commit(&second_cloned_repo, "Adding third file path.")?;

                    // Push should FAIL
                    let result = repositories::push(&second_cloned_repo).await;
                    assert!(result.is_err());

                    Ok(())
                })
                .await?;

                Ok(())
            })
            .await?;

            Ok(ret_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_push_many_commits_new_branch() -> Result<(), OxenError> {
        test::run_many_local_commits_empty_sync_remote_test(|local_repo, remote_repo| async move {
            // Nothing should be synced on remote and no commit objects created
            let history =
                api::client::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(history.len(), 0);

            // Create new local branch
            let new_branch_name = "my-branch";
            repositories::branches::create_checkout(&local_repo, new_branch_name)?;

            // New commit
            let new_file = "new_file.txt";
            let new_file_path = local_repo.path.join(new_file);
            let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
            repositories::add(&local_repo, &new_file_path).await?;
            repositories::commit(&local_repo, "Adding first file path.")?;

            // Push new branch to remote without first syncing main
            let opts = PushOpts {
                remote: DEFAULT_REMOTE_NAME.to_string(),
                branch: new_branch_name.to_string(),
                ..Default::default()
            };
            repositories::push::push_remote_branch(&local_repo, &opts).await?;

            // Should now have 6 commits on remote on new branch
            let history_new =
                api::client::commits::list_commit_history(&remote_repo, new_branch_name).await?;
            assert_eq!(history_new.len(), 6);

            // Should still have no commits on main
            let history_main =
                api::client::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME).await;
            log::debug!("history_main: {history_main:?}");
            // assert_eq!(history_main.len(), 1);
            assert!(history_main.is_err());

            // Back to main
            repositories::checkout(&local_repo, DEFAULT_BRANCH_NAME).await?;

            // Push to remote
            repositories::push(&local_repo).await?;

            // 5 on main
            let history_main =
                api::client::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(history_main.len(), 5);

            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_push_move_entire_directory() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            // Move the README to a new file name
            let train_images = local_repo.path.join("train");
            let new_path = local_repo.path.join("images").join("train");
            util::fs::create_dir_all(local_repo.path.join("images"))?;
            util::fs::rename(&train_images, &new_path)?;

            repositories::add(&local_repo, new_path).await?;
            let rm_opts = RmOpts {
                paths: vec![PathBuf::from("train")],
                staged: false,
                recursive: true,
                ..Default::default()
            };

            repositories::rm(&local_repo, &rm_opts).await?;
            let commit =
                repositories::commit(&local_repo, "Moved all the train image files to images/")?;
            repositories::push(&local_repo).await?;

            let path = PathBuf::from("");
            let page = 1;
            let page_size = 100;
            let dir_entries =
                api::client::dir::list(&remote_repo, &commit.id, &path, page, page_size).await?;
            // check to make sure we only have the images directory and not the train directory
            assert!(
                !dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "train")
            );
            assert!(
                dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "images")
            );

            // Add a single new file
            let new_file = local_repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&local_repo, new_file).await?;
            let commit = repositories::commit(&local_repo, "Added a new file")?;
            repositories::push(&local_repo).await?;

            let dir_entries =
                api::client::dir::list(&remote_repo, &commit.id, &path, page, page_size).await?;
            // make sure we have the new file
            assert!(
                dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "new_file.txt")
            );

            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_push_root_subtree_depth_1() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.fetch_opts.subtree_paths = Some(vec![PathBuf::from(".")]);
                opts.fetch_opts.depth = Some(1);
                let local_repo = repositories::clone::clone(&opts).await?;

                // Add a new file
                let readme_file = local_repo.path.join("ANOTHER_FILE.md");
                util::fs::write_to_path(
                    &readme_file,
                    r"
Q: How can I version a giant dataset of images?
A: Oxen.ai is a great tool for this! It can handle any size dataset, and is optimized for speed.
",
                )?;
                repositories::add(&local_repo, &readme_file).await?;
                let commit = repositories::commit(&local_repo, "Added another file")?;

                let result = repositories::push(&local_repo).await;
                println!("push result: {result:?}");

                assert!(result.is_ok());

                // List the files in the remote repo and confirm the new file is there
                let dir_entries =
                    api::client::dir::list(&remote_repo, &commit.id, &PathBuf::from(""), 1, 100)
                        .await?;

                assert!(
                    dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "ANOTHER_FILE.md")
                );

                Ok(())
            })
            .await?;
            Ok(cloned_remote)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_push_annotations_test_subtree() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.fetch_opts.subtree_paths =
                    Some(vec![PathBuf::from("annotations").join("test")]);
                let local_repo = repositories::clone::clone(&opts).await?;

                let annotations_test_dir = local_repo.path.join("annotations").join("test");

                // Add a new file
                let readme_file = annotations_test_dir.join("README.md");
                util::fs::write_to_path(
                    &readme_file,
                    r"
Q: What is a faster alternative to DVC?
A: Checkout Oxen.ai
",
                )?;
                repositories::add(&local_repo, &readme_file).await?;
                let commit = repositories::commit(&local_repo, "adding README.md to the test dir")?;

                let result = repositories::push(&local_repo).await;
                println!("push result: {result:?}");

                assert!(result.is_ok());

                // Get the file from the remote repo
                let dir_entries = api::client::dir::list(
                    &remote_repo,
                    &commit.id,
                    &PathBuf::from("annotations").join("test"),
                    1,
                    100,
                )
                .await?;
                println!("dir_entries: {dir_entries:?}");

                // Make sure we have the new file
                assert!(
                    dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "README.md")
                );

                Ok(())
            })
            .await?;
            Ok(cloned_remote)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_push_partial_clone_nlp_classification() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo
            test::run_empty_dir_test_async(|repos_base_dir| async move {
                let user_a_repo_dir = repos_base_dir.join("user_a_repo");

                // Make sure to clone a subtree to test subtree merge conflicts
                let mut clone_opts = CloneOpts::new(&remote_repo.remote.url, &user_a_repo_dir);
                clone_opts.fetch_opts.subtree_paths =
                    Some(vec![PathBuf::from("nlp").join("classification")]);
                clone_opts.fetch_opts.depth = Some(2);
                let user_a_repo = repositories::clone(&clone_opts).await?;

                // User adds multiple files and modifies an existing file
                let new_file_1 = PathBuf::from("nlp")
                    .join("classification")
                    .join("new_partial_data_1.tsv");
                let new_file_path_1 = user_a_repo.path.join(&new_file_1);
                let new_file_path_1 =
                    test::write_txt_file_to_path(new_file_path_1, "image\tlabel1")?;
                repositories::add(&user_a_repo, &new_file_path_1).await?;

                let new_file_2 = PathBuf::from("nlp")
                    .join("classification")
                    .join("new_partial_data_2.tsv");
                let new_file_path_2 = user_a_repo.path.join(&new_file_2);
                let new_file_path_2 =
                    test::write_txt_file_to_path(new_file_path_2, "image\tlabel2")?;
                repositories::add(&user_a_repo, &new_file_path_2).await?;

                // Modify an existing file
                let existing_file_path = user_a_repo
                    .path
                    .join("nlp/classification/existing_file.tsv");
                let modified_file_path =
                    test::write_txt_file_to_path(existing_file_path, "image\tmodified_label")?;
                repositories::add(&user_a_repo, &modified_file_path).await?;

                // Commit changes
                let commit = repositories::commit(
                    &user_a_repo,
                    "Adding new partial data files and modifying existing file",
                )?;
                repositories::push(&user_a_repo).await?;

                // Verify that the new files are in the remote repo
                let dir_entries = api::client::dir::list(
                    &remote_repo,
                    &commit.id,
                    &PathBuf::from("nlp").join("classification"),
                    1,
                    100,
                )
                .await?;

                assert!(
                    dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "new_partial_data_1.tsv")
                );
                assert!(
                    dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "new_partial_data_2.tsv")
                );
                assert!(
                    dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "existing_file.tsv")
                );

                let metadata_entry = dir_entries
                    .entries
                    .iter()
                    .find(|entry| entry.filename() == "existing_file.tsv")
                    .unwrap();

                let metadata_entry = match metadata_entry {
                    EMetadataEntry::MetadataEntry(entry) => entry,
                    _ => panic!("Expected a metadata entry"),
                };

                // Verify the content of the modified existing file
                api::client::entries::download_file(
                    &remote_repo,
                    metadata_entry,
                    &PathBuf::from("nlp/classification/existing_file.tsv"),
                    &user_a_repo
                        .path
                        .join("nlp/classification/existing_file.tsv"),
                    &commit.id,
                )
                .await?;
                let modified_file_content = std::fs::read_to_string(
                    user_a_repo
                        .path
                        .join("nlp/classification/existing_file.tsv"),
                )?;
                assert_eq!(modified_file_content, "image\tmodified_label");

                // Verify that the root directory is intact
                let root_dir_entries =
                    api::client::dir::list(&remote_repo, &commit.id, &PathBuf::from(""), 1, 100)
                        .await?;

                assert!(
                    root_dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "README.md")
                );

                // Verify that the original repo structure is intact
                let classification_dir_entries = api::client::dir::list(
                    &remote_repo,
                    &commit.id,
                    &PathBuf::from("nlp").join("classification"),
                    1,
                    100,
                )
                .await?;

                assert!(!classification_dir_entries.entries.is_empty()); // Ensure there are entries in the classification directory

                let root_dir_entries =
                    api::client::dir::list(&remote_repo, &commit.id, &PathBuf::from(""), 1, 100)
                        .await?;

                assert!(
                    root_dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "README.md")
                );

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }
}
