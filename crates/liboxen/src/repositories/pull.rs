//! # oxen pull
//!
//! Pull data from a remote branch
//!

use crate::core;
use crate::error::OxenError;
use crate::model::LocalRepository;

/// Pull a repository's data from default branches origin/main
/// Defaults defined in
/// `constants::DEFAULT_REMOTE_NAME` and `constants::DEFAULT_BRANCH_NAME`
#[tracing::instrument(skip_all, fields(oxen.repository_path = %repo.path.display()))]
pub async fn pull(repo: &LocalRepository) -> Result<(), OxenError> {
    #[cfg(feature = "metrics")]
    metrics::counter!("oxen_pull_total").increment(1);
    #[cfg(feature = "metrics")]
    let timer = std::time::Instant::now();

    let result = core::v_latest::pull::pull(repo).await;

    #[cfg(feature = "metrics")]
    {
        let end = timer.elapsed();
        metrics::histogram!("oxen_pull_duration_ms").record(end.as_millis() as f64);
    }

    result
}

#[tracing::instrument(skip_all, fields(oxen.repository_path = %repo.path.display()))]
pub async fn pull_all(repo: &LocalRepository) -> Result<(), OxenError> {
    #[cfg(feature = "metrics")]
    metrics::counter!("oxen_pull_total").increment(1);
    core::v_latest::pull::pull_all(repo).await
}

/// Pull a specific remote and branch
pub use crate::core::v_latest::pull::pull_remote_branch;

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::api::client::workspaces;
    use crate::api::client::workspaces::files::upload_single_file;

    use crate::config::RepositoryConfig;
    use crate::constants;
    use crate::constants::OXEN_HIDDEN_DIR;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::opts::CloneOpts;
    use crate::opts::FetchOpts;
    use crate::opts::PushOpts;
    use crate::opts::RmOpts;
    use crate::repositories;
    use crate::repositories::LocalRepository;
    use crate::test;
    use crate::util;
    use std::path::Path;
    use std::path::PathBuf;

    // One repository pushes a history to a new remote, and clones of the remote pull from it,
    // push back to it, and move between its branches, until a pull leaves no file in the root
    // directory.
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_command_push_clone_pull_push() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // Three commits: a labels file and a file without an extension, a train dir, and a
            // test dir
            let labels_path = test::write_txt_file_to_path(repo.path.join("labels.txt"), "labels")?;
            let license_contents = "I am the License.";
            let license_path =
                test::write_txt_file_to_path(repo.path.join("LICENSE"), license_contents)?;
            repositories::add(&repo, &labels_path).await?;
            repositories::add(&repo, &license_path).await?;
            repositories::commit(&repo, "Adding labels and LICENSE")?;
            let train_dir = test::populate_dir_with_txt_files(repo.path.join("train"), "train", 3)?;
            repositories::add(&repo, &train_dir).await?;
            repositories::commit(&repo, "Adding train dir")?;
            let test_dir = test::populate_dir_with_txt_files(repo.path.join("test"), "test", 2)?;
            repositories::add(&repo, &test_dir).await?;
            let pushed_commit = repositories::commit(&repo, "Adding test dir")?;
            let remote_repo = test::connect_remote_repo(&mut repo).await?;
            repositories::push(&repo).await?;

            // A fourth commit pushed on its own
            let party_ppl_filename = "party_ppl.txt";
            let party_ppl_path = repo.path.join(party_ppl_filename);
            util::fs::write_to_path(&party_ppl_path, "Wassup Party Ppl")?;
            repositories::add(&repo, &party_ppl_path).await?;
            let latest_commit = repositories::commit(&repo, "Adding party_ppl.txt")?;
            repositories::push(&repo).await?;
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);
            let og_num_commits = repositories::commits::list(&repo)?.len();

            test::run_empty_dir_test_async(|clones_dir| async move {
                // A clone has every committed file and commit, and a pull with nothing new
                // leaves it clean
                let cloned_repo =
                    repositories::clone_url(&remote_repo.remote.url, &clones_dir.join("cloned"))
                        .await?;
                assert!(cloned_repo.path.join(OXEN_HIDDEN_DIR).exists());
                repositories::pull(&cloned_repo).await?;
                assert_eq!(
                    util::fs::rcount_files_in_dir(&cloned_repo.path),
                    og_num_files
                );
                assert_eq!(
                    util::fs::rcount_files_in_dir(&cloned_repo.path.join("train")),
                    3
                );
                let cloned_party_ppl_path = cloned_repo.path.join(party_ppl_filename);
                assert_eq!(
                    util::fs::read_from_path(&cloned_party_ppl_path)?,
                    "Wassup Party Ppl"
                );
                assert_eq!(
                    util::fs::read_from_path(cloned_repo.path.join("LICENSE"))?,
                    license_contents
                );
                let head = repositories::commits::head_commit(&cloned_repo)?;
                assert_eq!(head.id, latest_commit.id);
                assert_eq!(
                    repositories::commits::list(&cloned_repo)?.len(),
                    og_num_commits
                );
                assert!(repositories::status(&cloned_repo).await?.is_clean());

                // A clone of the whole history has every commit
                let mut clone_opts =
                    CloneOpts::new(&remote_repo.remote.url, clones_dir.join("all_history"));
                clone_opts.fetch_opts.all = true;
                let all_history_repo = repositories::clone(&clone_opts).await?;
                assert_eq!(
                    repositories::commits::list(&all_history_repo)?.len(),
                    og_num_commits
                );

                // A clone whose branch lags the remote by a commit, and which has lost that
                // commit's version file, gets the file back from its next pull
                let lagging_repo =
                    repositories::clone_url(&remote_repo.remote.url, &clones_dir.join("lagging"))
                        .await?;
                repositories::branches::update(
                    &lagging_repo,
                    constants::DEFAULT_BRANCH_NAME,
                    &pushed_commit.id,
                )?;
                let party_ppl_node = repositories::tree::get_node_by_path(
                    &lagging_repo,
                    &latest_commit,
                    party_ppl_filename,
                )?
                .expect("party_ppl.txt must be reachable from the latest commit's tree");
                let blob_hash = party_ppl_node.hash.to_string();
                let version_store = lagging_repo.version_store();
                assert!(
                    version_store.version_exists(&blob_hash).await?,
                    "the clone downloaded party_ppl.txt's version file"
                );
                version_store.delete_version(&blob_hash).await?;
                assert!(!version_store.version_exists(&blob_hash).await?);
                repositories::pull(&lagging_repo).await?;
                assert!(
                    version_store.version_exists(&blob_hash).await?,
                    "the pull re-downloaded the version file it deleted"
                );
                let head = repositories::commits::head_commit(&lagging_repo)?;
                assert_eq!(head.id, latest_commit.id);
                assert_eq!(
                    util::fs::read_from_path(lagging_repo.path.join(party_ppl_filename))?,
                    "Wassup Party Ppl"
                );

                // The original pushes a branch with three more files. A clone of that branch
                // has them until it checks out main, and a clone of main gets them by checking
                // out the branch.
                let feature_branch = "feature/add-mooooore-data";
                repositories::branches::create_checkout(&repo, feature_branch)?;
                let more_dir =
                    test::populate_dir_with_txt_files(repo.path.join("more"), "more", 3)?;
                repositories::add(&repo, &more_dir).await?;
                repositories::commit(&repo, "Adding mooooore data")?;
                repositories::push(&repo).await?;
                let opts = CloneOpts::from_branch(
                    remote_repo.url(),
                    clones_dir.join("branch_repo"),
                    feature_branch,
                );
                let branch_repo = repositories::clone(&opts).await?;
                assert_eq!(
                    util::fs::rcount_files_in_dir(&branch_repo.path),
                    og_num_files + 3
                );
                repositories::fetch_all(&branch_repo, &FetchOpts::new()).await?;
                repositories::checkout(&branch_repo, constants::DEFAULT_BRANCH_NAME).await?;
                assert_eq!(
                    util::fs::rcount_files_in_dir(&branch_repo.path),
                    og_num_files
                );
                assert_eq!(
                    util::fs::rcount_files_in_dir(&lagging_repo.path),
                    og_num_files
                );
                repositories::fetch_all(&lagging_repo, &FetchOpts::new()).await?;
                repositories::checkout(&lagging_repo, feature_branch).await?;
                assert_eq!(
                    util::fs::rcount_files_in_dir(&lagging_repo.path),
                    og_num_files + 3
                );
                repositories::checkout(&repo, constants::DEFAULT_BRANCH_NAME).await?;

                // A clone pushes a branch of its own, which the original's pull of main leaves
                // out. The original then checks the branch out and pushes a file to it, which
                // the clone's pull of the branch brings down.
                let collab_branch = "adding-training-data";
                repositories::branches::create_checkout(&cloned_repo, collab_branch)?;
                let hotdog_1 = test::write_txt_file_to_path(
                    cloned_repo.path.join("train").join("hotdog_1.jpg"),
                    "hotdog 1",
                )?;
                repositories::add(&cloned_repo, &hotdog_1).await?;
                repositories::commit(&cloned_repo, "Adding one file to train dir")?;
                let collab_push_opts = PushOpts {
                    remote: constants::DEFAULT_REMOTE_NAME.to_string(),
                    branch: collab_branch.to_string(),
                    ..Default::default()
                };
                repositories::push::push_remote_branch(&cloned_repo, &collab_push_opts).await?;
                let fetch_opts = FetchOpts {
                    all: true,
                    ..FetchOpts::new()
                };
                repositories::pull_remote_branch(&repo, &fetch_opts).await?;
                assert_eq!(
                    util::fs::rcount_files_in_dir(&repo.path),
                    og_num_files,
                    "pulling main leaves out the branch's file"
                );
                let fetch_opts = FetchOpts {
                    all: true,
                    ..FetchOpts::from_branch(collab_branch)
                };
                repositories::fetch::fetch_branch(&repo, &fetch_opts).await?;
                repositories::checkout(&repo, collab_branch).await?;
                assert_eq!(util::fs::rcount_files_in_dir(&repo.path), og_num_files + 1);
                let hotdog_2 =
                    test::write_txt_file_to_path(train_dir.join("hotdog_2.jpg"), "hotdog 2")?;
                repositories::add(&repo, &hotdog_2).await?;
                repositories::commit(&repo, "Adding next file to train dir")?;
                repositories::push::push_remote_branch(&repo, &collab_push_opts).await?;
                let fetch_opts = FetchOpts::from_branch(collab_branch);
                repositories::pull_remote_branch(&cloned_repo, &fetch_opts).await?;
                assert_eq!(
                    util::fs::rcount_files_in_dir(&cloned_repo.path),
                    og_num_files + 2
                );
                repositories::checkout(&repo, constants::DEFAULT_BRANCH_NAME).await?;
                repositories::checkout(&cloned_repo, constants::DEFAULT_BRANCH_NAME).await?;

                // The clone pushes a new file, which the original pulls without its own files
                // showing as modified
                let send_it_back_filename = "send_it_back.txt";
                let send_it_back_contents = "Hello from the other side";
                let send_it_back_path = cloned_repo.path.join(send_it_back_filename);
                util::fs::write_to_path(&send_it_back_path, send_it_back_contents)?;
                repositories::add(&cloned_repo, &send_it_back_path).await?;
                repositories::commit(&cloned_repo, "Adding send_it_back.txt")?;
                repositories::push(&cloned_repo).await?;
                repositories::pull(&repo).await?;
                assert!(!repositories::status(&repo).await?.has_modified_entries());
                let pulled_send_it_back_path = repo.path.join(send_it_back_filename);
                assert_eq!(
                    util::fs::read_from_path(&pulled_send_it_back_path)?,
                    send_it_back_contents
                );

                // Each side modifies a file and pushes it, and the other side's pull brings the
                // change down
                util::fs::write_to_path(&party_ppl_path, "Late to the party")?;
                repositories::add(&repo, &party_ppl_path).await?;
                repositories::commit(&repo, "Modified party ppl contents")?;
                repositories::push(&repo).await?;
                repositories::pull(&cloned_repo).await?;
                assert_eq!(
                    util::fs::read_from_path(&cloned_party_ppl_path)?,
                    "Late to the party"
                );
                let cloned_labels_path = cloned_repo.path.join("labels.txt");
                util::fs::write_to_path(&cloned_labels_path, "messing up the labels")?;
                repositories::add(&cloned_repo, &cloned_labels_path).await?;
                repositories::commit(&cloned_repo, "I messed with the label file")?;
                repositories::push(&cloned_repo).await?;
                repositories::pull(&repo).await?;
                assert_eq!(
                    util::fs::read_from_path(&labels_path)?,
                    "messing up the labels"
                );

                // The clone removes a file, and the original's pull removes it too
                util::fs::remove_file(&send_it_back_path)?;
                repositories::add(&cloned_repo, &send_it_back_path).await?;
                repositories::commit(&cloned_repo, "Removing the send it back file")?;
                repositories::push(&cloned_repo).await?;
                repositories::pull(&repo).await?;
                assert!(!pulled_send_it_back_path.exists());

                // The original removes every file in the root directory, and the clone's pull
                // still works with none left there
                for path in [&labels_path, &license_path, &party_ppl_path] {
                    util::fs::remove_file(path)?;
                    repositories::add(&repo, path).await?;
                }
                repositories::commit(&repo, "You mess with it, I remove it")?;
                repositories::push(&repo).await?;
                repositories::pull(&cloned_repo).await?;
                for filename in ["labels.txt", "LICENSE", party_ppl_filename] {
                    assert!(
                        !cloned_repo.path.join(filename).exists(),
                        "{filename} is removed"
                    );
                }
                assert_eq!(
                    util::fs::rcount_files_in_dir(&cloned_repo.path.join("train")),
                    3
                );

                api::client::repositories::delete(&remote_repo).await?;

                Ok(())
            })
            .await
        })
        .await
    }

    // A clone of a remote pulls back a version file only when asked to, keeps the files a pull
    // would otherwise overwrite or remove, and merges a commit of its own with the remote's
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_pull_does_not_remove_local_files() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|user_a_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|clones_dir| async move {
                // A clone that lost a version file gets it back from a pull with
                // `missing_files` set, and not from a plain pull
                let user_b_repo =
                    repositories::clone_url(&remote_repo.remote.url, &clones_dir.join("user_b"))
                        .await?;
                let version_store = user_b_repo.version_store();
                let versions = version_store.list_versions().await?;
                assert!(
                    !versions.is_empty(),
                    "Cloned repo should have version files"
                );
                let deleted_hash = versions[0].clone();
                version_store.delete_version(&deleted_hash).await?;
                assert!(
                    !version_store.version_exists(&deleted_hash).await?,
                    "Version file should be deleted"
                );
                repositories::pull(&user_b_repo).await?;
                assert!(
                    !version_store.version_exists(&deleted_hash).await?,
                    "Normal pull should not re-download deleted version"
                );
                let fetch_opts = FetchOpts {
                    missing_files: true,
                    ..FetchOpts::new()
                };
                repositories::pull_remote_branch(&user_b_repo, &fetch_opts).await?;
                assert!(
                    version_store.version_exists(&deleted_hash).await?,
                    "pull --missing-files should re-download deleted version"
                );

                // User A pushes two files, one of which user B holds untracked with other
                // content, beside untracked files and directories of its own
                let file_1 = "file_1.txt";
                let file_2 = "file_2.txt";
                test::write_txt_file_to_path(user_a_repo.path.join(file_1), "File 1")?;
                test::write_txt_file_to_path(user_a_repo.path.join(file_2), "File 2")?;
                repositories::add(&user_a_repo, user_a_repo.path.join(file_1)).await?;
                repositories::add(&user_a_repo, user_a_repo.path.join(file_2)).await?;
                repositories::commit(&user_a_repo, "Adding file_1 and file_2")?;
                repositories::push(&user_a_repo).await?;
                let local_file_2 = user_b_repo.path.join(file_2);
                util::fs::write_to_path(&local_file_2, "wrong not correct content")?;
                let file_3 = "file_3.txt";
                let untracked = [
                    file_3,
                    "dir_2/file_4.txt",
                    "dir_2/file_5.txt",
                    "dir_3/subdir/subfile.txt",
                ];
                for path in untracked {
                    util::fs::write_to_path(user_b_repo.path.join(path), path)?;
                }
                std::fs::create_dir(user_b_repo.path.join("dir_1"))?;

                // The pull refuses to overwrite the untracked file_2. A second pull refuses as
                // well, so a failed pull leaves nothing that lets the next one overwrite it.
                for _ in 0..2 {
                    let result = repositories::pull(&user_b_repo).await;
                    assert!(
                        result.is_err(),
                        "the pull would overwrite the untracked file_2"
                    );
                    assert_eq!(
                        util::fs::read_from_path(&local_file_2)?,
                        "wrong not correct content"
                    );
                }

                // Once the untracked file_2 is gone, the pull brings down both files and leaves
                // every other untracked file in place
                util::fs::remove_file(&local_file_2)?;
                repositories::pull(&user_b_repo).await?;
                assert!(user_b_repo.path.join(file_1).exists());
                assert_eq!(util::fs::read_from_path(&local_file_2)?, "File 2");
                for path in untracked {
                    assert!(
                        user_b_repo.path.join(path).exists(),
                        "{path} survives the pull"
                    );
                }
                assert!(user_b_repo.path.join("dir_1").exists());

                // User B commits file_3 without pushing, and a pull merges user A's next commit
                // with it
                let file_6 = "file_6.txt";
                test::write_txt_file_to_path(user_a_repo.path.join(file_6), "File 6")?;
                repositories::add(&user_a_repo, user_a_repo.path.join(file_6)).await?;
                repositories::commit(&user_a_repo, "Adding file_6")?;
                repositories::push(&user_a_repo).await?;
                repositories::add(&user_b_repo, user_b_repo.path.join(file_3)).await?;
                repositories::commit(&user_b_repo, "Adding file_3")?;
                repositories::pull(&user_b_repo).await?;
                assert!(user_b_repo.path.join(file_3).exists());
                assert!(user_b_repo.path.join(file_6).exists());

                // User A copies a file to two new places and deletes the original in one
                // commit, and a deep clone has the copies and not the original
                let contents = "this is the file";
                let a_path = user_a_repo.path.join("a.txt");
                test::write_txt_file_to_path(&a_path, contents)?;
                repositories::add(&user_a_repo, &a_path).await?;
                repositories::commit(&user_a_repo, "Adding file for first time")?;
                let new_folder = user_a_repo.path.join("newfolder");
                util::fs::create_dir_all(&new_folder)?;
                for filename in ["a.txt", "b.txt"] {
                    let new_path =
                        test::write_txt_file_to_path(new_folder.join(filename), contents)?;
                    repositories::add(&user_a_repo, &new_path).await?;
                }
                util::fs::remove_file(&a_path)?;
                repositories::add(&user_a_repo, &a_path).await?;
                repositories::commit(
                    &user_a_repo,
                    "Moved file to 2 new places and deleted original",
                )?;
                repositories::push(&user_a_repo).await?;
                let deep_repo =
                    repositories::deep_clone_url(&remote_repo.remote.url, &clones_dir.join("deep"))
                        .await?;
                assert!(deep_repo.path.join("newfolder").join("a.txt").exists());
                assert!(deep_repo.path.join("newfolder").join("b.txt").exists());
                assert!(!deep_repo.path.join("a.txt").exists());

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // Clones of a remote pull what another user pushes without overwriting a README the pull
    // would replace or remove, push a new branch, and resolve a merge conflict to push again
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_flags_merge_conflict_on_pull() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|pusher_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|clones_dir| async move {
                let clone = |name: &str| {
                    let dir = clones_dir.join(name);
                    let url = remote_repo.remote.url.clone();
                    async move { repositories::clone_url(&url, &dir).await }
                };
                let push_file = async |filename: &str, contents: &str, message: &str| {
                    let path =
                        test::write_txt_file_to_path(pusher_repo.path.join(filename), contents)?;
                    repositories::add(&pusher_repo, &path).await?;
                    repositories::commit(&pusher_repo, message)?;
                    repositories::push(&pusher_repo).await
                };
                let user_a_readme = "# User A README";

                // A clone pushes a new branch, and the remote's branch points at its commit
                let branch_repo = clone("branch_repo").await?;
                let branch_name = "new-branch";
                repositories::branches::create_checkout(&branch_repo, branch_name)?;
                let path = test::write_txt_file_to_path(branch_repo.path.join("a.txt"), "a")?;
                repositories::add(&branch_repo, &path).await?;
                let commit = repositories::commit(&branch_repo, "Adding file for first time")?;
                let opts = PushOpts {
                    remote: constants::DEFAULT_REMOTE_NAME.to_string(),
                    branch: branch_name.to_string(),
                    ..Default::default()
                };
                repositories::push::push_remote_branch(&branch_repo, &opts).await?;
                let remote_branch = api::client::branches::get_by_name(&remote_repo, branch_name)
                    .await?
                    .unwrap();
                assert_eq!(remote_branch.commit_id, commit.id);

                // A clone of that branch fetches main, whose only commit is the branch's parent,
                // and checking main out drops the branch's file
                let opts = CloneOpts::from_branch(
                    remote_repo.url(),
                    clones_dir.join("from_branch"),
                    branch_name,
                );
                let from_branch_repo = repositories::clone(&opts).await?;
                assert!(from_branch_repo.path.join("a.txt").exists());
                repositories::fetch_all(&from_branch_repo, &FetchOpts::new()).await?;
                repositories::checkout(&from_branch_repo, constants::DEFAULT_BRANCH_NAME).await?;
                assert!(!from_branch_repo.path.join("a.txt").exists());
                assert!(from_branch_repo.path.join("README.md").exists());

                // A clone modifies the README, and then the remote's README changes. A second
                // pull refuses as well, so a failed pull leaves nothing that lets the next one
                // overwrite the README.
                let readme_repo = clone("readme_repo").await?;
                let local_readme = readme_repo.path.join("README.md");
                test::write_txt_file_to_path(&local_readme, user_a_readme)?;
                push_file(
                    "README.md",
                    "Hello from another user b README",
                    "Updating the README",
                )
                .await?;
                for _ in 0..2 {
                    let result = repositories::pull(&readme_repo).await;
                    assert!(
                        result.is_err(),
                        "the pull would overwrite the modified README"
                    );
                    assert_eq!(util::fs::read_from_path(&local_readme)?, user_a_readme);
                }

                // The remote gains a different file than the one a clone modified, so the pull
                // succeeds and keeps the modified README
                let other_file_repo = clone("other_file_repo").await?;
                push_file("other_file.txt", "Hello from user b", "Adding another file").await?;
                let local_readme = other_file_repo.path.join("README.md");
                test::write_txt_file_to_path(&local_readme, user_a_readme)?;
                for _ in 0..2 {
                    repositories::pull(&other_file_repo).await?;
                    assert_eq!(util::fs::read_from_path(&local_readme)?, user_a_readme);
                }

                // The remote then removes the README that clone modified
                let rm_opts = RmOpts {
                    paths: vec![PathBuf::from("README.md")],
                    ..Default::default()
                };
                repositories::rm(&pusher_repo, &rm_opts).await?;
                repositories::commit(&pusher_repo, "Removing the README.md on the remote")?;
                repositories::push(&pusher_repo).await?;
                for _ in 0..2 {
                    let result = repositories::pull(&other_file_repo).await;
                    assert!(result.is_err(), "the pull would remove the modified README");
                    assert_eq!(util::fs::read_from_path(&local_readme)?, user_a_readme);
                }

                // Two users commit the same new file. The second one's push and pull fail with a
                // merge conflict, which taking its own version resolves, and its push succeeds.
                let user_b_repo = clone("user_b_repo").await?;
                let conflict_file = "conflict.txt";
                push_file(conflict_file, "new file", "User A changing file.").await?;
                let new_file_path = test::write_txt_file_to_path(
                    user_b_repo.path.join(conflict_file),
                    "I am user B, try to stop me",
                )?;
                repositories::add(&user_b_repo, &new_file_path).await?;
                repositories::commit(&user_b_repo, "User B changing file.")?;
                assert!(
                    repositories::push(&user_b_repo).await.is_err(),
                    "user A pushed conflict.txt first"
                );
                assert!(
                    repositories::pull(&user_b_repo).await.is_err(),
                    "both users added conflict.txt"
                );
                let status = repositories::status(&user_b_repo).await?;
                assert!(!status.merge_conflicts.is_empty());
                repositories::checkout::checkout_ours(&user_b_repo, conflict_file).await?;
                repositories::add(&user_b_repo, &new_file_path).await?;
                repositories::commit(&user_b_repo, "Taking my changes")?;
                repositories::push(&user_b_repo).await?;

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // Clones of a remote pull its data frames with their schemas, and a shallow clone of one
    // subtree pulls the full history
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_pull_multiple_data_frames_multiple_schemas() -> Result<(), OxenError> {
        test::run_select_data_sync_remote("annotations nlp test", |repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            let commit = repositories::commits::head_commit(&repo)?;
            let num_schemas = repositories::data_frames::schemas::list(&repo, &commit)?.len();
            assert!(num_schemas > 0, "the data frames have schemas");

            // A second commit, outside the subtree the shallow clone takes
            let labels_path = test::write_txt_file_to_path(repo.path.join("labels.txt"), "cat")?;
            repositories::add(&repo, &labels_path).await?;
            repositories::commit(&repo, "Adding labels")?;
            repositories::push(&repo).await?;
            let og_num_commits = repositories::commits::list_all(&repo)?.len();

            test::run_empty_dir_test_async(|clones_dir| async move {
                let cloned_repo =
                    repositories::clone_url(&remote_repo.remote.url, &clones_dir.join("repoo"))
                        .await?;
                let data_frames = [
                    Path::new("nlp")
                        .join("classification")
                        .join("annotations")
                        .join("train.tsv"),
                    test::test_bounding_box_csv(),
                ];
                for filename in &data_frames {
                    assert_eq!(
                        util::fs::read_from_path(cloned_repo.path.join(filename))?,
                        util::fs::read_from_path(repo.path.join(filename))?,
                        "{filename:?}"
                    );
                }

                // The clone's status is clean and prints as clean
                let status = repositories::status(&cloned_repo).await?;
                assert!(status.is_clean());
                status.print();

                // The clone has every schema
                let head_commit = repositories::commits::head_commit(&cloned_repo)?;
                let pulled_schemas =
                    repositories::data_frames::schemas::list(&cloned_repo, &head_commit)?;
                assert_eq!(pulled_schemas.len(), num_schemas);

                // A shallow clone of one subtree pulls all the commits
                let mut opts = CloneOpts::new(&remote_repo.remote.url, clones_dir.join("shallow"));
                opts.fetch_opts.subtree_paths = Some(vec![PathBuf::from("test")]);
                opts.fetch_opts.depth = Some(1);
                let shallow_repo = repositories::clone(&opts).await?;
                repositories::pull_all(&shallow_repo).await?;
                assert_eq!(
                    repositories::commits::list_all(&shallow_repo)?.len(),
                    og_num_commits
                );

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // Deal with merge conflicts on subtree clones
    // 1) Clone subtree to user A
    // 2) Clone subtree to user B
    // 3) User A changes file commit and pushes
    // 4) User B changes same file, commits, and pushes and fails
    // 5) User B pulls user A's changes, there is a merge conflict
    // 6) User B cannot push until merge conflict is resolved
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_flags_merge_conflict_on_subtree_pull() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|repos_base_dir| async move {
                let user_a_repo_dir = repos_base_dir.join("user_a_repo");

                // Make sure to clone a subtree to test subtree merge conflicts
                let mut clone_opts = CloneOpts::new(&remote_repo.remote.url, &user_a_repo_dir);
                clone_opts.fetch_opts.subtree_paths =
                    Some(vec![PathBuf::from("nlp").join("classification")]);
                clone_opts.fetch_opts.depth = Some(2);
                let user_a_repo = repositories::clone(&clone_opts).await?;

                // Clone Repo to User B
                let user_b_repo_dir = repos_base_dir.join("user_b_repo");

                let mut clone_opts = CloneOpts::new(&remote_repo.remote.url, &user_b_repo_dir);
                clone_opts.fetch_opts.subtree_paths =
                    Some(vec![PathBuf::from("nlp").join("classification")]);
                clone_opts.fetch_opts.depth = Some(2);
                let user_b_repo = repositories::clone(&clone_opts).await?;

                // User A adds a file and pushes
                let new_file = PathBuf::from("nlp")
                    .join("classification")
                    .join("new_data.tsv");
                let new_file_path = user_a_repo.path.join(&new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "image\tlabel")?;
                repositories::add(&user_a_repo, &new_file_path).await?;
                repositories::commit(&user_a_repo, "User A adding new data.")?;
                repositories::push(&user_a_repo).await?;

                // User B adds the same file and pushes
                let new_file_path = user_b_repo.path.join(&new_file);
                let new_file_path =
                    test::write_txt_file_to_path(new_file_path, "I am user B, try to stop me")?;
                repositories::add(&user_b_repo, &new_file_path).await?;
                repositories::commit(&user_b_repo, "User B adding the same file.")?;

                // Push should fail
                let result = repositories::push(&user_b_repo).await;
                assert!(result.is_err());

                // Pull
                let result = repositories::pull(&user_b_repo).await;
                assert!(result.is_err());

                // Make sure it's not a full clone
                assert_eq!(user_b_repo.depth(), Some(2));
                assert_eq!(
                    user_b_repo.subtree_paths(),
                    Some(vec![PathBuf::from("nlp").join("classification")])
                );
                assert!(user_b_repo.path.join("nlp").join("classification").exists());
                assert!(!user_b_repo.path.join("train").exists());

                // Check for merge conflict
                let status = repositories::status(&user_b_repo).await?;
                assert!(!status.merge_conflicts.is_empty());
                status.print();

                // Checkout your version and add the changes
                repositories::checkout::checkout_ours(&user_b_repo, new_file).await?;
                repositories::add(&user_b_repo, &new_file_path).await?;
                // Commit the changes
                repositories::commit(&user_b_repo, "Taking my changes")?;

                // Push should succeed
                repositories::push(&user_b_repo).await?;

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // Deal with merge conflicts on ROOT subtree clones
    // 1) Clone subtree to user A
    // 2) Clone subtree to user B
    // 3) User A changes file commit and pushes
    // 4) User B changes same file, commits, and pushes and fails
    // 5) User B pulls user A's changes, there is a merge conflict
    // 6) User B cannot push until merge conflict is resolved
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_flags_merge_conflict_on_root_subtree_pull() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|repos_base_dir| async move {
                let user_a_repo_dir = repos_base_dir.join("user_a_repo");

                // Make sure to clone a subtree to test subtree merge conflicts
                let mut clone_opts = CloneOpts::new(&remote_repo.remote.url, &user_a_repo_dir);
                clone_opts.fetch_opts.subtree_paths = Some(vec![PathBuf::from(".")]);
                clone_opts.fetch_opts.depth = Some(1);
                let user_a_repo = repositories::clone(&clone_opts).await?;

                // Clone Repo to User B
                let user_b_repo_dir = repos_base_dir.join("user_b_repo");

                let mut clone_opts = CloneOpts::new(&remote_repo.remote.url, &user_b_repo_dir);
                clone_opts.fetch_opts.subtree_paths = Some(vec![PathBuf::from(".")]);
                clone_opts.fetch_opts.depth = Some(1);
                let user_b_repo = repositories::clone(&clone_opts).await?;

                // User A adds a file and pushes
                let new_file = PathBuf::from("README.md");
                let new_file_path = user_a_repo.path.join(&new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "User A's README")?;
                repositories::add(&user_a_repo, &new_file_path).await?;
                repositories::commit(&user_a_repo, "User A adding new data.")?;
                repositories::push(&user_a_repo).await?;

                // User B adds the same file and pushes
                let new_file_path = user_b_repo.path.join(&new_file);
                let new_file_path =
                    test::write_txt_file_to_path(new_file_path, "I am user B, try to stop me")?;
                repositories::add(&user_b_repo, &new_file_path).await?;
                repositories::commit(&user_b_repo, "User B adding the same README.")?;

                // Push should fail
                let result = repositories::push(&user_b_repo).await;
                assert!(result.is_err());

                // Pull
                let result = repositories::pull(&user_b_repo).await;
                assert!(result.is_err());

                // Make sure it's not a full clone
                assert_eq!(user_b_repo.depth(), Some(1));
                assert_eq!(user_b_repo.subtree_paths(), Some(vec![PathBuf::from("")]),);
                assert!(user_b_repo.path.join("README.md").exists());
                assert!(!user_b_repo.path.join("nlp").exists());
                assert!(!user_b_repo.path.join("train").exists());

                // Check for merge conflict
                let status = repositories::status(&user_b_repo).await?;
                status.print();
                assert!(!status.merge_conflicts.is_empty());
                assert_eq!(status.merge_conflicts.len(), 1);
                assert_eq!(status.merge_conflicts[0].base_entry.path, new_file);
                assert_eq!(status.removed_files.len(), 0);

                // Checkout your version and add the changes
                repositories::checkout::checkout_ours(&user_b_repo, new_file).await?;
                repositories::add(&user_b_repo, &new_file_path).await?;
                // Commit the changes
                repositories::commit(&user_b_repo, "Taking my changes")?;

                // Push should succeed
                repositories::push(&user_b_repo).await?;

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_subtree_clone_branch_push_pull() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            test::run_empty_dir_test_async(|repo_dir| async move {
                let repo_dir = repo_dir.join("subtree_repo");

                // 1. Clone repo with subtree filter
                let mut clone_opts = CloneOpts::new(&remote_repo.remote.url, &repo_dir);
                clone_opts.fetch_opts.subtree_paths = Some(vec![PathBuf::from("train")]);
                // clone_opts.fetch_opts.depth = Some(1);
                let repo = repositories::clone(&clone_opts).await?;

                // Verify we only have the subtree
                assert!(repo.path.join("train").exists());
                assert!(!repo.path.join("test").exists());

                // 2. Create and checkout new branch
                let branch_name = "branch1";
                repositories::branches::create_checkout(&repo, branch_name)?;

                // 3. Add new file in dir1
                let dir1 = repo.path.join("dir1");
                util::fs::create_dir_all(&dir1)?;
                let new_file = dir1.join("newfile.txt");
                test::write_txt_file_to_path(&new_file, "This is a new file")?;

                // 4. Add and commit the new file
                repositories::add(&repo, &new_file).await?;
                repositories::commit(&repo, "Adding new file in dir1")?;
                let opts = PushOpts {
                    remote: constants::DEFAULT_REMOTE_NAME.to_string(),
                    branch: branch_name.to_string(),
                    ..Default::default()
                };

                // 5. Push to origin branch1
                repositories::push::push_remote_branch(&repo, &opts).await?;

                // 6. Pull from main
                let pull_result = repositories::pull_remote_branch(
                    &repo,
                    &FetchOpts {
                        remote: constants::DEFAULT_REMOTE_NAME.to_string(),
                        branch: "main".to_string(),
                        all: false,
                        subtree_paths: Some(vec![PathBuf::from("train")]),
                        depth: None,
                        ..FetchOpts::new()
                    },
                )
                .await;

                // Pulling main into branch1 is a no-op: main is an ancestor of branch1 (branch1
                // was branched from main and added newfile.txt on top), so the merge step
                // succeeds as "Already up to date" with no new merge commit.
                pull_result?;

                // Verify the state after pull
                assert!(repo.path.join("train").exists());
                assert!(repo.path.join("dir1").join("newfile.txt").exists());

                // The new file lives outside the cloned subtree; confirm the push actually
                // uploaded it by fetching it back from the server (not just trusting the local
                // copy), so a regression that drops out-of-subtree files would fail here.
                let downloaded = repo.path.join("downloaded_newfile.txt");
                api::client::entries::download_entry(
                    &remote_repo,
                    "dir1/newfile.txt",
                    &downloaded,
                    branch_name,
                )
                .await?;
                assert_eq!(util::fs::read_from_path(&downloaded)?, "This is a new file");

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // Pull and merge a non-empty remote with a non-empty repo
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_pull_and_merge_non_empty_repos() -> Result<(), OxenError> {
        // Create a remote repo with a commit
        test::run_remote_created_and_readme_remote_repo_test(|remote_repo| async move {
            // Commit from a workspace on the repository the server created
            let workspace_id = "server-created";
            workspaces::create(&remote_repo, constants::DEFAULT_BRANCH_NAME, workspace_id).await?;
            let hello = test::test_text_file_with_name("hello.txt");
            upload_single_file(&remote_repo, workspace_id, "server", &hello).await?;
            let body = NewCommitBody {
                message: "Add hello from a workspace".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            workspaces::commit(
                &remote_repo,
                constants::DEFAULT_BRANCH_NAME,
                workspace_id,
                &body,
            )
            .await?;

            let remote_repo_clone = remote_repo.clone();
            test::run_empty_dir_test_async(|dir| async move {
                // Create an empty repo locally
                let mut local_repo = LocalRepository::new(dir, RepositoryConfig::default())?;

                // Add and commit a new file
                let hello_file = local_repo.path.join("dir").join("hello.txt");
                util::fs::write_to_path(
                    &hello_file,
                    "Oxen.ai is the best data version control system.",
                )?;

                repositories::add(&local_repo, &hello_file).await?;
                repositories::commit(&local_repo, "Hello!")?;

                // Set the remote
                test::attach_remote_repo(&mut local_repo, &remote_repo_clone)?;

                // Pull the remote
                repositories::pull(&local_repo).await?;

                // The local and remote files should all exist in the local repo
                assert!(hello_file.exists());
                assert!(PathBuf::from("README.md").exists());
                assert!(local_repo.path.join("server").join("hello.txt").exists());

                // There should be 4 total commits: the remote's two, the local one, and the merge
                let commits = repositories::commits::list_all(&local_repo)?;
                assert_eq!(commits.len(), 4);

                Ok(())
            })
            .await?;

            Ok(remote_repo)
        })
        .await
    }
}
