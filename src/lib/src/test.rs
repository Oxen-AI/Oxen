//! Helpers for our unit and integration tests
//!

use crate::api;
use crate::command;
use crate::constants;

use crate::constants::DEFAULT_REMOTE_NAME;
use crate::core::refs::RefWriter;

use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::data_frame::schema::Field;
use crate::model::RepoNew;
use crate::model::Schema;
use crate::model::{LocalRepository, RemoteRepository};
use crate::opts::RmOpts;
use crate::repositories;
use crate::util;

use rand::distributions::Alphanumeric;
use rand::Rng;
use std::fs::File;
use std::fs::OpenOptions;
use std::future::Future;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

pub const DEFAULT_TEST_HOST: &str = "localhost:3000";

pub fn test_run_dir() -> PathBuf {
    PathBuf::from("data").join("test").join("runs")
}

pub fn test_host() -> String {
    match std::env::var("OXEN_TEST_HOST") {
        Ok(host) => host,
        Err(_err) => String::from(DEFAULT_TEST_HOST),
    }
}

fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

pub fn repo_remote_url_from(name: &str) -> String {
    // Tests always point to localhost
    api::endpoint::remote_url_from_namespace_name(
        test_host().as_str(),
        constants::DEFAULT_NAMESPACE,
        name,
    )
}

pub fn init_test_env() {
    // check if logger is already initialized
    util::logging::init_logging();

    unsafe {
        std::env::set_var("TEST", "true");
    }
}

fn create_prefixed_dir(
    base_dir: impl AsRef<Path>,
    prefix: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let base_dir = base_dir.as_ref();
    let prefix = prefix.as_ref();
    let repo_name = prefix
        .join(base_dir)
        .join(format!("{}", uuid::Uuid::new_v4()));
    let full_dir = Path::new(base_dir).join(repo_name);
    util::fs::create_dir_all(&full_dir)?;
    Ok(full_dir)
}

pub fn create_repo_dir(base_dir: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    create_prefixed_dir(base_dir, "repo")
}

fn create_empty_dir(base_dir: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    create_prefixed_dir(base_dir, "dir")
}

pub async fn create_remote_repo(repo: &LocalRepository) -> Result<RemoteRepository, OxenError> {
    let repo_new = RepoNew::from_namespace_name_host(
        constants::DEFAULT_NAMESPACE,
        repo.dirname(),
        test_host(),
    );
    api::client::repositories::create_from_local(repo, repo_new).await
}

pub fn add_n_files_m_dirs(
    repo: &LocalRepository,
    num_files: u64,
    num_dirs: u64,
) -> Result<(), OxenError> {
    /*
    README.md
    files.csv
    files/
      file1.txt
      file2.txt
      ..
      fileN.txt
    */

    let readme_file = repo.path.join("README.md");
    util::fs::write_to_path(&readme_file, format!("Repo with {} files", num_files))?;

    repositories::add(repo, &readme_file)?;

    // Write files.csv
    let files_csv = repo.path.join("files.csv");
    let mut file = File::create(&files_csv)?;
    file.write_all(b"file,label\n")?;
    for i in 0..num_files {
        let label = if i % 2 == 0 { "cat" } else { "dog" };
        file.write_all(format!("file{}.txt,{}\n", i, label).as_bytes())?;
    }
    file.flush()?;

    // Write files
    let files_dir = repo.path.join("files");
    util::fs::create_dir_all(&files_dir)?;
    for i in 0..num_files {
        // Create num_dirs directories
        let dir_num = i % num_dirs;
        let dir_path = files_dir.join(format!("dir_{}", dir_num));
        util::fs::create_dir_all(&dir_path)?;

        let file_file = dir_path.join(format!("file{}.txt", i));
        util::fs::write_to_path(&file_file, format!("File {}", i))?;
    }

    repositories::add(repo, &files_csv)?;
    repositories::add(repo, &files_dir)?;

    Ok(())
}

/// # Run a unit test on a test repo directory
///
/// This function will create a directory with a uniq name
/// and take care of cleaning it up afterwards
///
/// ```
/// # use liboxen::test;
/// test::run_empty_dir_test(|repo_dir| {
///   // do your fancy testing here
///   assert!(true);
///   Ok(())
/// });
/// ```
pub fn run_empty_dir_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(&Path) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    log::info!("<<<<< run_empty_dir_test start");
    let repo_dir = create_empty_dir(test_run_dir())?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_empty_dir_test running test");
    let result = std::panic::catch_unwind(|| match test(&repo_dir) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());

    Ok(())
}

pub async fn run_empty_dir_test_async<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(PathBuf) -> Fut,
    Fut: Future<Output = Result<PathBuf, OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_empty_dir_test_async start");
    let repo_dir = create_empty_dir(test_run_dir())?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_empty_dir_test_async running test");
    let result = match test(repo_dir).await {
        Ok(repo_dir) => {
            // Remove repo dir
            util::fs::remove_dir_all(repo_dir)?;
            true
        }
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Assert everything okay after we cleanup the repo dir
    assert!(result);

    Ok(())
}

pub fn run_empty_local_repo_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    log::info!("<<<<< run_empty_local_repo_test start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    log::info!(">>>>> run_empty_local_repo_test running test");
    let result = std::panic::catch_unwind(|| match test(repo) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

pub fn run_empty_local_repo_test_w_version<T>(
    version: MinOxenVersion,
    test: T,
) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    log::info!("<<<<< run_empty_local_repo_test start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init::init_with_version(&repo_dir, version)?;

    log::info!(">>>>> run_empty_local_repo_test running test");
    let result = std::panic::catch_unwind(|| match test(repo) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    // util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

pub async fn run_empty_local_repo_test_async<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Fut,
    Fut: Future<Output = Result<(), OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_empty_local_repo_test_async start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    log::info!(">>>>> run_empty_local_repo_test_async running test");
    let result = match test(repo).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    // util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

pub async fn run_one_commit_local_repo_test_async<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Fut,
    Fut: Future<Output = Result<(), OxenError>>,
{
    init_test_env();
    log::debug!("run_empty_local_repo_test_async start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    let txt = generate_random_string(20);
    let file_path = add_txt_file_to_dir(&repo_dir, &txt)?;
    repositories::add(&repo, &file_path)?;
    repositories::commit(&repo, "Init commit")?;

    let result = match test(repo).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    // util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test syncing between local and remote, where both exist, and both are empty
pub async fn run_one_commit_sync_repo_test<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(&LocalRepository, RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_one_commit_sync_repo_test start");
    let repo_dir = create_repo_dir(test_run_dir())?;

    let mut local_repo = repositories::init(&repo_dir)?;

    let txt = generate_random_string(20);
    let file_path = add_txt_file_to_dir(&repo_dir, &txt)?;
    repositories::add(&local_repo, &file_path)?;
    repositories::commit(&local_repo, "Init commit")?;

    let remote_repo = create_remote_repo(&local_repo).await?;

    // Set remote
    command::config::set_remote(
        &mut local_repo,
        DEFAULT_REMOTE_NAME,
        &remote_repo.remote.url,
    )?;

    // Push
    repositories::push(&local_repo).await?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_one_commit_sync_repo_test running test");
    let result = match test(&local_repo, remote_repo).await {
        Ok(remote_repo) => {
            // Cleanup remote repo
            api::client::repositories::delete(&remote_repo).await?;
            true
        }
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Cleanup local repo
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test syncing between local and remote where local has high n commits and remote is empty
pub async fn run_many_local_commits_empty_sync_remote_test<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository, RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_many_local_commits_empty_sync_remote_test start");
    let repo_dir = create_repo_dir(test_run_dir())?;

    let mut local_repo = repositories::init(&repo_dir)?;
    let remote_repo = create_remote_repo(&local_repo).await?;

    // Set remote
    command::config::set_remote(
        &mut local_repo,
        DEFAULT_REMOTE_NAME,
        &remote_repo.remote.url,
    )?;

    let local_repo_dir = local_repo.path.clone();

    for i in 1..26 {
        // Get random string
        let txt = generate_random_string(20);
        let file_path = add_txt_file_to_dir(&local_repo_dir, &txt)?;
        repositories::add(&local_repo, &file_path)?;
        repositories::commit(&local_repo, &format!("Adding file_{}", i))?;
    }

    // Run test to see if it panic'd
    log::info!(">>>>> run_many_local_commits_empty_sync_remote_test running test");
    let result = match test(local_repo, remote_repo).await {
        Ok(remote_repo) => {
            // Cleanup remote repo
            api::client::repositories::delete(&remote_repo).await?;
            true
        }
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    assert!(result);
    Ok(())
}

/// Test where the local repo has training data in it
pub async fn run_training_data_sync_test_no_commits<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository, RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_training_data_sync_test_no_commits start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let local_repo = repositories::init(&repo_dir)?;

    // Write all the training data files
    populate_dir_with_training_data(&repo_dir)?;

    let remote_repo = create_remote_repo(&local_repo).await?;
    println!("Got remote repo: {remote_repo:?}");

    // Run test to see if it panic'd
    log::info!(">>>>> run_training_data_sync_test_no_commits running test");
    let result = match test(local_repo, remote_repo).await {
        Ok(_remote_repo) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test where we synced training data to the remote
pub async fn run_training_data_fully_sync_remote<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository, RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_training_data_fully_sync_remote start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let mut local_repo = repositories::init(&repo_dir)?;

    // Write all the training data files
    populate_dir_with_training_data(&repo_dir)?;
    // Make a few commits before we sync
    repositories::add(&local_repo, local_repo.path.join("train"))?;
    repositories::commit(&local_repo, "Adding train/")?;

    repositories::add(&local_repo, local_repo.path.join("test"))?;
    repositories::commit(&local_repo, "Adding test/")?;

    repositories::add(&local_repo, local_repo.path.join("annotations"))?;
    repositories::commit(&local_repo, "Adding annotations/")?;

    repositories::add(&local_repo, local_repo.path.join("nlp"))?;
    repositories::commit(&local_repo, "Adding nlp/")?;

    // Remove the test dir to make a more complex history
    let rm_opts = RmOpts {
        path: PathBuf::from("test"),
        recursive: true,
        staged: false,
    };

    repositories::rm(&local_repo, &rm_opts)?;
    repositories::commit(&local_repo, "Removing test/")?;

    // Add all the files
    repositories::add(&local_repo, &local_repo.path)?;
    // Commit all the data locally
    repositories::commit(&local_repo, "Adding rest of data")?;

    // Create remote
    let remote_repo = create_remote_repo(&local_repo).await?;

    // Add remote
    let remote_url = repo_remote_url_from(&local_repo.dirname());
    command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote_url)?;

    // Push data
    repositories::push(&local_repo).await?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_training_data_fully_sync_remote running test");
    let result = match test(local_repo, remote_repo).await {
        Ok(_remote_repo) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

pub async fn run_select_data_sync_remote<T, Fut>(data: &str, test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository, RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_select_data_sync_remote start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let mut local_repo = repositories::init(&repo_dir)?;

    // Write all the training data files
    populate_select_training_data(&repo_dir, data)?;

    // Make a few commits before we sync
    repositories::add(&local_repo, local_repo.path.join(data))?;
    repositories::commit(&local_repo, &format!("Adding {data}"))?;

    // Create remote
    let remote_repo = create_remote_repo(&local_repo).await?;

    // Add remote
    let remote_url = repo_remote_url_from(&local_repo.dirname());
    command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote_url)?;
    // Push data
    repositories::push(&local_repo).await?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_select_data_sync_remote running test");
    let result = match test(local_repo, remote_repo).await {
        Ok(_remote_repo) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test where certain data is synced to the remote
pub async fn run_subset_of_data_fully_sync_remote<T, Fut>(
    data: &str,
    test: T,
) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository, RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_subset_of_data_fully_sync_remote start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let mut local_repo = repositories::init(&repo_dir)?;

    // Write all the training data files
    populate_select_training_data(&repo_dir, data)?;

    // Create remote
    let remote_repo = create_remote_repo(&local_repo).await?;

    // Add remote
    let remote_url = repo_remote_url_from(&local_repo.dirname());
    command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote_url)?;
    // Push data
    repositories::push(&local_repo).await?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_subset_of_data_fully_sync_remote running test");
    let result = match test(local_repo, remote_repo).await {
        Ok(_remote_repo) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test interacting with a remote repo that was created via API, not local repo
pub async fn run_no_commit_remote_repo_test<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_no_commit_remote_repo_test start");
    let name = format!("repo_{}", uuid::Uuid::new_v4());
    let namespace = constants::DEFAULT_NAMESPACE;
    let repo_new = RepoNew::from_namespace_name_host(namespace, name, test_host());
    let repo = api::client::repositories::create_empty(repo_new).await?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_no_commit_remote_repo_test running test");
    let result = match test(repo).await {
        Ok(repo) => {
            // Cleanup remote repo
            api::client::repositories::delete(&repo).await?;
            true
        }
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test interacting with a remote repo that has nothing synced
pub async fn run_empty_remote_repo_test<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository, RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_empty_remote_repo_test start");
    let empty_dir = create_empty_dir(test_run_dir())?;
    let name = format!("repo_{}", uuid::Uuid::new_v4());
    let path = empty_dir.join(name);
    let local_repo = repositories::init(&path)?;
    let remote_repo = create_remote_repo(&local_repo).await?;

    println!("REMOTE REPO: {remote_repo:?}");

    // Run test to see if it panic'd
    log::info!(">>>>> run_empty_remote_repo_test running test");
    let result = match test(local_repo, remote_repo).await {
        Ok(repo) => {
            // Cleanup remote repo
            api::client::repositories::delete(&repo).await?;
            true
        }
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Cleanup Local
    util::fs::remove_dir_all(path)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test interacting with a remote repo that has nothing synced
pub async fn run_readme_remote_repo_test<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository, RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_empty_remote_repo_test start");
    let empty_dir = create_empty_dir(test_run_dir())?;
    let name = format!("repo_{}", uuid::Uuid::new_v4());
    let path = empty_dir.join(name);
    let mut local_repo = repositories::init(&path)?;

    // Add a README file
    util::fs::write_to_path(local_repo.path.join("README.md"), "Hello World")?;
    repositories::add(&local_repo, &local_repo.path)?;
    repositories::commit(&local_repo, "Adding README")?;

    // Set the proper remote
    let remote_repo = create_remote_repo(&local_repo).await?;
    command::config::set_remote(
        &mut local_repo,
        constants::DEFAULT_REMOTE_NAME,
        remote_repo.url(),
    )?;

    // Push
    repositories::push(&local_repo).await?;

    println!("REMOTE REPO: {remote_repo:?}");

    // Run test to see if it panic'd
    log::info!(">>>>> run_empty_remote_repo_test running test");
    let result = match test(local_repo, remote_repo).await {
        Ok(repo) => {
            // Cleanup remote repo
            api::client::repositories::delete(&repo).await?;
            true
        }
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Cleanup Local
    util::fs::remove_dir_all(path)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Test interacting with a remote repo that has has the initial commit pushed
pub async fn run_remote_repo_test_all_data_pushed<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_remote_repo_test_all_data_pushed start");
    let empty_dir = create_empty_dir(test_run_dir())?;
    let name = format!("repo_{}", uuid::Uuid::new_v4());
    let path = empty_dir.join(name);
    let mut local_repo = repositories::init(&path)?;

    // Write all the files
    populate_dir_with_training_data(&local_repo.path)?;
    add_all_data_to_repo(&local_repo)?;
    repositories::commit(&local_repo, "Adding all data")?;

    // Set the proper remote
    let remote = repo_remote_url_from(&local_repo.dirname());
    command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

    // Create remote repo
    let repo = create_remote_repo(&local_repo).await?;

    repositories::push(&local_repo).await?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_remote_repo_test_all_data_pushed running test");
    let result = match test(repo).await {
        Ok(_repo) => {
            // TODO: Cleanup remote repo
            // this was failing
            true
        }
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Cleanup Local
    util::fs::remove_dir_all(path)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Same as run_remote_repo_test_all_data_pushed but with just one file
pub async fn run_remote_repo_test_bounding_box_csv_pushed<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(RemoteRepository) -> Fut,
    Fut: Future<Output = Result<RemoteRepository, OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_remote_repo_test_bounding_box_csv_pushed start");
    let empty_dir = create_empty_dir(test_run_dir())?;
    let name = format!("repo_{}", uuid::Uuid::new_v4());
    let path = empty_dir.join(name);
    let mut local_repo = repositories::init(&path)?;

    // Write all the files
    create_bounding_box_csv(&local_repo.path)?;
    repositories::add(&local_repo, &local_repo.path)?;
    log::debug!("about to commit bounding box csv");
    repositories::commit(&local_repo, "Adding bounding box csv")?;
    log::debug!("successfully committed bounding box csv");

    // Set the proper remote
    let remote = repo_remote_url_from(&local_repo.dirname());
    command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

    // Create remote repo
    let repo = create_remote_repo(&local_repo).await?;

    repositories::push(&local_repo).await?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_remote_repo_test_bounding_box_csv_pushed running test");
    let result = match test(repo).await {
        Ok(_repo) => {
            // TODO: Cleanup remote repo
            // this was failing
            true
        }
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Cleanup Local
    util::fs::remove_dir_all(path)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Run a test on a repo with a bunch of filees
pub async fn run_training_data_repo_test_no_commits_async<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Fut,
    Fut: Future<Output = Result<(), OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_training_data_repo_test_no_commits_async start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    // Write all the files
    populate_dir_with_training_data(&repo_dir)?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_training_data_repo_test_no_commits_async running test");
    let result = match test(repo).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    // util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

pub async fn run_training_data_repo_test_no_commits_async_with_version<T, Fut>(
    version: MinOxenVersion,
    test: T,
) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Fut,
    Fut: Future<Output = Result<(), OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_training_data_repo_test_no_commits_async start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init::init_with_version(&repo_dir, version)?;

    // Write all the files
    populate_dir_with_training_data(&repo_dir)?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_training_data_repo_test_no_commits_async running test");
    let result = match test(repo).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    // util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

pub async fn run_select_data_repo_test_no_commits_async<T, Fut>(
    data: &str,
    test: T,
) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Fut,
    Fut: Future<Output = Result<(), OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_select_data_repo_test_no_commits_async start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    // Write all the files
    populate_select_training_data(&repo_dir, data)?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_select_data_repo_test_no_commits_async running test");
    let result = match test(repo).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

pub async fn run_select_data_repo_test_committed_async<T, Fut>(
    data: &str,
    test: T,
) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Fut,
    Fut: Future<Output = Result<(), OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_select_data_repo_test_committed_async start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    // Write all the files
    populate_select_training_data(&repo_dir, data)?;

    // Add all the files
    repositories::add(&repo, &repo.path)?;
    log::debug!("about to commit whole repo");
    // commit
    repositories::commit(&repo, "Adding all data")?;
    log::debug!("committed whole repo");

    // Run test to see if it panic'd
    log::info!(">>>>> run_select_data_repo_test_committed_async running test");
    let result = match test(repo).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

pub async fn run_empty_data_repo_test_no_commits_async<T, Fut>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Fut,
    Fut: Future<Output = Result<(), OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_empty_data_repo_test_no_commits_async start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_empty_data_repo_test_no_commits_async running test");
    let result = match test(repo).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Run a test on a repo with a bunch of files
pub fn run_training_data_repo_test_no_commits<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    log::info!("<<<<< run_training_data_repo_test_no_commits start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    // Write all the files
    populate_dir_with_training_data(&repo_dir)?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_training_data_repo_test_no_commits running test");
    let result = std::panic::catch_unwind(|| match test(repo) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

pub fn run_select_data_repo_test_no_commits<T>(data: &str, test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    log::info!("<<<<< run_select_data_repo_test_no_commits start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    // Write the select files
    populate_select_training_data(&repo_dir, data)?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_select_data_repo_test_no_commits running test");
    let result = std::panic::catch_unwind(|| match test(repo) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

/// Run a test on a repo with a bunch of files
pub async fn run_training_data_repo_test_fully_committed_async<T, Fut>(
    test: T,
) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Fut,
    Fut: Future<Output = Result<(), OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_training_data_repo_test_fully_committed_async start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    // Write all the files
    populate_dir_with_training_data(&repo_dir)?;
    // Add all the files
    repositories::add(&repo, &repo.path)?;
    log::debug!("about to commit this repo");
    repositories::commit(&repo, "adding all data baby")?;
    log::debug!("successfully committed the repo");
    // Run test to see if it panic'd
    log::info!(">>>>> run_training_data_repo_test_fully_committed_async running test");
    let result = match test(repo).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Run a test on a repo with a bunch of files
pub async fn run_training_data_repo_test_fully_committed_async_min_version<T, Fut>(
    version: MinOxenVersion,
    test: T,
) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Fut,
    Fut: Future<Output = Result<(), OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_training_data_repo_test_fully_committed_async_min_version start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init::init_with_version(&repo_dir, version)?;

    // Write all the files
    populate_dir_with_training_data(&repo_dir)?;
    // Add all the files
    repositories::add(&repo, &repo.path)?;
    log::debug!("about to commit this repo");
    repositories::commit(&repo, "adding all data baby")?;
    log::debug!("successfully committed the repo");
    // Run test to see if it panic'd
    log::info!(">>>>> run_training_data_repo_test_fully_committed_async running test");
    let result = match test(repo).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

fn create_bounding_box_csv(repo_path: &Path) -> Result<(), OxenError> {
    let dir = repo_path.join("annotations").join("train");
    // Create dir
    util::fs::create_dir_all(&dir)?;

    // Write all the files
    write_txt_file_to_path(
        dir.join("bounding_box.csv"),
        r"file,label,min_x,min_y,width,height
train/dog_1.jpg,dog,101.5,32.0,385,330
train/dog_1.jpg,dog,102.5,31.0,386,330
train/dog_2.jpg,dog,7.0,29.5,246,247
train/dog_3.jpg,dog,19.0,63.5,376,421
train/cat_1.jpg,cat,57.0,35.5,304,427
train/cat_2.jpg,cat,30.5,44.0,333,396
",
    )?;

    Ok(())
}

pub async fn run_bounding_box_csv_repo_test_fully_committed_async<T, Fut>(
    test: T,
) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Fut,
    Fut: Future<Output = Result<(), OxenError>>,
{
    init_test_env();
    log::info!("<<<<< run_bounding_box_csv_repo_test_fully_committed_async start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    // Write all the files
    create_bounding_box_csv(&repo.path)?;
    // Add all the files
    repositories::add(&repo, &repo.path)?;
    repositories::commit(&repo, "adding all data baby")?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_bounding_box_csv_repo_test_fully_committed_async running test");
    let result = match test(repo).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

/// Run a test on a repo with just a nested annotations/train/bounding_box.csv file
pub fn run_bounding_box_csv_repo_test_fully_committed<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    log::info!("<<<<< run_bounding_box_csv_repo_test_fully_committed start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    // Add all the files
    create_bounding_box_csv(&repo.path)?;
    repositories::add(&repo, &repo.path)?;
    repositories::commit(&repo, "adding all data baby")?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_bounding_box_csv_repo_test_fully_committed running test");
    let result = match test(repo) {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result);
    Ok(())
}

pub fn run_compare_data_repo_test_fully_committed<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    log::info!("<<<<< run_compare_data_repo_test_fully_committed start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    // Has 6 match observations in both keys, 5 diffs,
    // 2 key sets left only, 1 keyset right only.
    write_txt_file_to_path(
        repo.path.join("compare_left.csv"),
        r"height,weight,gender,target,other_target
57,150,M,1,yes
57,160,M,0,yes
58,160,M,1,no
59,170,F,1,no
60,170,F,0,yes
61,180,F,0,yes
62,180,F,0,no
63,190,M,1,no
64,190,M,0,yes
65,200,M,0,no
70,240,M,1,yes
71,241,F,1,no
71,242,F,1,no",
    )?;

    write_txt_file_to_path(
        repo.path.join("compare_right.csv"),
        r"height,weight,gender,target,other_target
57,150,M,1,yes
57,160,M,0,yes
58,160,M,1,no
59,170,F,1,no
60,170,F,0,yes
61,180,F,0,yes
62,180,F,1,no
63,190,M,0,no
64,190,M,1,yes
65,200,M,0,yes
70,240,M,0,no
71,241,M,1,no",
    )?;

    repositories::add(&repo, &repo.path)?;
    repositories::commit(&repo, "adding both csvs for compare")?;

    log::info!(">>>>> run_compare_data_repo_test_fully_committed running test");
    let result = match test(repo) {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Error running test. Err: {err}");
            false
        }
    };

    util::fs::remove_dir_all(&repo_dir)?;

    assert!(result);
    Ok(())
}

/// Run a test on a repo with a bunch of files
pub fn run_training_data_repo_test_fully_committed<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    log::info!("<<<<< run_training_data_repo_test_fully_committed start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;
    // Write all the files
    populate_dir_with_training_data(&repo_dir)?;

    // Add all the files
    repositories::add(&repo, &repo.path)?;

    // Get the status and print it
    let status = repositories::status(&repo)?;
    println!("setup status: {status:?}");
    status.print();

    // Commit the data
    let commit = repositories::commit(&repo, "adding all data baby")?;

    // Read the tree for debug
    let tree = repositories::tree::get_by_commit(&repo, &commit)?;
    println!("setup tree after commit:");
    tree.print();

    // Run test to see if it panic'd
    log::info!(">>>>> run_training_data_repo_test_fully_committed running test");
    let result = std::panic::catch_unwind(|| match test(repo) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    // util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    match result {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {:?}", err);
        }
    }
    Ok(())
}

/// Run a test on a repo with a bunch of files
pub fn run_training_data_repo_test_fully_committed_w_version<T>(
    version: MinOxenVersion,
    test: T,
) -> Result<(), OxenError>
where
    T: FnOnce(LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    log::info!("<<<<< run_training_data_repo_test_fully_committed start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init::init_with_version(&repo_dir, version)?;
    // Write all the files
    populate_dir_with_training_data(&repo_dir)?;

    // Add all the files
    repositories::add(&repo, &repo.path)?;

    // Get the status and print it
    let status = repositories::status(&repo)?;
    println!("setup status: {status:?}");
    status.print();

    // Commit the data
    repositories::commit(&repo, "adding all data baby")?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_training_data_repo_test_fully_committed running test");
    let result = std::panic::catch_unwind(|| match test(repo) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    // util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

fn add_all_data_to_repo(repo: &LocalRepository) -> Result<(), OxenError> {
    repositories::add(repo, repo.path.join("train"))?;
    repositories::add(repo, repo.path.join("test"))?;
    repositories::add(repo, repo.path.join("annotations"))?;
    repositories::add(repo, repo.path.join("large_files"))?;
    repositories::add(repo, repo.path.join("nlp"))?;
    repositories::add(repo, repo.path.join("labels.txt"))?;
    repositories::add(repo, repo.path.join("README.md"))?;

    Ok(())
}

pub fn run_referencer_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(RefWriter) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    init_test_env();
    log::info!("<<<<< run_referencer_test start");
    let repo_dir = create_repo_dir(test_run_dir())?;
    let repo = repositories::init(&repo_dir)?;

    // add and commit a file
    let new_file = repo.path.join("new_file.txt");
    util::fs::write(&new_file, "I am a new file")?;
    repositories::add(&repo, new_file)?;
    repositories::commit(&repo, "Added a new file")?;

    let referencer = RefWriter::new(&repo)?;

    // Run test to see if it panic'd
    log::info!(">>>>> run_referencer_test running test");
    let result = std::panic::catch_unwind(|| match test(referencer) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    util::fs::remove_dir_all(&repo_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());
    Ok(())
}

pub fn user_cfg_file() -> PathBuf {
    Path::new("data")
        .join("test")
        .join("config")
        .join("user_config.toml")
}

pub fn auth_cfg_file() -> PathBuf {
    Path::new("data")
        .join("test")
        .join("config")
        .join("auth_config.toml")
}

pub fn repo_cfg_file() -> PathBuf {
    Path::new("data")
        .join("test")
        .join("config")
        .join("repo_config.toml")
}

pub fn test_img_file() -> PathBuf {
    Path::new("data")
        .join("test")
        .join("images")
        .join("dwight_vince.jpeg")
}

pub fn test_csv_file_with_name(name: &str) -> PathBuf {
    PathBuf::from("data").join("test").join("csvs").join(name)
}

pub fn test_img_file_with_name(name: &str) -> PathBuf {
    PathBuf::from("data").join("test").join("images").join(name)
}

pub fn test_text_file_with_name(name: &str) -> PathBuf {
    PathBuf::from("data").join("test").join("text").join(name)
}

pub fn test_video_file_with_name(name: &str) -> PathBuf {
    PathBuf::from("data").join("test").join("video").join(name)
}

pub fn test_audio_file_with_name(name: &str) -> PathBuf {
    PathBuf::from("data").join("test").join("audio").join(name)
}

/// Returns: data/test/text/celeb_a_200k.csv
pub fn test_200k_csv() -> PathBuf {
    Path::new("data")
        .join("test")
        .join("text")
        .join("celeb_a_200k.csv")
}

/// Returns: data/test/parquet/wiki_1k.parquet
pub fn test_1k_parquet() -> PathBuf {
    Path::new("data")
        .join("test")
        .join("parquet")
        .join("wiki_1k.parquet")
}

/// Returns: data/test/nlp/classification/annotations/test.tsv
pub fn test_nlp_classification_csv() -> PathBuf {
    Path::new("nlp")
        .join("classification")
        .join("annotations")
        .join("test.tsv")
}

pub fn populate_readme(repo_dir: &Path) -> Result<(), OxenError> {
    write_txt_file_to_path(
        repo_dir.join("README.md"),
        r"
        # Welcome to the party

        If you are seeing this, you are deep in the test framework, love to see it, keep testing.

        Yes I am biased, dog is label 0, cat is label 1, not alphabetical. Interpret that as you will.

        🐂 💨
    ",
    )?;

    Ok(())
}

pub fn populate_labels(repo_dir: &Path) -> Result<(), OxenError> {
    write_txt_file_to_path(
        repo_dir.join("labels.txt"),
        r"
        dog
        cat
    ",
    )?;

    Ok(())
}

pub fn populate_large_files(repo_dir: &Path) -> Result<(), OxenError> {
    let large_dir = repo_dir.join("large_files");
    util::fs::create_dir_all(&large_dir)?;
    let large_file_1 = large_dir.join("test.csv");
    let from_file = test_200k_csv();
    util::fs::copy(from_file, large_file_1)?;

    Ok(())
}

pub fn populate_train_dir(repo_dir: &Path) -> Result<(), OxenError> {
    let train_dir = repo_dir.join("train");
    util::fs::create_dir_all(&train_dir)?;
    util::fs::copy(
        Path::new("data")
            .join("test")
            .join("images")
            .join("dog_1.jpg"),
        train_dir.join("dog_1.jpg"),
    )?;
    util::fs::copy(
        Path::new("data")
            .join("test")
            .join("images")
            .join("dog_2.jpg"),
        train_dir.join("dog_2.jpg"),
    )?;
    util::fs::copy(
        Path::new("data")
            .join("test")
            .join("images")
            .join("dog_3.jpg"),
        train_dir.join("dog_3.jpg"),
    )?;
    util::fs::copy(
        Path::new("data")
            .join("test")
            .join("images")
            .join("cat_1.jpg"),
        train_dir.join("cat_1.jpg"),
    )?;
    util::fs::copy(
        Path::new("data")
            .join("test")
            .join("images")
            .join("cat_2.jpg"),
        train_dir.join("cat_2.jpg"),
    )?;

    Ok(())
}

pub fn populate_test_dir(repo_dir: &Path) -> Result<(), OxenError> {
    let test_dir = repo_dir.join("test");
    util::fs::create_dir_all(&test_dir)?;
    util::fs::copy(
        Path::new("data")
            .join("test")
            .join("images")
            .join("dog_4.jpg"),
        test_dir.join("1.jpg"),
    )?;
    util::fs::copy(
        Path::new("data")
            .join("test")
            .join("images")
            .join("cat_3.jpg"),
        test_dir.join("2.jpg"),
    )?;

    Ok(())
}

pub fn populate_annotations_dir(repo_dir: &Path) -> Result<(), OxenError> {
    let annotations_dir = repo_dir.join("annotations");
    util::fs::create_dir_all(&annotations_dir)?;
    let annotations_readme_file = annotations_dir.join("README.md");
    write_txt_file_to_path(
        annotations_readme_file,
        r"
        # Annotations
        Some info about our annotations structure....
        ",
    )?;

    // annotations/train/
    let train_annotations_dir = annotations_dir.join("train");
    util::fs::create_dir_all(&train_annotations_dir)?;
    write_txt_file_to_path(
        train_annotations_dir.join("annotations.txt"),
        r"
train/dog_1.jpg 0
train/dog_2.jpg 0
train/dog_3.jpg 0
train/cat_1.jpg 1
train/cat_2.jpg 1
    ",
    )?;

    create_bounding_box_csv(repo_dir)?;

    write_txt_file_to_path(
        train_annotations_dir.join("one_shot.csv"),
        r"file,label,min_x,min_y,width,height
train/dog_1.jpg,dog,101.5,32.0,385,330
",
    )?;

    write_txt_file_to_path(
        train_annotations_dir.join("two_shot.csv"),
        r"file,label,min_x,min_y,width,height
train/dog_3.jpg,dog,19.0,63.5,376,421
train/cat_1.jpg,cat,57.0,35.5,304,427
",
    )?;

    // annotations/test/
    let test_annotations_dir = annotations_dir.join("test");
    util::fs::create_dir_all(&test_annotations_dir)?;
    write_txt_file_to_path(
        test_annotations_dir.join("annotations.csv"),
        r"file,label,min_x,min_y,width,height
test/dog_3.jpg,dog,19.0,63.5,376,421
test/cat_1.jpg,cat,57.0,35.5,304,427
test/unknown.jpg,unknown,0.0,0.0,0,0
",
    )?;

    Ok(())
}

pub fn populate_nlp_dir(repo_dir: &Path) -> Result<(), OxenError> {
    // Make sure to add a few duplicate examples for testing
    let nlp_annotations_dir = repo_dir
        .join("nlp")
        .join("classification")
        .join("annotations");
    util::fs::create_dir_all(&nlp_annotations_dir)?;
    let train_data = [
        "text\tlabel",
        "My tummy hurts\tnegative",
        "I have a headache\tnegative",
        "My tummy hurts\tnegative",
        "I have a headache\tnegative",
        "loving the sunshine\tpositive",
        "And another unique one\tpositive",
        "My tummy hurts\tnegative",
        "loving the sunshine\tpositive",
        "I am a lonely example\tnegative",
        "I am adding more examples\tpositive",
        "One more time\tpositive",
    ]
    .join("\n");
    write_txt_file_to_path(nlp_annotations_dir.join("train.tsv"), &train_data)?;

    let test_data = [
        "text\tlabel",
        "My tummy hurts\tnegative",
        "My tummy hurts\tnegative",
        "My tummy hurts\tnegative",
        "I have a headache\tnegative",
        "I have a headache\tnegative",
        "loving the sunshine\tpositive",
        "loving the sunshine\tpositive",
        "I am a lonely example\tnegative",
        "I am a great testing example\tpositive",
    ]
    .join("\n");
    write_txt_file_to_path(nlp_annotations_dir.join("test.tsv"), &test_data)?;
    Ok(())
}

pub fn populate_dir_with_training_data(repo_dir: &Path) -> Result<(), OxenError> {
    // Directory Structure
    // Features:
    //   - has multiple content types (jpg, txt, md)
    //   - has a few large data files that we have to chunk and transfer
    //   - has multiple directory levels (annotations/train/one_shot.txt)
    //   - has files at top level (README.md)
    //   - has files without extensions (LICENSE)
    //   - has files/dirs at different levels with same names (annotations.txt)
    //
    // nlp/
    //   classification/
    //     annotations/
    //       train.tsv
    //       test.tsv
    //
    // train/
    //   dog_1.jpg
    //   dog_2.jpg
    //   dog_3.jpg
    //   cat_1.jpg
    //   cat_2.jpg
    // test/
    //   1.jpg
    //   2.jpg
    // annotations/
    //   README.md
    //   train/
    //     bounding_box.csv
    //     one_shot.csv
    //     two_shot.csv
    //     annotations.txt
    //   test/
    //     annotations.txt
    // labels.txt
    // LICENSE
    // README.md

    // README.md
    populate_readme(repo_dir)?;

    // labels.txt
    populate_labels(repo_dir)?;

    // large_files/test.csv
    populate_large_files(repo_dir)?;

    // train/
    populate_train_dir(repo_dir)?;

    // test/
    populate_test_dir(repo_dir)?;

    // annotations/
    populate_annotations_dir(repo_dir)?;

    // nlp/classification/annotations/
    populate_nlp_dir(repo_dir)?;

    Ok(())
}

pub fn populate_select_training_data(repo_dir: &Path, data: &str) -> Result<(), OxenError> {
    // README.md
    if data.contains("README") {
        populate_readme(repo_dir)?;
    }

    // labels.txt
    if data.contains("labels") {
        populate_labels(repo_dir)?;
    }

    // large_files/test.csv
    if data.contains("large_files") {
        populate_large_files(repo_dir)?;
    }

    // train/
    if data.contains("train") {
        populate_train_dir(repo_dir)?;
    }

    // test/
    if data.contains("test") {
        populate_test_dir(repo_dir)?;
    }

    // annotations/
    if data.contains("annotations") {
        populate_annotations_dir(repo_dir)?;
    }

    // nlp/classification/annotations/
    if data.contains("nlp") {
        populate_nlp_dir(repo_dir)?;
    }

    Ok(())
}

pub fn add_file_to_dir(dir: &Path, contents: &str, extension: &str) -> Result<PathBuf, OxenError> {
    // Generate random name, because tests run in parallel, then return that name
    let file_path = PathBuf::from(format!("{}.{extension}", uuid::Uuid::new_v4()));
    let full_path = dir.join(file_path);
    // println!("add_txt_file_to_dir: {:?} to {:?}", file_path, full_path);

    let mut file = File::create(&full_path)?;
    file.write_all(contents.as_bytes())?;

    Ok(full_path)
}

pub fn add_txt_file_to_dir(dir: &Path, contents: &str) -> Result<PathBuf, OxenError> {
    add_file_to_dir(dir, contents, "txt")
}

pub fn add_csv_file_to_dir(dir: &Path, contents: &str) -> Result<PathBuf, OxenError> {
    add_file_to_dir(dir, contents, "csv")
}

pub fn write_txt_file_to_path(
    path: impl AsRef<Path>,
    contents: impl AsRef<str>,
) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();
    let contents = contents.as_ref();
    let mut file = File::create(path)?;
    file.write_all(contents.as_bytes())?;
    Ok(path.to_path_buf())
}

pub fn append_line_txt_file<P: AsRef<Path>>(path: P, line: &str) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();

    let mut file = OpenOptions::new().append(true).open(path)?;

    if let Err(e) = writeln!(file, "{line}") {
        return Err(OxenError::basic_str(format!("Couldn't write to file: {e}")));
    }

    Ok(path.to_path_buf())
}

pub fn modify_txt_file<P: AsRef<Path>>(path: P, contents: &str) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();

    // Overwrite
    if path.exists() {
        util::fs::remove_file(path)?;
    }

    let path = write_txt_file_to_path(path, contents)?;
    Ok(path)
}

pub fn schema_bounding_box() -> Schema {
    let fields = vec![
        Field::new("file", "str"),
        Field::new("min_x", "f32"),
        Field::new("min_y", "f32"),
        Field::new("width", "f32"),
        Field::new("height", "f32"),
    ];
    Schema::new(fields)
}

pub fn add_random_bbox_to_file<P: AsRef<Path>>(path: P) -> Result<PathBuf, OxenError> {
    let mut rng = rand::thread_rng();
    let file_name = format!("random_img_{}.jpg", rng.gen_range(0..10));
    let x: f64 = rng.gen_range(0.0..1000.0);
    let y: f64 = rng.gen_range(0.0..1000.0);
    let w: i64 = rng.gen_range(0..1000);
    let h: i64 = rng.gen_range(0..1000);
    let line = format!("{file_name},{x:2},{y:2},{w},{h}");
    append_line_txt_file(path, &line)
}

pub fn add_img_file_to_dir(dir: &Path, file_path: &Path) -> Result<PathBuf, OxenError> {
    if let Some(ext) = file_path.extension() {
        // Generate random name with same extension, because tests run in parallel, then return that name
        let new_path = PathBuf::from(format!(
            "{}.{}",
            uuid::Uuid::new_v4(),
            ext.to_str().unwrap()
        ));
        let full_new_path = dir.join(new_path);

        // println!("COPY FILE FROM {:?} => {:?}", file_path, full_new_path);
        util::fs::copy(file_path, &full_new_path)?;
        Ok(full_new_path)
    } else {
        let err = format!("Unknown extension file: {file_path:?}");
        Err(OxenError::basic_str(err))
    }
}
