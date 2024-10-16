use crate::app_data::OxenAppData;
use crate::helpers;
use crate::queues::{InMemoryTaskQueue, RedisTaskQueue, TaskQueue};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;
use liboxen::util;

use serde::Serialize;
use std::borrow::Cow;
use std::path::{Path, PathBuf};

pub fn init_test_env() {
    util::logging::init_logging();

    unsafe {
        std::env::set_var("TEST", "true");
    }

    init_queue();
}

pub fn init_queue() -> TaskQueue {
    match helpers::get_redis_connection() {
        Ok(pool) => {
            println!("connecting to redis established, initializing queue");
            TaskQueue::Redis(RedisTaskQueue { pool })
        }
        Err(_) => {
            println!("Failed to connect to Redis. Falling back to in-memory queue.");
            TaskQueue::InMemory(InMemoryTaskQueue::new())
        }
    }
}

pub fn get_sync_dir() -> Result<PathBuf, OxenError> {
    init_test_env();
    let sync_dir = PathBuf::from(format!("data/test/runs/{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&sync_dir)?;
    Ok(sync_dir)
}
pub fn create_local_repo(
    sync_dir: &Path,
    namespace: &str,
    name: &str,
) -> Result<LocalRepository, OxenError> {
    let repo_dir = sync_dir.join(namespace).join(name);
    std::fs::create_dir_all(&repo_dir)?;
    let repo = repositories::init(&repo_dir)?;
    Ok(repo)
}

pub fn run_empty_sync_dir_test<T>(test: T) -> Result<(), OxenError>
where
    T: FnOnce(&Path) -> Result<(), OxenError> + std::panic::UnwindSafe,
{
    let sync_dir = get_sync_dir()?;

    // Run test to see if it panic'd
    let result = std::panic::catch_unwind(|| match test(&sync_dir) {
        Ok(_) => {}
        Err(err) => {
            panic!("Error running test. Err: {}", err);
        }
    });

    // Remove repo dir
    util::fs::remove_dir_all(&sync_dir)?;

    // Assert everything okay after we cleanup the repo dir
    assert!(result.is_ok());

    Ok(())
}

pub fn request(sync_dir: &Path, queue: TaskQueue, uri: &str) -> actix_web::HttpRequest {
    actix_web::test::TestRequest::with_uri(uri)
        .app_data(OxenAppData::new(sync_dir.to_path_buf(), queue))
        .to_http_request()
}

pub fn namespace_request(
    sync_dir: &Path,
    queue: TaskQueue,
    uri: &str,
    repo_namespace: impl Into<Cow<'static, str>>,
) -> actix_web::HttpRequest {
    actix_web::test::TestRequest::with_uri(uri)
        .app_data(OxenAppData::new(sync_dir.to_path_buf(), queue))
        .param("namespace", repo_namespace)
        .to_http_request()
}

pub fn repo_request(
    sync_dir: &Path,
    queue: TaskQueue,
    uri: &str,
    repo_namespace: impl Into<Cow<'static, str>>,
    repo_name: impl Into<Cow<'static, str>>,
) -> actix_web::HttpRequest {
    actix_web::test::TestRequest::with_uri(uri)
        .app_data(OxenAppData::new(sync_dir.to_path_buf(), queue))
        .param("namespace", repo_namespace)
        .param("repo_name", repo_name)
        .to_http_request()
}

pub fn repo_request_with_param(
    sync_dir: &Path,
    queue: TaskQueue,
    uri: &str,
    repo_namespace: impl Into<Cow<'static, str>>,
    repo_name: impl Into<Cow<'static, str>>,
    key: impl Into<Cow<'static, str>>,
    val: impl Into<Cow<'static, str>>,
) -> actix_web::HttpRequest {
    actix_web::test::TestRequest::with_uri(uri)
        .app_data(OxenAppData::new(sync_dir.to_path_buf(), queue))
        .param("namespace", repo_namespace)
        .param("repo_name", repo_name)
        .param(key, val)
        .to_http_request()
}

pub fn request_with_param(
    sync_dir: &Path,
    queue: TaskQueue,
    uri: &str,
    key: impl Into<Cow<'static, str>>,
    val: impl Into<Cow<'static, str>>,
) -> actix_web::HttpRequest {
    actix_web::test::TestRequest::with_uri(uri)
        .app_data(OxenAppData::new(sync_dir.to_path_buf(), queue))
        .param(key, val)
        .to_http_request()
}

pub fn request_with_json(
    sync_dir: &Path,
    queue: TaskQueue,
    uri: &str,
    data: impl Serialize,
) -> actix_web::HttpRequest {
    actix_web::test::TestRequest::with_uri(uri)
        .app_data(OxenAppData::new(sync_dir.to_path_buf(), queue))
        .set_json(data)
        .to_http_request()
}

pub fn request_with_payload_and_entry(
    sync_dir: &Path,
    queue: TaskQueue,
    uri: &str,
    filename: impl Into<Cow<'static, str>>,
    hash: impl Into<Cow<'static, str>>,
    data: impl Into<actix_web::web::Bytes>,
) -> (actix_web::HttpRequest, actix_web::dev::Payload) {
    actix_web::test::TestRequest::with_uri(uri)
        .app_data(OxenAppData::new(sync_dir.to_path_buf(), queue))
        .param("filename", filename)
        .param("hash", hash)
        .set_payload(data)
        .to_http_parts()
}
