use std::path::Path;

use liboxen::api;
use liboxen::constants::DEFAULT_REDIS_URL;
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, RepositoryNew};

use r2d2;

use crate::errors::OxenHttpError;

pub fn get_repo(
    path: &Path,
    namespace: impl AsRef<str>,
    name: impl AsRef<str>,
) -> Result<LocalRepository, OxenHttpError> {
    Ok(
        api::local::repositories::get_by_namespace_and_name(path, &namespace, &name)?.ok_or(
            OxenError::repo_not_found(RepositoryNew::new(&namespace, &name)),
        )?,
    )
}

pub fn get_redis_connection() -> Result<r2d2::Pool<redis::Client>, OxenError> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_string());
    let redis_client = redis::Client::open(redis_url)?;

    // First, ping redis to see if available - builder retries infinitely and spews error messages
    let mut test_conn = redis_client.get_connection()?;
    redis::cmd("ECHO").arg("test").query(&mut test_conn)?;

    // If echo test didn't error, init the builder
    let pool = r2d2::Pool::builder().build(redis_client)?;
    Ok(pool)
}
