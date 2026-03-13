use std::path::Path;

// use liboxen::constants::DEFAULT_REDIS_URL;
use actix_web::http::header;
use actix_web::{HttpResponse, HttpResponseBuilder};
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, RepoNew, User};
use liboxen::repositories;

use crate::errors::OxenHttpError;

pub fn get_repo(
    path: &Path,
    namespace: &str,
    name: &str,
) -> Result<LocalRepository, OxenHttpError> {
    let repo = repositories::get_by_namespace_and_name(path, namespace, name)?;
    let Some(repo) = repo else {
        return Err(
            OxenError::repo_not_found(RepoNew::from_namespace_name(namespace, name, None)).into(),
        );
    };

    Ok(repo)
}

// Helper function for user creation
pub fn create_user_from_options(
    name: Option<String>,
    email: Option<String>,
) -> actix_web::Result<User, OxenHttpError> {
    Ok(User {
        name: name.ok_or(OxenHttpError::BadRequest("Name is required".into()))?,
        email: email.ok_or(OxenHttpError::BadRequest("Email is required".into()))?,
    })
}

pub fn expose_response_header(builder: &mut HttpResponseBuilder, header_name: header::HeaderName) {
    builder.append_header((header::ACCESS_CONTROL_EXPOSE_HEADERS, header_name.as_str()));
}

pub fn expose_content_length(builder: &mut HttpResponseBuilder) {
    expose_response_header(builder, header::CONTENT_LENGTH);
}

pub fn file_stream_response(
    mime_type: &str,
    last_commit_id: &str,
    content_length: Option<u64>,
) -> HttpResponseBuilder {
    let mut response = HttpResponse::Ok();
    response.content_type(mime_type);
    if let Some(content_length) = content_length {
        response.no_chunking(content_length);
    }
    response.insert_header(("oxen-revision-id", last_commit_id));
    expose_content_length(&mut response);
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expose_content_length_sets_header() {
        let mut builder = HttpResponse::Ok();
        expose_content_length(&mut builder);

        let response = builder.finish();
        let value = response
            .headers()
            .get(header::ACCESS_CONTROL_EXPOSE_HEADERS)
            .unwrap()
            .to_str()
            .unwrap();

        assert_eq!(value, header::CONTENT_LENGTH.as_str());
    }

    #[test]
    fn test_expose_content_length_appends_to_existing_headers() {
        let mut builder = HttpResponse::Ok();
        builder.insert_header((header::ACCESS_CONTROL_EXPOSE_HEADERS, "X-Existing-Header"));
        expose_content_length(&mut builder);

        let response = builder.finish();
        let values: Vec<&str> = response
            .headers()
            .get_all(header::ACCESS_CONTROL_EXPOSE_HEADERS)
            .into_iter()
            .map(|value| value.to_str().unwrap())
            .collect();

        assert_eq!(
            values,
            vec!["X-Existing-Header", header::CONTENT_LENGTH.as_str()]
        );
    }
}

// #[allow(dependency_on_unit_never_type_fallback)]
// pub fn get_redis_connection() -> Result<r2d2::Pool<redis::Client>, OxenError> {
//     let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_string());
//     let redis_client = redis::Client::open(redis_url)?;

//     // First, ping redis to see if available - builder retries infinitely and spews error messages
//     let mut test_conn = redis_client.get_connection()?;
//     let _: () = redis::cmd("ECHO").arg("test").query(&mut test_conn)?;

//     // If echo test didn't error, init the builder
//     let pool = r2d2::Pool::builder().build(redis_client)?;
//     Ok(pool)
// }
