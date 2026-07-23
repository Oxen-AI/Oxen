use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use actix_web::http::header;
use actix_web::web::Bytes;
use actix_web::{HttpResponse, HttpResponseBuilder};
use futures_util::stream;
use liboxen::api::requests::RepoNew;
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, User};
use liboxen::repositories;
use liboxen::view::StatusMessage;
use serde::Serialize;
use tokio::time::{Interval, interval};

use crate::app_data::OxenAppData;
use crate::errors::OxenHttpError;

/// Look up a repo by `<namespace>/<name>` under the server's sync dir.
pub fn get_repo(
    app_data: &OxenAppData,
    namespace: impl AsRef<str>,
    name: impl AsRef<str>,
) -> Result<LocalRepository, OxenHttpError> {
    let repo = repositories::get_by_namespace_and_name(
        &app_data.path,
        &namespace,
        &name,
        app_data.config.storage.s3(),
    )?;
    let Some(repo) = repo else {
        return Err(
            OxenError::RepoNotFound(Box::new(RepoNew::from_namespace_name(
                &namespace, &name, None,
            )))
            .into(),
        );
    };

    Ok(repo)
}

/// Look up a repo by `<namespace>/<name>` under the server's sync dir, off the async worker.
pub async fn get_repo_async(
    app_data: &OxenAppData,
    namespace: &str,
    name: &str,
) -> Result<LocalRepository, OxenHttpError> {
    let repo = repositories::get_by_namespace_and_name_async(
        &app_data.path,
        namespace,
        name,
        app_data.config.storage.s3(),
    )
    .await?;
    let Some(repo) = repo else {
        return Err(
            OxenError::RepoNotFound(Box::new(RepoNew::from_namespace_name(
                namespace, name, None,
            )))
            .into(),
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
        name: name.ok_or_else(|| OxenHttpError::BadRequest("Name is required".into()))?,
        email: email.ok_or_else(|| OxenHttpError::BadRequest("Email is required".into()))?,
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

/// Interval between newline keep-alive bytes emitted while a long operation runs.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Stream `work`'s JSON result, emitting a newline keep-alive byte every [`HEARTBEAT_INTERVAL`]
/// until it finishes. The leading newlines keep the idle timers along the connection (proxy, load
/// balancer, client) from firing during the silent stretch while the server processes a request,
/// and every JSON parser skips leading whitespace, so the client reads the final body unchanged.
///
/// The `200 OK` status commits with the first byte, so a failure once the work has started is
/// reported as a `StatusMessage::error` in the body — which the client still reads as a failure via
/// the body's `status` field — rather than as an error status code. `work` must own everything it
/// captures (`'static`); resolve the repo and read the request body before calling this so those
/// failures still return a real error status.
pub fn stream_with_heartbeat<F, T>(work: F) -> HttpResponse
where
    F: Future<Output = Result<T, OxenError>> + 'static,
    T: Serialize + 'static,
{
    stream_with_heartbeat_every(work, HEARTBEAT_INTERVAL)
}

/// [`stream_with_heartbeat`] with an explicit heartbeat interval (tests use a short one).
fn stream_with_heartbeat_every<F, T>(work: F, heartbeat_interval: Duration) -> HttpResponse
where
    F: Future<Output = Result<T, OxenError>> + 'static,
    T: Serialize + 'static,
{
    enum State<F> {
        Running { work: Pin<Box<F>>, ticker: Interval },
        Done,
    }

    let body = stream::unfold(
        State::Running {
            work: Box::pin(work),
            ticker: interval(heartbeat_interval),
        },
        |state| async move {
            let State::Running {
                mut work,
                mut ticker,
            } = state
            else {
                return None;
            };
            // Poll the work first: a fast (or already-failed) operation returns its result on the
            // first step with no heartbeat; otherwise emit a newline and keep waiting.
            tokio::select! {
                biased;
                result = work.as_mut() => {
                    Some((Ok(result_to_json_bytes(result)), State::Done))
                }
                _ = ticker.tick() => {
                    Some((
                        Ok::<Bytes, std::io::Error>(Bytes::from_static(b"\n")),
                        State::Running { work, ticker },
                    ))
                }
            }
        },
    );

    HttpResponse::Ok()
        .content_type("application/json")
        .streaming(body)
}

/// Serialize a heartbeat operation's outcome: the value on success, or a `StatusMessage::error`
/// (which the client reads as a failure) on error.
fn result_to_json_bytes<T: Serialize>(result: Result<T, OxenError>) -> Bytes {
    match result {
        Ok(value) => match serde_json::to_vec(&value) {
            Ok(bytes) => Bytes::from(bytes),
            Err(err) => {
                log::error!("failed to serialize heartbeat response body: {err}");
                error_status_bytes(&format!("failed to serialize response: {err}"))
            }
        },
        Err(err) => {
            log::error!("operation behind heartbeat stream failed: {err}");
            error_status_bytes(&err.to_string())
        }
    }
}

fn error_status_bytes(message: &str) -> Bytes {
    match serde_json::to_vec(&StatusMessage::error(message)) {
        Ok(bytes) => Bytes::from(bytes),
        Err(_) => Bytes::from_static(
            b"{\"status\":\"error\",\"status_message\":\"internal serialization error\"}",
        ),
    }
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
            .map(|value| value.to_str().unwrap())
            .collect();

        assert_eq!(
            values,
            vec!["X-Existing-Header", header::CONTENT_LENGTH.as_str()]
        );
    }

    // A slow operation streams leading newline heartbeats, then its JSON result — and the whole
    // body still deserializes (leading whitespace is skipped), the way the client parses it.
    #[actix_web::test]
    async fn test_stream_with_heartbeat_emits_newlines_then_json() {
        let work = async {
            tokio::time::sleep(Duration::from_millis(60)).await;
            Ok(StatusMessage::success("done"))
        };
        let response = stream_with_heartbeat_every(work, Duration::from_millis(15));
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("collect body");

        assert!(body.starts_with(b"\n"), "expected a leading heartbeat byte");
        let parsed: StatusMessage = serde_json::from_slice(&body).expect("body parses as JSON");
        assert_eq!(parsed.status, "success");
    }

    // A failure once the work has started is reported in the body as an error StatusMessage.
    #[actix_web::test]
    async fn test_stream_with_heartbeat_reports_error_in_body() {
        let work = async { Err::<StatusMessage, _>(OxenError::basic_str("boom")) };
        let response = stream_with_heartbeat_every(work, Duration::from_millis(15));
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("collect body");

        let parsed: StatusMessage = serde_json::from_slice(&body).expect("error body parses");
        assert_eq!(parsed.status, "error");
        assert!(parsed.status_message.contains("boom"));
    }

    // A fast operation finishes before the first tick, so it emits its result with no heartbeat.
    #[actix_web::test]
    async fn test_stream_with_heartbeat_fast_op_emits_no_heartbeat() {
        let work = async { Ok(StatusMessage::success("done")) };
        let response = stream_with_heartbeat_every(work, Duration::from_millis(15));
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("collect body");

        assert!(
            !body.starts_with(b"\n"),
            "a fast operation should emit no heartbeat"
        );
        let parsed: StatusMessage = serde_json::from_slice(&body).expect("body parses as JSON");
        assert_eq!(parsed.status, "success");
    }
}
