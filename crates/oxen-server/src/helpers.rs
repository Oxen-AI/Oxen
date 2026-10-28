use std::future::Future;
use std::io::{self, BufWriter, Write};
use std::panic;
use std::path::PathBuf;
use std::time::Duration;

use actix_web::http::header;
use actix_web::web::Bytes;
use actix_web::{HttpRequest, HttpResponse, HttpResponseBuilder};
use futures_util::stream::{self, Stream, StreamExt as _};
use liboxen::api::requests::RepoNew;
use liboxen::constants::STREAMING_BUF_SIZE;
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, MerkleHash, User};
use liboxen::repositories;
use liboxen::util;
use liboxen::util::hasher::HashingWriter;
use liboxen::view::StatusMessage;
use serde::Serialize;
use tempfile::NamedTempFile;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, Interval, interval_at};
use tokio_util::io::{StreamReader, SyncIoBridge};

use crate::app_data::OxenAppData;
use crate::errors::OxenHttpError;
use crate::tasks;

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
    accept_ranges(&mut response);
    expose_content_length(&mut response);
    response
}

/// What a `Range` header asked for, measured against the file.
///
/// A client that cannot seek downloads a video again from the start every time the viewer drags
/// the scrubber, and `<video preload="metadata">` has no way to ask for the header alone, so it
/// pulls the whole file just to learn the duration.
pub enum ByteRange {
    /// No range asked for, or one in a form we do not serve. Send the file.
    Whole,
    /// Both ends inclusive, as HTTP counts them.
    Partial { start: u64, end: u64 },
    /// Asked for, and entirely past the end.
    Unsatisfiable,
}

/// Reads a `Range` header against a file of `len` bytes.
///
/// Only a single `bytes=` range is served. A client asking for several gets the whole file, which
/// the specification allows and which keeps us from having to write a multipart body.
pub fn parse_byte_range(req: &HttpRequest, len: u64) -> ByteRange {
    let Some(header) = req
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
    else {
        return ByteRange::Whole;
    };
    let Some(spec) = header.trim().strip_prefix("bytes=") else {
        return ByteRange::Whole;
    };
    if spec.contains(',') {
        return ByteRange::Whole;
    }
    let Some((from, to)) = spec.trim().split_once('-') else {
        return ByteRange::Whole;
    };

    // `bytes=-500` means the last 500 bytes, not everything up to byte 500.
    let (start, end) = if from.is_empty() {
        let Ok(suffix) = to.parse::<u64>() else {
            return ByteRange::Whole;
        };
        if suffix == 0 {
            return ByteRange::Unsatisfiable;
        }
        (len.saturating_sub(suffix), len.saturating_sub(1))
    } else {
        let Ok(start) = from.parse::<u64>() else {
            return ByteRange::Whole;
        };
        let end = if to.is_empty() {
            len.saturating_sub(1)
        } else {
            match to.parse::<u64>() {
                // A range that ends before it starts is malformed rather than past the end.
                Ok(end) if end < start => return ByteRange::Whole,
                Ok(end) => end.min(len.saturating_sub(1)),
                Err(_) => return ByteRange::Whole,
            }
        };
        (start, end)
    };

    if len == 0 || start >= len || start > end {
        return ByteRange::Unsatisfiable;
    }
    ByteRange::Partial { start, end }
}

/// Tells the client it may ask for part of a file. Without this a browser will not try.
fn accept_ranges(response: &mut HttpResponseBuilder) {
    response.insert_header((header::ACCEPT_RANGES, "bytes"));
}

/// The `206` answer to a satisfiable range.
pub fn file_range_response(
    mime_type: &str,
    last_commit_id: &str,
    start: u64,
    end: u64,
    len: u64,
) -> HttpResponseBuilder {
    let mut response = HttpResponse::PartialContent();
    response.content_type(mime_type);
    response.no_chunking(end - start + 1);
    response.insert_header(("oxen-revision-id", last_commit_id));
    response.insert_header((header::CONTENT_RANGE, format!("bytes {start}-{end}/{len}")));
    accept_ranges(&mut response);
    expose_content_length(&mut response);
    response
}

/// The `416` answer to a range that starts past the end, which carries the real length so the
/// client can ask again.
pub fn range_not_satisfiable_response(len: u64) -> HttpResponse {
    let mut response = HttpResponse::RangeNotSatisfiable();
    response.insert_header((header::CONTENT_RANGE, format!("bytes */{len}")));
    accept_ranges(&mut response);
    response.finish()
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
///
/// `work` runs to completion even when the client goes away before reading the body.
///
/// Call from the request handler, not from another task — `work` inherits the calling thread's
/// crash-reporting context, and a panic inside it reports under that request.
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
    enum State<T> {
        Running {
            work: JoinHandle<Result<T, OxenError>>,
            ticker: Interval,
        },
        Done,
    }

    let body = stream::unfold(
        State::Running {
            work: actix_web::rt::spawn(tasks::inherit_hub(work)),
            ticker: interval_at(Instant::now() + heartbeat_interval, heartbeat_interval),
        },
        |state| async move {
            let State::Running {
                mut work,
                mut ticker,
            } = state
            else {
                return None;
            };
            // An operation that finishes (or fails) within the first interval returns its result
            // with no heartbeat; otherwise emit a newline each interval and keep waiting.
            tokio::select! {
                biased;
                result = &mut work => {
                    // Re-raise a panic inside the work here, where the body reads it.
                    let result = result.unwrap_or_else(|err| panic::resume_unwind(err.into_panic()));
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

/// A request body written to a temporary file, which is deleted when this drops.
pub(crate) struct SpooledBody {
    pub(crate) file: NamedTempFile,
    /// The size of the file, after any gunzipping.
    pub(crate) len: u64,
    /// The hash of the file's bytes, after any gunzipping.
    pub(crate) hash: MerkleHash,
}

/// How a request body's bytes are encoded.
pub(crate) enum BodyEncoding {
    Plain,
    /// Gzipped, and refused once it inflates past `max_decompressed` bytes.
    Gzip {
        max_decompressed: u64,
    },
}

/// How many chunks of a request body can wait for the disk while it is written to a file. The bound
/// slows a fast client to the speed of the disk rather than keeping the body in memory.
const SPOOL_CHANNEL_CAPACITY: usize = 8;

/// Write `body` to a new temporary file in `dir`, gunzipping it on the way when it is gzipped, and
/// hash what is written. The outer error is a failure to read the body, or a body of more than
/// `max_len` bytes. The inner one is a failure to gunzip the body or to write the file.
pub(crate) async fn spool_to_temp_file<E: Into<actix_web::Error>>(
    mut body: impl Stream<Item = Result<Bytes, E>> + Unpin,
    dir: PathBuf,
    max_len: u64,
    encoding: BodyEncoding,
) -> Result<Result<SpooledBody, OxenError>, OxenHttpError> {
    let (tx, rx) = mpsc::channel::<Bytes>(SPOOL_CHANNEL_CAPACITY);
    let chunks = stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|chunk| (Ok::<_, io::Error>(chunk), rx))
    });
    let mut reader = SyncIoBridge::new(StreamReader::new(Box::pin(chunks)));
    let writer = tasks::spawn_blocking_per_item(move || -> Result<SpooledBody, OxenError> {
        util::fs::create_dir_all(&dir)?;
        let file = NamedTempFile::new_in(&dir)?;
        let mut buffered = BufWriter::with_capacity(STREAMING_BUF_SIZE, file.as_file());
        let mut hashing = HashingWriter::new(&mut buffered);
        let len = match encoding {
            BodyEncoding::Plain => io::copy(&mut reader, &mut hashing)?,
            BodyEncoding::Gzip { max_decompressed } => {
                util::compression::decompress_gzip_capped_into(
                    &mut reader,
                    &mut hashing,
                    max_decompressed,
                )?
            }
        };
        let hash = MerkleHash::new(hashing.digest128());
        buffered.flush()?;
        drop(buffered); // release the borrow of `file` before handing it back
        Ok(SpooledBody { file, len, hash })
    });
    let mut received: u64 = 0;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(Into::<actix_web::Error>::into)?;
        received += chunk.len() as u64;
        if received > max_len {
            return Err(OxenHttpError::BadRequest(
                format!("The upload is larger than the {max_len} bytes allowed").into(),
            ));
        }
        // A send error means the writer ended early (a write or gunzip failed). Stop reading and
        // let the join below surface the underlying error.
        if tx.send(chunk).await.is_err() {
            break;
        }
    }
    drop(tx); // close the channel so the writer reads to the end
    writer
        .await
        .map_err(|e| OxenError::internal_error(format!("spool task panicked: {e}")).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test::TestRequest;

    fn asked_for(header: &str, len: u64) -> ByteRange {
        parse_byte_range(
            &TestRequest::default()
                .insert_header((header::RANGE, header))
                .to_http_request(),
            len,
        )
    }

    fn partial(range: ByteRange) -> (u64, u64) {
        match range {
            ByteRange::Partial { start, end } => (start, end),
            ByteRange::Whole => panic!("expected a partial range, got the whole file"),
            ByteRange::Unsatisfiable => panic!("expected a partial range, got unsatisfiable"),
        }
    }

    #[test]
    fn test_no_range_header_serves_the_whole_file() {
        let req = TestRequest::default().to_http_request();
        assert!(matches!(parse_byte_range(&req, 100), ByteRange::Whole));
    }

    #[test]
    fn test_both_ends_are_inclusive() {
        assert_eq!(partial(asked_for("bytes=0-499", 1000)), (0, 499));
    }

    #[test]
    fn test_an_open_end_runs_to_the_last_byte() {
        assert_eq!(partial(asked_for("bytes=500-", 1000)), (500, 999));
    }

    #[test]
    fn test_a_suffix_counts_back_from_the_end() {
        assert_eq!(partial(asked_for("bytes=-200", 1000)), (800, 999));
    }

    #[test]
    fn test_a_suffix_longer_than_the_file_is_the_file() {
        assert_eq!(partial(asked_for("bytes=-5000", 1000)), (0, 999));
    }

    #[test]
    fn test_an_end_past_the_file_stops_at_the_last_byte() {
        assert_eq!(partial(asked_for("bytes=900-5000", 1000)), (900, 999));
    }

    #[test]
    fn test_a_start_past_the_end_is_unsatisfiable() {
        assert!(matches!(
            asked_for("bytes=1000-", 1000),
            ByteRange::Unsatisfiable
        ));
    }

    #[test]
    fn test_nothing_is_satisfiable_in_an_empty_file() {
        assert!(matches!(asked_for("bytes=0-", 0), ByteRange::Unsatisfiable));
    }

    // Serving several ranges means a multipart body, and the specification lets us decline.
    #[test]
    fn test_several_ranges_get_the_whole_file() {
        assert!(matches!(
            asked_for("bytes=0-99,200-299", 1000),
            ByteRange::Whole
        ));
    }

    #[test]
    fn test_a_unit_we_do_not_serve_gets_the_whole_file() {
        assert!(matches!(asked_for("items=0-99", 1000), ByteRange::Whole));
        assert!(matches!(asked_for("bytes=abc-def", 1000), ByteRange::Whole));
    }

    #[test]
    fn test_a_range_that_ends_before_it_starts_gets_the_whole_file() {
        assert!(matches!(asked_for("bytes=50-10", 1000), ByteRange::Whole));
        assert!(matches!(asked_for("bytes=5000-10", 1000), ByteRange::Whole));
    }

    #[test]
    fn test_a_partial_answer_carries_the_range_and_the_length() {
        let response = file_range_response("video/mp4", "abc123", 100, 199, 1000).finish();
        assert_eq!(response.status().as_u16(), 206);
        let value = response.headers().get(header::CONTENT_RANGE).unwrap();
        assert_eq!(value.to_str().unwrap(), "bytes 100-199/1000");
    }

    #[test]
    fn test_an_unsatisfiable_answer_tells_the_client_the_real_length() {
        let response = range_not_satisfiable_response(1000);
        assert_eq!(response.status().as_u16(), 416);
        let value = response.headers().get(header::CONTENT_RANGE).unwrap();
        assert_eq!(value.to_str().unwrap(), "bytes */1000");
    }

    #[test]
    fn test_a_whole_file_answer_says_ranges_are_available() {
        let response = file_stream_response("video/mp4", "abc123", Some(10)).finish();
        assert_eq!(
            response.headers().get(header::ACCEPT_RANGES).unwrap(),
            "bytes"
        );
    }

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

    // The work behind a body the client never reads still runs to completion.
    #[actix_web::test]
    async fn test_stream_with_heartbeat_finishes_work_whose_body_was_dropped() {
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let work = async move {
            finished_tx.send(()).expect("the test awaits the finish");
            Ok(StatusMessage::success("done"))
        };
        drop(stream_with_heartbeat_every(work, Duration::from_millis(15)));

        assert!(
            finished_rx.await.is_ok(),
            "the work ran to completion after its body was dropped"
        );
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
