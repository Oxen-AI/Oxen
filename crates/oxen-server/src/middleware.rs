use actix_web::{
    Error, HttpMessage, HttpRequest,
    body::MessageBody,
    dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready},
    http::header,
    middleware::Next,
};
use futures_util::future::LocalBoxFuture;
use liboxen::core::repo_locks;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::request_context::REQUEST_ID;
use std::future::{Future, Ready, ready};
use std::panic;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_util::sync::CancellationToken;

use crate::errors::OxenHttpError;
use tracing::Span;
use tracing_actix_web::{DefaultRootSpanBuilder, RootSpanBuilder, root_span};

// Oxen request Id
pub const OXEN_REQUEST_ID: &str = "x-oxen-request-id";

/// Longest inbound request id this server will adopt — room for a UUID several times over.
const MAX_REQUEST_ID_LEN: usize = 128;

/// Whether an inbound request id is one this server will carry as its own.
///
/// Narrower than what a header value may hold, because the id is echoed on the response, written to
/// both access-log lines, and recorded on every span of the request: an unbounded value inflates
/// all three, and a tab or space blurs the access-log format. The accepted shape covers a UUID and
/// a URL-safe base64 id.
fn is_acceptable_request_id(candidate: &str) -> bool {
    !candidate.is_empty()
        && candidate.len() <= MAX_REQUEST_ID_LEN
        && candidate
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

/// The caller's request id, or a freshly generated one when it sent none this server can use.
pub fn extract_or_generate_request_id(headers: &actix_web::http::header::HeaderMap) -> String {
    let Some(header) = headers.get(OXEN_REQUEST_ID) else {
        return generate_request_id();
    };
    if let Ok(inbound) = header.to_str()
        && is_acceptable_request_id(inbound)
    {
        return inbound.to_string();
    }
    // Substituting an id loses correlation with the caller, so leave something to find. At `debug`,
    // and without the value: both are caller-controlled, and the request still succeeds. A header
    // that is not even UTF-8 reports here too, rather than looking like no header at all.
    log::debug!(
        "ignoring malformed {OXEN_REQUEST_ID} header ({} bytes); generating a request id instead",
        header.len()
    );
    generate_request_id()
}

pub fn generate_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// The request id assigned by [`RequestIdMiddleware`], stored in the request's extensions.
struct RequestId(String);

/// Returns the request id [`RequestIdMiddleware`] stored on the request, or `"-"` if none.
pub fn request_id(req: &HttpRequest) -> String {
    req.extensions()
        .get::<RequestId>()
        .map(|id| id.0.clone())
        .unwrap_or_else(|| "-".to_string())
}

/// Canceled when the client goes away before the handler has returned its response. Stored in the
/// request's extensions by [`run_request_as_task`].
struct ClientDeparture(CancellationToken);

/// Runs each request as a task of its own, so a client that drops its connection leaves the request
/// to run to completion, holding its write guards until it finishes, rather than canceling it
/// partway. Wrap it outermost, so every other middleware runs inside the task. Use with
/// [`actix_web::middleware::from_fn`].
pub async fn run_request_as_task<B: MessageBody + 'static>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<B>, Error> {
    let departure = CancellationToken::new();
    req.extensions_mut()
        .insert(ClientDeparture(departure.clone()));
    let request = actix_web::rt::spawn(next.call(req));
    // Fires if the client goes away first, which drops this future before the request finishes.
    let client_waiting = departure.drop_guard();
    // Re-raise a panic inside the request here, on the connection that made it.
    let response = request
        .await
        .unwrap_or_else(|err| panic::resume_unwind(err.into_panic()));
    client_waiting.disarm();
    response
}

/// [`repo_locks::with_repo_exclusive`] for the request `req`. If the client goes away before `work`
/// starts, this gives the repository back to writers at once and returns
/// [`OxenHttpError::ClientDisconnected`], and `work` never runs. Once started, `work` runs to
/// completion whether or not the client stays.
pub(crate) async fn with_repo_exclusive_for_client<T>(
    req: &HttpRequest,
    repo: &LocalRepository,
    work: impl Future<Output = Result<T, OxenError>>,
) -> Result<T, OxenHttpError> {
    let departure = req
        .extensions()
        .get::<ClientDeparture>()
        .map(|departure| departure.0.clone())
        .unwrap_or_default();
    let started = AtomicBool::new(false);
    let exclusive = repo_locks::with_repo_exclusive(repo, async {
        if departure.is_cancelled() {
            return Ok(None);
        }
        started.store(true, Ordering::Relaxed);
        work.await.map(Some)
    });
    tokio::pin!(exclusive);
    tokio::select! {
        biased;
        result = &mut exclusive => return result?.ok_or(OxenHttpError::ClientDisconnected),
        () = departure.cancelled() => {}
    }
    if !started.load(Ordering::Relaxed) {
        return Err(OxenHttpError::ClientDisconnected);
    }
    exclusive.await?.ok_or(OxenHttpError::ClientDisconnected)
}

/// Assigns every request an id (the inbound `x-oxen-request-id` header when the caller sent a
/// usable one, otherwise a fresh uuid) and publishes it to the request extensions, the
/// [`REQUEST_ID`] task-local, the Sentry scope as the `request_id` tag, and the response's own
/// `x-oxen-request-id` header.
///
/// `request_id` is also the tag key OxenHub sets on its own events, making one value enough to
/// find an event in either service.
pub struct RequestIdMiddleware;

impl<S, B> Transform<S, ServiceRequest> for RequestIdMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = RequestIdMiddlewareService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RequestIdMiddlewareService { service }))
    }
}

pub struct RequestIdMiddlewareService<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for RequestIdMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // Extract or generate request ID
        let request_id = extract_or_generate_request_id(req.headers());

        // Store in request extensions for later retrieval if needed
        req.extensions_mut().insert(RequestId(request_id.clone()));

        // Reaches every event the request reports, including from tasks that inherit the hub.
        sentry::configure_scope(|scope| scope.set_tag("request_id", &request_id));

        let fut = self.service.call(req);

        Box::pin(REQUEST_ID.scope(
            std::cell::RefCell::new(Some(request_id.clone())),
            async move {
                let mut res = fut.await?;

                // Add request ID to response headers
                res.headers_mut().insert(
                    actix_web::http::header::HeaderName::from_static(OXEN_REQUEST_ID),
                    actix_web::http::header::HeaderValue::from_str(&request_id).unwrap_or_else(
                        |_| actix_web::http::header::HeaderValue::from_static("invalid"),
                    ),
                );

                Ok(res)
            },
        ))
    }
}

/// Builds the HTTP root span every other span and event of a request hangs under, adding the
/// `oxen.request_id` field to the fields `tracing-actix-web` records by default.
///
/// Two request ids are in play and they are not interchangeable. `request_id` is
/// `tracing-actix-web`'s own: a uuid it mints per request, never leaving this process.
/// `oxen.request_id` is the id [`RequestIdMiddleware`] assigns — taken from the inbound
/// `x-oxen-request-id` header when a caller sent one and echoed back on the response — so it is the
/// one shared with the services on either side of this request, and the one to correlate a trace
/// against a log line or an error report. Registering `TracingLogger` inside `RequestIdMiddleware`
/// is what makes that id available this early.
pub struct OxenRootSpanBuilder;

impl RootSpanBuilder for OxenRootSpanBuilder {
    fn on_request_start(request: &ServiceRequest) -> Span {
        let oxen_request_id = request_id(request.request());
        root_span!(request, oxen.request_id = %oxen_request_id)
    }

    fn on_request_end<B: MessageBody>(span: Span, outcome: &Result<ServiceResponse<B>, Error>) {
        DefaultRootSpanBuilder::on_request_end(span, outcome);
    }
}

/// Logs each request at INFO on entry: remote addr, request line, Referer, User-Agent, request id.
pub struct RequestStartLogMiddleware;

impl<S, B> Transform<S, ServiceRequest> for RequestStartLogMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = RequestStartLogMiddlewareService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RequestStartLogMiddlewareService { service }))
    }
}

pub struct RequestStartLogMiddlewareService<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for RequestStartLogMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = S::Future;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let request_id = request_id(req.request());
        // Mirror the access log's start-known fields (%a "%r" "%{Referer}i" "%{User-Agent}i").
        let remote_addr = req.connection_info().peer_addr().unwrap_or("-").to_string();
        let request_line = if req.query_string().is_empty() {
            format!("{} {} {:?}", req.method(), req.path(), req.version())
        } else {
            format!(
                "{} {}?{} {:?}",
                req.method(),
                req.path(),
                req.query_string(),
                req.version()
            )
        };
        let referer = request_header_or_dash(&req, header::REFERER);
        let user_agent = request_header_or_dash(&req, header::USER_AGENT);
        log::info!(
            "start {remote_addr} \"{request_line}\" \"{referer}\" \"{user_agent}\" req={request_id}"
        );

        self.service.call(req)
    }
}

/// Renders a request header the way the access log does: its UTF-8-lossy value, or "-" if absent.
fn request_header_or_dash(req: &ServiceRequest, name: header::HeaderName) -> String {
    req.headers()
        .get(name)
        .map(|val| String::from_utf8_lossy(val.as_bytes()).into_owned())
        .unwrap_or_else(|| "-".to_string())
}

/// Middleware that records HTTP request count and duration for every route.
///
/// Emits three (3) Prometheus metrics per request:
///   1. `http_requests_total{method, path, status}` — counter
///   2. 'http_errors_total{method, path, status}`   — counter
///   3. `http_request_duration_ms{method, path}`    — histogram (milliseconds)
///
/// The `path` label uses the matched Actix route pattern (e.g.
/// `/api/repos/{namespace}/{repo_name}/branches`) to keep cardinality low.
pub struct MetricsMiddleware;

// These constants are consumed by the `counter!`/`histogram!` macros from `metrics`.
// When the `metrics` feature is disabled, the macros expand to no-ops and the constants
// appear unused to the compiler — but they are still required for compilation with metrics.
#[cfg(feature = "metrics")]
const HTTP_REQUESTS_TOTAL: &str = "http_requests_total";
#[cfg(feature = "metrics")]
const HTTP_ERRORS_TOTAL: &str = "http_errors_total";
#[cfg(feature = "metrics")]
const HTTP_REQUEST_DURATION_MS: &str = "http_request_duration_ms";
#[cfg(feature = "metrics")]
const METHOD: &str = "method";
#[cfg(feature = "metrics")]
const PATH: &str = "path";
#[cfg(feature = "metrics")]
const STATUS: &str = "status";

impl<S, B> Transform<S, ServiceRequest> for MetricsMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = MetricsMiddlewareService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(MetricsMiddlewareService { service }))
    }
}

pub struct MetricsMiddlewareService<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for MetricsMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    #[inline]
    fn call(&self, req: ServiceRequest) -> Self::Future {
        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();
        #[cfg(feature = "metrics")]
        let method = req.method().to_string();

        let fut = self.service.call(req);

        #[cfg(feature = "metrics")]
        {
            Box::pin(async move {
                match fut.await {
                    Ok(res) => {
                        let status = res.status().as_u16().to_string();
                        let path = res
                            .request()
                            .match_pattern()
                            .unwrap_or_else(|| "unmatched".to_string());
                        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

                        metrics::counter!(HTTP_REQUESTS_TOTAL, METHOD => method.clone(), PATH => path.clone(), STATUS => status.clone()).increment(1);
                        if res.status().is_client_error() || res.status().is_server_error() {
                            metrics::counter!(HTTP_ERRORS_TOTAL, METHOD => method.clone(), PATH => path.clone(), STATUS => status)
                                .increment(1);
                        }
                        metrics::histogram!(HTTP_REQUEST_DURATION_MS, METHOD => method, PATH => path)
                            .record(elapsed_ms);

                        Ok(res)
                    }
                    Err(err) => {
                        let status = "500";
                        let path = "unmatched";
                        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

                        metrics::counter!(HTTP_REQUESTS_TOTAL, METHOD => method.clone(), PATH => path, STATUS => status).increment(1);
                        metrics::counter!(HTTP_ERRORS_TOTAL, METHOD => method.clone(), PATH => path, STATUS => status)
                                                .increment(1);
                        metrics::histogram!(HTTP_REQUEST_DURATION_MS, METHOD => method, PATH => path)
                            .record(elapsed_ms);

                        Err(err)
                    }
                }
            })
        }

        #[cfg(not(feature = "metrics"))]
        {
            Box::pin(fut)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use liboxen::request_context::get_request_id;

    #[tokio::test]
    async fn test_request_id_task_local() {
        let request_id = generate_request_id();

        REQUEST_ID
            .scope(
                std::cell::RefCell::new(Some(request_id.clone())),
                async move {
                    assert_eq!(get_request_id(), Some(request_id));
                },
            )
            .await;
    }

    #[tokio::test]
    async fn test_no_request_id() {
        // Outside scope, should return None
        assert_eq!(get_request_id(), None);
    }

    /// Builds a header map carrying `value` as the inbound request id.
    fn headers_with_request_id(value: &str) -> actix_web::http::header::HeaderMap {
        use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(OXEN_REQUEST_ID),
            HeaderValue::from_str(value).expect("the test id should be a valid header value"),
        );
        headers
    }

    /// A UUID, a URL-safe base64 id, and one at the length limit all survive untouched — carrying
    /// the caller's id through is the whole point of honoring the header.
    #[test]
    fn test_extract_request_id_accepts_usable_values() {
        for id in [
            "1b4e28ba-2fa1-11d2-883f-0016d3cca427",
            "aB3-_xYz9Qw2",
            &"a".repeat(MAX_REQUEST_ID_LEN),
        ] {
            assert_eq!(
                extract_or_generate_request_id(&headers_with_request_id(id)),
                id,
                "{id} should be carried through unchanged"
            );
        }
    }

    /// A header value that is not UTF-8 at all takes the same path: `to_str` rejects it before the
    /// shape check runs, and it must still be replaced rather than read as no header at all.
    #[test]
    fn test_extract_request_id_replaces_a_non_utf8_value() {
        use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(OXEN_REQUEST_ID),
            HeaderValue::from_bytes(b"\xff\xfeabc").expect("the bytes should form a header value"),
        );

        let extracted = extract_or_generate_request_id(&headers);
        assert!(
            is_acceptable_request_id(&extracted),
            "a non-UTF-8 id should be replaced with a usable one, got {extracted:?}"
        );
    }

    /// An id that is oversized or carries characters that would blur a log line is replaced rather
    /// than propagated onto every span and log line of the request.
    #[test]
    fn test_extract_request_id_replaces_unusable_values() {
        for id in [
            &"a".repeat(MAX_REQUEST_ID_LEN + 1),
            "has space",
            "has\ttab",
            "has.dot",
            "",
        ] {
            let extracted = extract_or_generate_request_id(&headers_with_request_id(id));
            assert_ne!(extracted, id, "{id:?} should not be carried through");
            assert!(
                is_acceptable_request_id(&extracted),
                "the replacement for {id:?} should itself be usable, got {extracted:?}"
            );
        }
    }

    #[test]
    fn test_extract_request_id_from_header() {
        use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(OXEN_REQUEST_ID),
            HeaderValue::from_static("test-id-123"),
        );

        let id = extract_or_generate_request_id(&headers);
        assert_eq!(id, "test-id-123");
    }

    #[test]
    fn test_generate_request_id_when_missing() {
        use actix_web::http::header::HeaderMap;

        let headers = HeaderMap::new();
        let id = extract_or_generate_request_id(&headers);

        // Should be valid UUID format
        assert_eq!(id.len(), 36); // UUID length with hyphens
    }

    #[actix_web::test]
    async fn test_run_request_as_task_finishes_a_request_its_caller_dropped()
    -> Result<(), OxenError> {
        use actix_web::middleware::from_fn;
        use actix_web::{App, HttpResponse, test, web};
        use liboxen::config::RepositoryConfig;
        use tokio::sync::mpsc;

        liboxen::test::run_empty_dir_test_async(|dir| async move {
            let repo = LocalRepository::new(&dir, RepositoryConfig::default())?;
            let (started_tx, mut started_rx) = mpsc::channel::<()>(1);
            // Carries whether the handler's exclusive work ran, rather than being refused with
            // `ClientDisconnected`.
            let (finished_tx, mut finished_rx) = mpsc::channel::<bool>(1);
            let handler = {
                let repo = repo.clone();
                move |req: HttpRequest| {
                    let (started_tx, finished_tx) = (started_tx.clone(), finished_tx.clone());
                    let repo = repo.clone();
                    async move {
                        started_tx.send(()).await.expect("the test awaits the start");
                        let ran = match with_repo_exclusive_for_client(&req, &repo, async {
                            Ok(())
                        })
                        .await
                        {
                            Ok(()) => true,
                            Err(OxenHttpError::ClientDisconnected) => false,
                            Err(err) => panic!("unexpected error from the exclusive section: {err}"),
                        };
                        finished_tx.send(ran).await.expect("the test awaits the finish");
                        HttpResponse::Ok().finish()
                    }
                }
            };
            let app = test::init_service(
                App::new()
                    .route("/", web::get().to(handler))
                    .wrap(from_fn(run_request_as_task)),
            )
            .await;

            let response =
                test::call_service(&app, test::TestRequest::get().uri("/").to_request()).await;
            assert!(response.status().is_success());
            started_rx.recv().await;
            assert_eq!(
                finished_rx.recv().await,
                Some(true),
                "exclusive work runs while its client waits"
            );

            // A write in flight holds the next request's exclusive section in its drain.
            let write = repo_locks::begin_write(&repo)?;
            let mut call = Box::pin(app.call(test::TestRequest::get().uri("/").to_request()));
            tokio::select! {
                _ = &mut call => panic!("the exclusive section finished while a write was in flight"),
                _ = started_rx.recv() => {}
            }
            // The client goes away mid-drain: nothing polls the call again, and the app is gone
            // too, so the only sender left to report the finish is the one the handler holds.
            drop(call);
            drop(app);

            assert_eq!(
                finished_rx.recv().await,
                Some(false),
                "the request ran to completion after its caller stopped waiting for it, giving up \
                 the exclusive section it had not started"
            );
            assert!(
                repo_locks::begin_write(&repo).is_ok(),
                "a departed client's exclusive section releases the repository without waiting \
                 out the drain"
            );
            drop(write);
            Ok(())
        })
        .await
    }

    #[actix_web::test]
    async fn test_request_start_log_middleware_passes_through() {
        use actix_web::{App, HttpResponse, http::header, test, web};

        let app = test::init_service(App::new().wrap(RequestStartLogMiddleware).route(
            "/x",
            web::get().to(|| async { HttpResponse::Ok().finish() }),
        ))
        .await;

        let req = test::TestRequest::get()
            .uri("/x?page=1")
            .insert_header((header::USER_AGENT, "oxen-test-agent"))
            .insert_header((header::REFERER, "http://example.test/prev"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
    }
}
