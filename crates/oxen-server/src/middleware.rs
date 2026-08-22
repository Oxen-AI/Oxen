use crate::app_data::OxenAppData;
use crate::transitional_identity::{StatedRequest, TransitionalIdentity, with_stated_request};
use actix_web::{
    Error, HttpMessage, HttpRequest,
    body::MessageBody,
    dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready},
    http::{Method, header},
};
use futures_util::future::LocalBoxFuture;
use liboxen::request_context::REQUEST_ID;
use std::future::{Ready, ready};
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

/// Middleware factory for request ID injection
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

/// Refuses a request whose transitional identity headers are present but unusable, and makes the
/// parsed identity available to handlers through [`transitional_identity`].
///
/// Rejecting here rather than where a handler happens to look means a caller that meant to state
/// identity is told it failed, instead of being served as though it had stated nothing.
pub struct TransitionalIdentityMiddleware;

impl<S, B> Transform<S, ServiceRequest> for TransitionalIdentityMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = TransitionalIdentityMiddlewareService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(TransitionalIdentityMiddlewareService { service }))
    }
}

pub struct TransitionalIdentityMiddlewareService<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for TransitionalIdentityMiddlewareService<S>
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
        // These headers are how a control plane above the server states identity. A server that
        // owns its own namespaces is given real names in the URL, so a caller-stated one there is
        // ignored for the same reason a caller-supplied UUID is: identity is not the caller's.
        let states_identity = req.app_data::<OxenAppData>().is_some_and(|data| {
            !data
                .config
                .identity
                .repo_uuids_assigned_by()
                .supplies_names()
        });
        if !states_identity {
            return Box::pin(self.service.call(req));
        }

        let identity = match TransitionalIdentity::from_headers(req.headers()) {
            Ok(identity) => identity,
            Err(err) => return Box::pin(ready(Err(err.into()))),
        };
        if identity.is_empty() {
            return Box::pin(self.service.call(req));
        }

        let is_write = !matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS);
        let stated = StatedRequest { identity, is_write };
        Box::pin(with_stated_request(stated, self.service.call(req)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::config::identity_policy::IdentityPolicy;
    use crate::transitional_identity::REPO_UUID_HEADER;
    use actix_web::test as actix_test;
    use actix_web::{App, HttpResponse, web};
    use liboxen::request_context::get_request_id;

    async fn responds_ok() -> HttpResponse {
        HttpResponse::Ok().finish()
    }

    /// A malformed header is a 400, so a status other than 400 means the header was not read.
    async fn status_for_a_malformed_header(config: Config) -> u16 {
        let app_data = OxenAppData {
            path: std::path::PathBuf::from("/tmp/does-not-need-to-exist"),
            config,
            test_mode: true,
        };
        let app = actix_test::init_service(
            App::new()
                .app_data(app_data)
                .wrap(TransitionalIdentityMiddleware)
                .route("/", web::get().to(responds_ok)),
        )
        .await;

        let req = actix_test::TestRequest::get()
            .uri("/")
            .insert_header((REPO_UUID_HEADER, "not-a-uuid"))
            .to_request();
        // The middleware refuses by returning an error rather than a response, so both shapes
        // have to be read for a status.
        match actix_test::try_call_service(&app, req).await {
            Ok(res) => res.status().as_u16(),
            // The rendered response, not `status_code()`: `OxenHttpError` builds its status in
            // `error_response`, which is the path actix renders through.
            Err(err) => err.error_response().status().as_u16(),
        }
    }

    /// A server that owns its own namespaces is given real names in the URL, so a caller stating
    /// identity is ignored rather than obeyed or rejected.
    #[actix_web::test]
    async fn a_stated_identity_is_ignored_where_the_server_owns_its_namespaces() {
        assert_eq!(status_for_a_malformed_header(Config::default()).await, 200);
    }

    #[actix_web::test]
    async fn a_stated_identity_is_read_where_a_control_plane_owns_namespaces() {
        let config = Config {
            identity: toml::from_str::<IdentityPolicy>(
                r#"repo_uuids_assigned_by = "auth-provider""#,
            )
            .expect("a known source parses"),
            ..Default::default()
        };
        assert_eq!(status_for_a_malformed_header(config).await, 400);
    }

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
    async fn test_request_start_log_middleware_passes_through() {
        use actix_web::{App, HttpResponse, http::header, test, web};

        let app = actix_test::init_service(App::new().wrap(RequestStartLogMiddleware).route(
            "/x",
            web::get().to(|| async { HttpResponse::Ok().finish() }),
        ))
        .await;

        let req = actix_test::TestRequest::get()
            .uri("/x?page=1")
            .insert_header((header::USER_AGENT, "oxen-test-agent"))
            .insert_header((header::REFERER, "http://example.test/prev"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
    }
}
