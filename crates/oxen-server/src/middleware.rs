use actix_web::{
    Error, HttpMessage, HttpRequest,
    dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready},
    http::header,
};
use futures_util::future::LocalBoxFuture;
use liboxen::request_context::REQUEST_ID;
use std::future::{Ready, ready};

// Oxen request Id
pub const OXEN_REQUEST_ID: &str = "x-oxen-request-id";

pub fn extract_or_generate_request_id(headers: &actix_web::http::header::HeaderMap) -> String {
    headers
        .get(OXEN_REQUEST_ID)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(generate_request_id)
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
