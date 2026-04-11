use actix_web::{
    Error, HttpMessage,
    dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready},
};
use futures_util::future::LocalBoxFuture;
use liboxen::request_context::REQUEST_ID;
use std::future::{Ready, ready};
use std::time::Instant;
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
        req.extensions_mut().insert(request_id.clone());

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

/// Middleware that records HTTP request count and latency for every route.
///
/// Emits three (3) Prometheus metrics per request:
///   1. `http_requests_total{method, path, status}` — counter
///   2. 'http_errors_total{method, path, status}`   — counter
///   3. `http_request_duration_ms{method, path}`    — histogram (milliseconds)
///
/// The `path` label uses the matched Actix route pattern (e.g.
/// `/api/repos/{namespace}/{repo_name}/branches`) to keep cardinality low.
pub struct MetricsMiddleware;

const HTTP_REQUESTS_TOTAL: &str = "http_requests_total";
const HTTP_ERRORS_TOTAL: &str = "http_errors_total";
const HTTP_REQUEST_DURATION_MS: &str = "http_request_duration_ms";

const METHOD: &str = "method";
const PATH: &str = "path";
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

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let start = Instant::now();
        let method = req.method().to_string();

        let fut = self.service.call(req);

        Box::pin(async move {
            match fut.await {
                Ok(res) => {
                    let status = res.status().as_u16().to_string();
                    let path = res
                        .request()
                        .match_pattern()
                        .unwrap_or_else(|| "unmatched".to_string());
                    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

                    metrics::counter!(HTTP_REQUESTS_TOTAL, METHOD => method.clone(), PATH => path.clone(), STATUS => status.clone())
                        .increment(1);
                    if res.status().is_client_error() || res.status().is_server_error() {
                        metrics::counter!(HTTP_ERRORS_TOTAL, METHOD => method.clone(), PATH => path.clone(), STATUS => status)
                            .increment(1);
                    }
                    metrics::histogram!(HTTP_REQUEST_DURATION_MS, METHOD => method, PATH => path)
                        .record(elapsed_ms);

                    Ok(res)
                }
                Err(err) => {
                    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
                    let status = "500";
                    let path = "unmatched";

                    metrics::counter!(HTTP_REQUESTS_TOTAL, METHOD => method.clone(), PATH => path, STATUS => status)
                        .increment(1);
                    metrics::counter!(HTTP_ERRORS_TOTAL, METHOD => method.clone(), PATH => path, STATUS => status)
                        .increment(1);
                    metrics::histogram!(HTTP_REQUEST_DURATION_MS, METHOD => method, PATH => path)
                        .record(elapsed_ms);

                    Err(err)
                }
            }
        })
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
}
