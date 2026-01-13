use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error, HttpMessage,
};
use futures_util::future::LocalBoxFuture;
use std::future::{ready, Ready};

use crate::constants::OXEN_REQUEST_ID;
use liboxen::request_context::{extract_or_generate_request_id, REQUEST_ID};

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
