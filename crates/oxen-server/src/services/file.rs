use actix_multipart::MultipartError;
use actix_multipart::form::MultipartFormConfig;
use actix_web::Scope;
use actix_web::error::{InternalError, PayloadError};
use actix_web::{HttpResponse, guard, web};
use liboxen::view::StatusMessageDescription;

use crate::controllers;
use crate::errors::OxenHttpError;

/// The most one file upload reads, all of its files together and all kept in memory: 100 MiB.
const MAX_UPLOAD_BYTES: usize = 100 * 1024 * 1024;

pub fn file() -> Scope {
    web::scope("/file")
        .route("/{resource:.*}", web::get().to(controllers::file::get))
        .route("/{resource:.*}", web::head().to(controllers::file::get))
        .service(
            web::resource("/{resource:.*}")
                .guard(guard::Put())
                .app_data(upload_limits(MAX_UPLOAD_BYTES))
                .route(web::put().to(controllers::file::put)),
        )
        .route("/{resource:.*}", web::patch().to(controllers::file::mv))
        .route(
            "/{resource:.*}",
            web::delete().to(controllers::file::delete),
        )
        // Note: the 'upload_zip' and 'import' routes here are deprecated.
        // Please use the import module
        .route(
            "/upload_zip/{resource:.*}",
            web::post().to(controllers::import::upload_zip),
        )
        .route(
            "/import/{resource:.*}",
            web::post().to(controllers::import::import),
        )
}

/// Reads at most `max_bytes` of a multipart form, answering a larger one with 413 and the limit.
pub(crate) fn upload_limits(max_bytes: usize) -> MultipartFormConfig {
    MultipartFormConfig::default()
        .total_limit(max_bytes)
        .memory_limit(max_bytes)
        .error_handler(move |err, _req| match err {
            MultipartError::Payload(PayloadError::Overflow) => {
                let description = format!(
                    "An upload takes at most {} MiB in one request. Add larger files to a \
                     workspace and commit it.",
                    max_bytes / (1024 * 1024)
                );
                let response = HttpResponse::PayloadTooLarge()
                    .json(StatusMessageDescription::bad_request(&description));
                InternalError::from_response(description, response).into()
            }
            err => OxenHttpError::MultipartError(err).into(),
        })
}
