use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn file() -> Scope {
    web::scope("/file")
        .route("/{resource:.*}", web::get().to(controllers::file::get))
        .route("/{resource:.*}", web::head().to(controllers::file::get))
        .route("/{resource:.*}", web::put().to(controllers::file::put))
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
