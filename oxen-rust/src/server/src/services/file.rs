use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn file() -> Scope {
    web::scope("/file")
        .route("/{resource:.*}", web::get().to(controllers::file::get))
        .route("/{resource:.*}", web::head().to(controllers::file::get))
        .route("/{resource:.*}", web::put().to(controllers::file::put))
        .route("/{resource:.*}", web::delete().to(controllers::file::delete))
        .route(
            "/import/{resource:.*}",
            web::post().to(controllers::file::import),
        )
}
