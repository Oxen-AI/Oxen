use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn meta() -> Scope {
    web::scope("/meta")
        .route("/{resource:.*}", web::get().to(controllers::metadata::file))
        .route(
            "/{resource:.*}",
            web::post().to(controllers::metadata::update_metadata),
        )
}
