use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn import() -> Scope {
    web::scope("/import")
        .route(
            "/{resource:.*}",
            web::post().to(controllers::import::import),
        )
        .route(
            "/upload_zip/{resource:.*}",
            web::post().to(controllers::import::upload_zip),
        )
}
