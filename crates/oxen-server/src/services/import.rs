use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn import() -> Scope {
    web::scope("/import")
        .service(web::scope("/upload").route(
            "/{resource:.*}",
            web::post().to(controllers::import::upload_zip),
        ))
        .route(
            "/{resource:.*}",
            web::post().to(controllers::import::import),
        )
}
