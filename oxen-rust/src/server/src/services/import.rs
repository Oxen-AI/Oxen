use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn import() -> Scope {
    web::scope("/import")
        .service(web::scope("/upload").route(
            "/{resource:.*}",
            web::post().to(controllers::import::upload_zip),
        ))
        .service(web::scope("/db").route(
            "/{resource:.*}",
            web::post().to(controllers::db_import::import_db),
        ))
        .route(
            "/{resource:.*}",
            web::post().to(controllers::import::import),
        )
}
