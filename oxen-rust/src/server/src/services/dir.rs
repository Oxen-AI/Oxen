use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn dir() -> Scope {
    web::scope("/dir")
        .route("/download/{resource:.*}", web::get().to(controllers::dir::get))
        .route(
            "/download_zip/{resource:.*}",
            web::get().to(controllers::dir::download_dir_as_zip),
        )
}
