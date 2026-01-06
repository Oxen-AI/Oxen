use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn export() -> Scope {
    web::scope("/export").route(
        "/download_zip/{resource:.*}",
        web::get().to(controllers::export::download_zip),
    )
}
