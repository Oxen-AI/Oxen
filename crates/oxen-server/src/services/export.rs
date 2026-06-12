use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn export() -> Scope {
    web::scope("/export").route(
        "/download/{resource:.*}",
        web::get().to(controllers::export::download_zip),
    )
}
