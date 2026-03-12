use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn size() -> Scope {
    web::scope("/size")
        .route("", web::post().to(controllers::repositories::update_size))
        .route("", web::get().to(controllers::repositories::get_size))
}
