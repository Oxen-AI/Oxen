use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn chunk() -> Scope {
    web::scope("/chunk").route(
        "/{resource:.*}",
        web::get().to(controllers::entries::download_chunk),
    )
}
