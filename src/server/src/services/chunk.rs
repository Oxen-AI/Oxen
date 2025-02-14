use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn chunk() -> Scope {
    web::scope("/chunk").route(
        "/{resource:.*}",
        web::get().to(controllers::entries::download_chunk),
    )
}
