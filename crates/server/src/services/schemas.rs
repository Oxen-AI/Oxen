use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn schemas() -> Scope {
    web::scope("/schemas").route(
        "/{resource:.*}",
        web::get().to(controllers::schemas::list_or_get),
    )
}
