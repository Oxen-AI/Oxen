use actix_web::{Scope, web};

use crate::controllers;

pub fn migrations() -> Scope {
    web::scope("/migrations").route(
        "/{migration_name}",
        web::post().to(controllers::migrations::run),
    )
}
