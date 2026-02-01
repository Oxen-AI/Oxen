use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn branches() -> Scope {
    web::scope("/branches")
        .route("", web::get().to(controllers::branches::index))
        .route("", web::post().to(controllers::branches::create))
        .route(
            "/{branch_name:.*}/versions/{path:.*}",
            web::get().to(controllers::branches::list_entry_versions),
        )
        .route(
            "/{branch_name:.*}/merge",
            web::put().to(controllers::branches::maybe_create_merge),
        )
        .route(
            "/{branch_name:.*}",
            web::get().to(controllers::branches::show),
        )
        .route(
            "/{branch_name:.*}",
            web::delete().to(controllers::branches::delete),
        )
        .route(
            "/{branch_name:.*}",
            web::put().to(controllers::branches::update),
        )
}
