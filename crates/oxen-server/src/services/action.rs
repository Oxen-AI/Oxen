use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn action() -> Scope {
    web::scope("/action")
        // Ahead of the wildcard below, so only a POST to this exact path starts the size
        // recalculation a finished push needs.
        .route(
            "/completed/push",
            web::post().to(controllers::action::completed_push),
        )
        .route(
            "/completed/{action}",
            web::get().to(controllers::action::completed),
        )
        .route(
            "/started/{action}",
            web::get().to(controllers::action::started),
        )
        .route(
            "/completed/{action}",
            web::post().to(controllers::action::completed),
        )
        .route(
            "/started/{action}",
            web::post().to(controllers::action::started),
        )
}
