use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn action() -> Scope {
    web::scope("/action")
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
