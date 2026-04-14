use actix_web::{Scope, web};

use crate::controllers;

pub fn webhooks() -> Scope {
    web::scope("/webhooks")
        .route("/add", web::post().to(controllers::webhooks::create))
        .route("", web::get().to(controllers::webhooks::list))
        .route("/stats", web::get().to(controllers::webhooks::stats))
        .route("/config", web::get().to(controllers::webhooks::get_config))
        .route(
            "/config",
            web::put().to(controllers::webhooks::update_config),
        )
        .route(
            "/{webhook_id}",
            web::delete().to(controllers::webhooks::delete),
        )
}
