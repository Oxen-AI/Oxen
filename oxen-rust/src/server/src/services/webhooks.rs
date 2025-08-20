use actix_web::{web, Scope};

use crate::controllers;

pub fn webhooks() -> Scope {
    web::scope("/webhooks")
        .route("/add", web::post().to(controllers::webhooks::add_webhook))
        .route("", web::get().to(controllers::webhooks::list_webhooks))
        .route("/stats", web::get().to(controllers::webhooks::webhook_stats))
        .route("/config", web::get().to(controllers::webhooks::get_webhook_config))
        .route("/config", web::put().to(controllers::webhooks::set_webhook_config))
        .route("/{webhook_id}", web::delete().to(controllers::webhooks::remove_webhook))
        .route("/cleanup", web::post().to(controllers::webhooks::cleanup_webhooks))
}