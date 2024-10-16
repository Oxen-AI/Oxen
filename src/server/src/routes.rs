use super::controllers;

use actix_web::web;

use crate::services;

pub fn config(cfg: &mut web::ServiceConfig) {
    // Create Repository
    cfg.route("", web::post().to(controllers::repositories::create))
        // List Repositories in a Namespace
        .route(
            "/{namespace}",
            web::get().to(controllers::repositories::index),
        )
        // Get/Delete Repository
        .service(
            web::resource("/{namespace}/{repo_name}")
                // we give the resource a name here so it can be used with HttpRequest.url_for
                .name("repo_root")
                .route(web::get().to(controllers::repositories::show))
                .route(web::delete().to(controllers::repositories::delete)),
        )
        // Repository Services
        .service(
            web::scope("/{namespace}/{repo_name}")
                .service(services::action())
                .service(services::branches())
                .service(services::chunk())
                .service(services::commits())
                .service(services::commits_db())
                .service(services::compare())
                .service(services::data_frames())
                .service(services::dir())
                .service(services::file())
                .service(services::merge())
                .service(services::meta())
                .service(services::objects_db())
                .service(services::revisions())
                .service(services::schemas())
                .service(services::stats())
                .service(services::tabular())
                .service(services::transfer())
                .service(services::tree())
                .service(services::versions())
                .service(services::workspace()),
        );
}
