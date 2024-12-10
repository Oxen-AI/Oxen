use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub mod data_frames;

pub fn workspace() -> Scope {
    web::scope("/workspaces")
        .route("", web::put().to(controllers::workspaces::get_or_create))
        .route("", web::post().to(controllers::workspaces::create))
        .route("", web::get().to(controllers::workspaces::list))
        .service(
            web::scope("/{workspace_id}")
                .route("", web::get().to(controllers::workspaces::get))
                .route("", web::delete().to(controllers::workspaces::delete))
                .route(
                    "/changes/{path:.*}",
                    web::get().to(controllers::workspaces::changes::list),
                )
                .route(
                    "/changes/{path:.*}",
                    web::delete().to(controllers::workspaces::files::delete),
                )
                .route(
                    "/files/{path:.*}",
                    web::get().to(controllers::workspaces::files::get),
                )
                .route(
                    "/files/{path:.*}",
                    web::post().to(controllers::workspaces::files::add),
                )
                .route(
                    "/files/{path:.*}",
                    web::delete().to(controllers::workspaces::files::delete),
                )
                .route(
                    "/commit/{branch:.*}",
                    web::post().to(controllers::workspaces::commit),
                )
                .service(data_frames::data_frames()),
        )
}
