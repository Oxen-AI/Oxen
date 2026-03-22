use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub mod columns;
pub mod embeddings;
pub mod rows;

pub fn data_frames() -> Scope {
    web::scope("/data_frames")
        .route(
            "/branch/{branch:.*}",
            web::get().to(controllers::workspaces::data_frames::get_by_branch),
        )
        .route(
            "/diff/{path:.*}",
            web::get().to(controllers::workspaces::data_frames::diff),
        )
        .route(
            "/download/{path:.*}",
            web::get().to(controllers::workspaces::data_frames::download),
        )
        .route(
            "/rename/{path:.*}",
            web::put().to(controllers::workspaces::data_frames::rename),
        )
        .route(
            "/resource/{path:.*}",
            web::get().to(controllers::workspaces::data_frames::get),
        )
        .route(
            "/schema/{path:.*}",
            web::get().to(controllers::workspaces::data_frames::get_schema),
        )
        .route(
            "/resource/{path:.*}",
            web::put().to(controllers::workspaces::data_frames::put),
        )
        .route(
            "/resource/{path:.*}",
            web::delete().to(controllers::workspaces::data_frames::delete),
        )
        .service(rows::rows())
        .service(columns::columns())
        .service(embeddings::embeddings())
}
