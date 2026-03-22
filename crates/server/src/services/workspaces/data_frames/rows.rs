use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn rows() -> Scope {
    web::scope("/rows")
        .route(
            "/{row_id}/restore/{path:.*}",
            web::post().to(controllers::workspaces::data_frames::rows::restore),
        )
        .route(
            "/resource/{path:.*}",
            web::post().to(controllers::workspaces::data_frames::rows::create),
        )
        .route(
            "/{row_id}/resource/{path:.*}",
            web::put().to(controllers::workspaces::data_frames::rows::update),
        )
        .route(
            "/resource/{path:.*}",
            web::put().to(controllers::workspaces::data_frames::rows::batch_update),
        )
        .route(
            "/{row_id}/resource/{path:.*}",
            web::delete().to(controllers::workspaces::data_frames::rows::delete),
        )
        .route(
            "/{row_id}/resource/{path:.*}",
            web::get().to(controllers::workspaces::data_frames::rows::get),
        )
}
