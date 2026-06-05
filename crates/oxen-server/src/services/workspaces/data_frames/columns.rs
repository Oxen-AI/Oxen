use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn columns() -> Scope {
    web::scope("/columns")
        .route(
            "/resource/{path:.*}",
            web::post().to(controllers::workspaces::data_frames::columns::create),
        )
        .route(
            "/schema/metadata/{path:.*}",
            web::post().to(controllers::workspaces::data_frames::columns::add_column_metadata),
        )
        .route(
            "{column_name:.*}/resource/{path:.*}",
            web::delete().to(controllers::workspaces::data_frames::columns::delete),
        )
        .route(
            "{column_name:.*}/resource/{path:.*}",
            web::put().to(controllers::workspaces::data_frames::columns::update),
        )
        .route(
            "/{column_name:.*}/restore/{path:.*}",
            web::post().to(controllers::workspaces::data_frames::columns::restore),
        )
}
