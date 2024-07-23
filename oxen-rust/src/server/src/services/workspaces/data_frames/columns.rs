use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn columns() -> Scope {
    web::scope("/columns")
        .route(
            "/resource/{path:.*}",
            web::post().to(controllers::workspaces::data_frames::columns::create),
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
            web::post().to(controllers::workspaces::data_frames::rows::restore),
        )
    // .route(
    //     "/{row_id}/resource/{path:.*}",
    //     web::get().to(controllers::workspaces::data_frames::rows::get),
    // )
}
