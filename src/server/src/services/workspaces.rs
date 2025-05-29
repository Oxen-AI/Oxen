use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub mod data_frames;

pub fn workspace() -> Scope {
    web::scope("/workspaces")
        .route("", web::put().to(controllers::workspaces::get_or_create))
        .route("", web::post().to(controllers::workspaces::create))
        .route("", web::get().to(controllers::workspaces::list))
        .route("", web::delete().to(controllers::workspaces::clear))
        .service(
            web::scope("/{workspace_id}")
                .route("", web::get().to(controllers::workspaces::get))
                .route("", web::delete().to(controllers::workspaces::delete))
                .route(
                    "/changes",
                    web::get().to(controllers::workspaces::changes::list_root),
                )
                .route(
                    "/changes/{path:.*}",
                    web::get().to(controllers::workspaces::changes::list),
                )
                .route(
                    "/changes/{path:.*}",
                    web::delete().to(controllers::workspaces::files::delete),
                )
                .route(
                    "/versions",
                    web::post().to(controllers::workspaces::files::add_version_file),
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
                // TODO: Depreciate /commit as we are calling it /merge instead to be consistent with the /merge branch endpoint
                .route(
                    "/commit/{branch:.*}",
                    web::post().to(controllers::workspaces::commit),
                )
                .route(
                    "/merge/{branch:.*}",
                    web::post().to(controllers::workspaces::commit),
                )
                .route(
                    "/merge/{branch:.*}",
                    web::get().to(controllers::workspaces::mergeability),
                )
                .service(data_frames::data_frames()),
        )
}
