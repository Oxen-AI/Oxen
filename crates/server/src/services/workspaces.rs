use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub mod data_frames;

pub fn workspace() -> Scope {
    web::scope("/workspaces")
        .route("", web::put().to(controllers::workspaces::get_or_create))
        // POST is deprecated - use PUT for idempotent get-or-create behavior
        .route("", web::post().to(controllers::workspaces::get_or_create))
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
                    "/changes",
                    web::delete().to(controllers::workspaces::changes::unstage_many),
                )
                .route(
                    "/changes/{path:.*}",
                    web::delete().to(controllers::workspaces::changes::unstage),
                )
                .route(
                    "/versions/{directory:.*}",
                    web::post().to(controllers::workspaces::files::add_version_files),
                )
                // DEPRECATED: use DELETE /files instead
                .route(
                    "/versions",
                    web::delete().to(controllers::workspaces::files::rm_files),
                )
                // DEPRECATED: use DELETE /changes instead
                .route(
                    "/staged",
                    web::delete().to(controllers::workspaces::changes::unstage_many),
                )
                .route(
                    "/files/{path:.*}",
                    web::patch().to(controllers::workspaces::files::mv),
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
                    "/files",
                    web::delete().to(controllers::workspaces::files::rm_files),
                )
                // DEPRECATED: use DELETE /changes/{path:.*} instead
                .route(
                    "/files/{path:.*}",
                    web::delete().to(controllers::workspaces::changes::unstage),
                )
                .route(
                    "/validate",
                    web::post().to(controllers::workspaces::files::validate),
                )
                .route(
                    "/merge/{branch:.*}",
                    web::post().to(controllers::workspaces::commit),
                )
                // /commit is deprecated - use /merge instead to be consistent with the /merge branch endpoint
                .route(
                    "/commit/{branch:.*}",
                    web::post().to(controllers::workspaces::commit),
                )
                .route(
                    "/merge/{branch:.*}",
                    web::get().to(controllers::workspaces::mergeability),
                )
                .service(data_frames::data_frames()),
        )
}
