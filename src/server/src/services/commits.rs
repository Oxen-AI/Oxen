use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn commits() -> Scope {
    web::scope("/commits")
        .route("", web::get().to(controllers::commits::index))
        .route("", web::post().to(controllers::commits::create))
        .route("/bulk", web::post().to(controllers::commits::create_bulk))
        .route("/root", web::get().to(controllers::commits::root_commit))
        .route(
            "/complete",
            web::post().to(controllers::commits::complete_bulk),
        )
        .route(
            "/{commit_id}/db_status",
            web::get().to(controllers::commits::commits_db_status),
        )
        .route(
            "/{commit_id}/entries_status",
            web::get().to(controllers::commits::entries_status),
        )
        .route("/all", web::get().to(controllers::commits::list_all))
        .route(
            "/missing",
            web::post().to(controllers::commits::list_missing),
        )
        .route(
            "/{commit_id}/latest_synced",
            web::get().to(controllers::commits::latest_synced),
        )
        .route("/{commit_id}", web::get().to(controllers::commits::show))
        .route(
            "/{commit_id}/data",
            web::post().to(controllers::commits::upload),
        )
        .route(
            "/{commit_id}/can_push",
            web::get().to(controllers::commits::can_push),
        )
        .route(
            "/{commit_id}/complete",
            web::post().to(controllers::commits::complete),
        )
        .route(
            "/{commit_id}/upload_chunk",
            web::post().to(controllers::commits::upload_chunk),
        )
        .route(
            "/history/{resource:.*}",
            web::get().to(controllers::commits::commit_history),
        )
        .route(
            "/{commit_or_branch:.*}/parents",
            web::get().to(controllers::commits::parents),
        )
        .route(
            "/{commit_or_branch:.*}/is_synced",
            web::get().to(controllers::commits::is_synced),
        )
        .route(
            "/{commit_or_branch:.*}/commit_db",
            web::get().to(controllers::commits::download_commit_entries_db),
        )
        .route(
            "/{base_head}/download_dir_hashes_db",
            web::get().to(controllers::commits::download_dir_hashes_db),
        )
}
