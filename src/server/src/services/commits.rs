use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn commits() -> Scope {
    web::scope("/commits")
        //  This is commented out because the list_commit function reads from the head file, which should not be used server side
        // .route("", web::get().to(controllers::commits::index))
        .route("", web::post().to(controllers::commits::create))
        .route("/root", web::get().to(controllers::commits::root_commit))
        .route("/all", web::get().to(controllers::commits::list_all))
        .route("/upload", web::post().to(controllers::commits::upload))
        .route(
            "/upload_chunk",
            web::post().to(controllers::commits::upload_chunk),
        )
        .route(
            "/missing",
            web::post().to(controllers::commits::list_missing),
        )
        .route(
            "/mark_hashes_as_synced",
            web::post().to(controllers::commits::mark_hashes_as_synced),
        )
        .route("/{commit_id}", web::get().to(controllers::commits::show))
        .route(
            "/{commit_id}/complete",
            web::post().to(controllers::commits::complete),
        )
        .route(
            "/history/{resource:.*}",
            web::get().to(controllers::commits::history),
        )
        .route(
            "/{commit_or_branch:.*}/parents",
            web::get().to(controllers::commits::parents),
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
