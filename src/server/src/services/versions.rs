use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub mod chunks;

pub fn versions() -> Scope {
    web::scope("/versions")
        .route(
            "",
            web::get().to(controllers::entries::download_data_from_version_paths),
        )
        .route("", web::post().to(controllers::versions::batch_upload))
        .route(
            "/{version_id}/metadata",
            web::get().to(controllers::versions::metadata),
        )
        .route(
            "/{version_id}/chunks/{chunk_number}",
            web::put().to(controllers::versions::chunks::upload),
        )
        .route(
            "/{version_id}/complete",
            web::post().to(controllers::versions::chunks::complete),
        )
        .route(
            "/{version_id}/chunks/download",
            web::get().to(controllers::versions::chunks::download),
        )
        .route(
            "/{version_id}",
            web::get().to(controllers::versions::download),
        )
}
