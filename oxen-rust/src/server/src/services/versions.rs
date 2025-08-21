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
            // TODO: change the endpoint to /{version_id} for consistency and a more efficient download when client has the file hash
            // For this change, a easy mapping from file_hash to file_node is needed.
            "/{resource:.*}",
            web::get().to(controllers::versions::download),
        )
        .route(
            //TODO: The versions chunk download endpoint is not functional now. Needs the same changes mentioned above.
            "/{version_id}/chunks/download",
            web::get().to(controllers::versions::chunks::download),
        )
        .route(
            "/{version_id}/create",
            web::post().to(controllers::versions::chunks::create),
        )
}
