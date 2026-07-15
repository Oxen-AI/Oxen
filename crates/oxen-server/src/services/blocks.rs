use actix_web::Scope;
use actix_web::web;

use crate::controllers;

/// Block-dedup wire protocol routes (push negotiation and transfer). `/chunks` is
/// content-defined chunks — distinct from the legacy `/chunk` file-segment route
/// and the `/versions/{id}/chunks` upload segments.
pub fn chunks() -> Scope {
    web::scope("/chunks").route("/missing", web::post().to(controllers::chunks::missing))
}

pub fn blocks() -> Scope {
    web::scope("/blocks").route(
        "/{block_hash}",
        web::put().to(controllers::chunks::upload_block),
    )
}

pub fn manifests() -> Scope {
    web::scope("/manifests").route(
        "/{file_hash}",
        web::put().to(controllers::chunks::upload_manifest),
    )
}
