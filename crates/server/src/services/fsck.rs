use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn fsck() -> Scope {
    web::scope("/fsck")
        .route("/clean", web::post().to(controllers::fsck::clean))
        .route(
            "/rebuild-dir-hashes",
            web::post().to(controllers::fsck::rebuild_dir_hashes),
        )
}
