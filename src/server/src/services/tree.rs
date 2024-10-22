use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn tree() -> Scope {
    web::scope("/tree")
        .service(
            web::scope("/nodes")
                .route("", web::post().to(controllers::tree::create_node))
                .service(
                    web::scope("/{hash}")
                        .route("", web::get().to(controllers::tree::get_node_by_id))
                        .route("/download", web::get().to(controllers::tree::download_node))
                        .route(
                            "/missing_file_hashes",
                            web::get().to(controllers::tree::list_missing_file_hashes),
                        ),
                ),
        )
        .route(
            "/commits/{base_head}/download",
            web::get().to(controllers::tree::download_commits),
        )
        .route(
            "/download/{hash}",
            web::get().to(controllers::tree::download_tree_from),
        )
        .route("/download", web::get().to(controllers::tree::download_tree))
}
