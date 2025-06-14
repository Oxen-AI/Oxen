use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn tree() -> Scope {
    web::scope("/tree")
        .service(
            web::scope("/nodes")
                .route("", web::post().to(controllers::tree::create_nodes))
                .route(
                    "/{resource:.*}",
                    web::get().to(controllers::tree::get_node_hash_by_path),
                )
                .route(
                    "/missing_node_hashes",
                    web::post().to(controllers::tree::list_missing_node_hashes),
                )
                .route(
                    "/missing_file_hashes_from_commits",
                    web::post().to(controllers::tree::list_missing_file_hashes_from_commits),
                )
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
            "/download/{base_head}",
            web::get().to(controllers::tree::download_tree_nodes),
        )
        .route("/download", web::get().to(controllers::tree::download_tree))
}
