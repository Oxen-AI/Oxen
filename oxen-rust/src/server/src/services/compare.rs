use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn compare() -> Scope {
    web::scope("/compare")
        .route(
            "/dir_tree/{base_head:.*}",
            web::get().to(controllers::diff::dir_tree),
        )
        .route(
            "/entries/{base_head:.*}/dir/{dir:.*}",
            web::get().to(controllers::diff::dir_entries),
        )
        .route(
            "/entries/{base_head:.*}",
            web::get().to(controllers::diff::entries),
        )
        .route(
            "/file/{base_head:.*}",
            web::get().to(controllers::diff::file),
        )
        .route(
            "/data_frames/{compare_id}/{path}/{base_head:.*}",
            web::get().to(controllers::diff::get_derived_df),
        )
        .route(
            "/data_frames/{compare_id}",
            web::post().to(controllers::diff::get_df_diff),
        )
        .route(
            "/data_frames/{compare_id}",
            web::put().to(controllers::diff::update_df_diff),
        )
        .route(
            "/data_frames",
            web::post().to(controllers::diff::create_df_diff),
        )
        .route(
            "/data_frames/{compare_id}",
            web::delete().to(controllers::diff::delete_df_diff),
        )
}
