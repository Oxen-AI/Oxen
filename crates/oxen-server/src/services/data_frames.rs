use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn data_frames() -> Scope {
    web::scope("/data_frames")
        .route(
            "/index/{resource:.*}",
            web::post().to(controllers::data_frames::index),
        )
        .route(
            "/from_directory/{resource:.*}",
            web::post().to(controllers::data_frames::from_directory),
        )
        .route(
            "/{resource:.*}",
            web::get().to(controllers::data_frames::get),
        )
}
