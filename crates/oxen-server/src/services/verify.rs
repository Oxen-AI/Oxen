use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn verify() -> Scope {
    web::scope("/verify").route("", web::post().to(controllers::verify::verify))
}
