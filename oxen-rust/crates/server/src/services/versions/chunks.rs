use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn chunks() -> Scope {
    web::scope("/chunks").route("", web::post().to(controllers::versions::chunks::complete))
}
