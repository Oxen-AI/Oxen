use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn prune() -> Scope {
    web::scope("/prune").route("", web::post().to(controllers::prune::prune))
}
