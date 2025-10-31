use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn prune() -> Scope {
    web::scope("/prune").route("", web::post().to(controllers::prune::prune))
}
