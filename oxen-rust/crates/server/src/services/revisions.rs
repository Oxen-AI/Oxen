use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn revisions() -> Scope {
    web::scope("/revisions").route("/{resource:.*}", web::get().to(controllers::revisions::get))
}
