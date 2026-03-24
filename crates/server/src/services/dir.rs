use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn dir() -> Scope {
    web::scope("/dir").route("/{resource:.*}", web::get().to(controllers::dir::get))
}
