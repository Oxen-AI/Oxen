use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn rename() -> Scope {
    web::scope("/rename").route("", web::patch().to(controllers::repositories::rename))
}
