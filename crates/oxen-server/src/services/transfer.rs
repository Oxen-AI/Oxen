use actix_web::Scope;
use actix_web::web;

use crate::controllers;

pub fn transfer() -> Scope {
    web::scope("/transfer").route(
        "",
        web::patch().to(controllers::repositories::transfer_namespace),
    )
}
