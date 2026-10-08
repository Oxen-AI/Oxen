use actix_web::{Scope, web};

use crate::controllers;

pub fn storage() -> Scope {
    web::scope("/storage").route("", web::put().to(controllers::storage::update))
}
