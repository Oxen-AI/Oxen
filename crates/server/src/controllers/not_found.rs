use actix_web::{HttpRequest, HttpResponse};
use liboxen::view::StatusMessage;

pub async fn index(_req: HttpRequest) -> HttpResponse {
    HttpResponse::NotFound().json(StatusMessage::resource_not_found())
}
