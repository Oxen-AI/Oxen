use actix_web::{HttpRequest, HttpResponse};
use liboxen::view::StatusMessage;

#[tracing::instrument(skip_all)]
pub async fn index(_req: HttpRequest) -> HttpResponse {
    metrics::counter!("oxen_server_not_found_index_total").increment(1);
    HttpResponse::NotFound().json(StatusMessage::resource_not_found())
}
