use std::cell::RefCell;

tokio::task_local! {
    pub static REQUEST_ID: RefCell<Option<String>>;
}

pub fn get_request_id() -> Option<String> {
    REQUEST_ID.try_with(|id| id.borrow().clone()).ok().flatten()
}

pub fn generate_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub fn extract_or_generate_request_id(headers: &actix_web::http::header::HeaderMap) -> String {
    headers
        .get("x-oxen-request-id")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(generate_request_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_request_id_task_local() {
        let request_id = generate_request_id();

        REQUEST_ID
            .scope(
                std::cell::RefCell::new(Some(request_id.clone())),
                async move {
                    assert_eq!(get_request_id(), Some(request_id));
                },
            )
            .await;
    }

    #[tokio::test]
    async fn test_no_request_id() {
        // Outside scope, should return None
        assert_eq!(get_request_id(), None);
    }

    #[test]
    fn test_extract_request_id_from_header() {
        use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-oxen-request-id"),
            HeaderValue::from_static("test-id-123"),
        );

        let id = extract_or_generate_request_id(&headers);
        assert_eq!(id, "test-id-123");
    }

    #[test]
    fn test_generate_request_id_when_missing() {
        use actix_web::http::header::HeaderMap;

        let headers = HeaderMap::new();
        let id = extract_or_generate_request_id(&headers);

        // Should be valid UUID format
        assert_eq!(id.len(), 36); // UUID length with hyphens
    }
}
