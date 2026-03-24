use crate::api::endpoint;

pub fn http_endpoint(host: &str) -> String {
    let scheme = endpoint::get_scheme(host);
    format!("{scheme}://{host}/api/v1")
}
