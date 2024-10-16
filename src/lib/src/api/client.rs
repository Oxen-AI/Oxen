//! # API Client - For interacting with repositories on a remote machine
//!

use crate::config::AuthConfig;
use crate::error::OxenError;
use crate::view::http;
use crate::view::OxenResponse;

pub use reqwest::Url;
use reqwest::{header, Client, ClientBuilder, IntoUrl};

pub mod branches;
pub mod commits;
pub mod compare;
pub mod data_frames;
pub mod diff;
pub mod dir;
pub mod entries;
pub mod merger;
pub mod metadata;
pub mod repositories;
pub mod schemas;
pub mod stats;
pub mod tree;
pub mod version;
pub mod workspaces;

const VERSION: &str = crate::constants::OXEN_VERSION;
const USER_AGENT: &str = "Oxen";

pub fn get_host_from_url<U: IntoUrl>(url: U) -> Result<String, OxenError> {
    let parsed_url = url.into_url()?;
    let mut host_str = parsed_url.host_str().unwrap_or_default().to_string();
    if let Some(port) = parsed_url.port() {
        host_str = format!("{host_str}:{port}");
    }
    Ok(host_str)
}

// TODO: we probably want to create a pool of clients instead of constructing a
// new one for each request so we can take advantage of keep-alive
pub fn new_for_url<U: IntoUrl>(url: U) -> Result<Client, OxenError> {
    let host = get_host_from_url(url)?;
    new_for_host(host, true)
}

pub fn new_for_url_no_user_agent<U: IntoUrl>(url: U) -> Result<Client, OxenError> {
    let host = get_host_from_url(url)?;
    new_for_host(host, false)
}

fn new_for_host<S: AsRef<str>>(host: S, should_add_user_agent: bool) -> Result<Client, OxenError> {
    match builder_for_host(host.as_ref(), should_add_user_agent)?.build() {
        Ok(client) => Ok(client),
        Err(reqwest_err) => Err(OxenError::HTTP(reqwest_err)),
    }
}

pub fn builder_for_url<U: IntoUrl>(url: U) -> Result<ClientBuilder, OxenError> {
    let host = get_host_from_url(url)?;
    builder_for_host(host, true)
}

fn builder_for_host<S: AsRef<str>>(
    host: S,
    should_add_user_agent: bool,
) -> Result<ClientBuilder, OxenError> {
    let builder = if should_add_user_agent {
        builder()
    } else {
        builder_no_user_agent()
    };

    let config = match AuthConfig::get() {
        Ok(config) => config,
        Err(err) => {
            log::debug!("remote::client::new_for_host error getting config: {}", err);

            return Ok(builder);
        }
    };
    if let Some(auth_token) = config.auth_token_for_host(host.as_ref()) {
        log::debug!("Setting auth token for host: {}", host.as_ref());
        let auth_header = format!("Bearer {auth_token}");
        let mut auth_value = match header::HeaderValue::from_str(auth_header.as_str()) {
            Ok(header) => header,
            Err(err) => {
                log::debug!("remote::client::new invalid header value: {}", err);
                return Err(OxenError::basic_str(
                    "Error setting request auth. Please check your Oxen config.",
                ));
            }
        };
        auth_value.set_sensitive(true);
        let mut headers = header::HeaderMap::new();
        headers.insert(header::AUTHORIZATION, auth_value);
        Ok(builder.default_headers(headers))
    } else {
        log::debug!("No auth token found for host: {}", host.as_ref());
        Ok(builder)
    }
}

fn builder() -> ClientBuilder {
    Client::builder().user_agent(format!("{USER_AGENT}/{VERSION}"))
}

fn builder_no_user_agent() -> ClientBuilder {
    Client::builder()
}

/// Performs an extra parse to validate that the response is success
pub async fn parse_json_body(url: &str, res: reqwest::Response) -> Result<String, OxenError> {
    let type_override = "unauthenticated";
    let err_msg = "You are unauthenticated.\n\nObtain an API Key at https://oxen.ai or ask you system admin. Set your auth token with the command:\n\n  oxen config --auth hub.oxen.ai YOUR_AUTH_TOKEN\n";

    // Raise auth token error for user if unauthorized and no token set
    if res.status() == reqwest::StatusCode::FORBIDDEN {
        let _ = match AuthConfig::get() {
            Ok(config) => config,
            Err(err) => {
                log::debug!("remote::client::new_for_host error getting config: {}", err);
                return Err(OxenError::auth_token_not_set());
            }
        };
    }

    parse_json_body_with_err_msg(url, res, Some(type_override), Some(err_msg)).await
}

/// Used to override error message when parsing json body
pub async fn parse_json_body_with_err_msg(
    url: &str,
    res: reqwest::Response,
    response_type: Option<&str>,
    response_msg_override: Option<&str>,
) -> Result<String, OxenError> {
    let status = res.status();
    let body = res.text().await?;

    log::debug!("parse_json_body_with_err_msg url: {url}\nstatus: {status}\nbody: {body}");

    let response: Result<OxenResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(response) => parse_status_and_message(
            url,
            body,
            status,
            response,
            response_type,
            response_msg_override,
        ),
        Err(err) => {
            log::debug!("Err: {}", err);
            Err(OxenError::basic_str(format!(
                "Could not deserialize response from [{url}]\n{status}\n'{body}'"
            )))
        }
    }
}

fn parse_status_and_message(
    url: &str,
    body: String,
    status: reqwest::StatusCode,
    response: OxenResponse,
    response_type: Option<&str>,
    response_msg_override: Option<&str>,
) -> Result<String, OxenError> {
    match response.status.as_str() {
        http::STATUS_SUCCESS => {
            log::debug!("Status success: {status}");
            if !status.is_success() {
                return Err(OxenError::basic_str(format!(
                    "Err status [{}] from url {} [{}]",
                    status,
                    url,
                    response.desc_or_msg()
                )));
            }

            Ok(body)
        }
        http::STATUS_WARNING => {
            log::debug!("Status warning: {status}");
            Err(OxenError::basic_str(format!(
                "Remote Warning: {}",
                response.desc_or_msg()
            )))
        }
        http::STATUS_ERROR => {
            log::debug!("Status error: {status}");
            if let Some(msg) = response_msg_override {
                if let Some(response_type) = response_type {
                    if response.desc_or_msg() == response_type {
                        return Err(OxenError::basic_str(msg));
                    }
                }
            }

            Err(OxenError::basic_str(response.full_err_msg()))
        }
        status => Err(OxenError::basic_str(format!("Unknown status [{status}]"))),
    }
}
