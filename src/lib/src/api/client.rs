//! # API Client - For interacting with repositories on a remote machine
//!

use crate::config::runtime_config::runtime::Runtime;
use crate::config::AuthConfig;
use crate::config::RuntimeConfig;
use crate::constants;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::http;
use crate::view::OxenResponse;
pub use reqwest::Url;
use reqwest::{header, Client, ClientBuilder, IntoUrl};
use std::time;

pub mod branches;
pub mod commits;
pub mod compare;
pub mod data_frames;
pub mod diff;
pub mod dir;
pub mod entries;
pub mod file;
pub mod merger;
pub mod metadata;
pub mod notebooks;
pub mod oxen_version;
pub mod repositories;
pub mod revisions;
pub mod schemas;
pub mod stats;
pub mod tree;
pub mod versions;
pub mod workspaces;

const VERSION: &str = crate::constants::OXEN_VERSION;
const USER_AGENT: &str = "Oxen";

pub fn get_scheme_and_host_from_url<U: IntoUrl>(url: U) -> Result<(String, String), OxenError> {
    let parsed_url = url.into_url()?;
    let mut host_str = parsed_url.host_str().unwrap_or_default().to_string();
    if let Some(port) = parsed_url.port() {
        host_str = format!("{host_str}:{port}");
    }
    Ok((parsed_url.scheme().to_owned(), host_str))
}

// TODO: we probably want to create a pool of clients instead of constructing a
// new one for each request so we can take advantage of keep-alive
pub fn new_for_url<U: IntoUrl>(url: U) -> Result<Client, OxenError> {
    let (_scheme, host) = get_scheme_and_host_from_url(url)?;
    new_for_host(host, true)
}

pub fn new_for_url_no_user_agent<U: IntoUrl>(url: U) -> Result<Client, OxenError> {
    let (_scheme, host) = get_scheme_and_host_from_url(url)?;
    new_for_host(host, false)
}

pub fn new_for_url_with_bearer_token<U: IntoUrl>(url: U, bearer_token: &str) -> Result<Client, OxenError> {
    let (_scheme, host) = get_scheme_and_host_from_url(url)?;
    new_for_host_with_bearer_token(host, bearer_token, false)
}

fn new_for_host<S: AsRef<str>>(host: S, should_add_user_agent: bool) -> Result<Client, OxenError> {
    match builder_for_host(host.as_ref(), should_add_user_agent)?
        .timeout(time::Duration::from_secs(constants::DEFAULT_TIMEOUT_SECS))
        .build()
    {
        Ok(client) => Ok(client),
        Err(reqwest_err) => Err(OxenError::HTTP(reqwest_err)),
    }
}

fn new_for_host_with_bearer_token<S: AsRef<str>>(host: S, bearer_token: &str, should_add_user_agent: bool) -> Result<Client, OxenError> {
    match builder_for_host_with_bearer_token(host.as_ref(), bearer_token, should_add_user_agent)?
        .timeout(time::Duration::from_secs(constants::DEFAULT_TIMEOUT_SECS))
        .build()
    {
        Ok(client) => Ok(client),
        Err(reqwest_err) => Err(OxenError::HTTP(reqwest_err)),
    }
}

pub fn new_for_remote_repo(remote_repo: &RemoteRepository) -> Result<Client, OxenError> {
    let (_scheme, host) = get_scheme_and_host_from_url(remote_repo.url())?;
    new_for_host(host, true)
}

pub fn builder_for_remote_repo(remote_repo: &RemoteRepository) -> Result<ClientBuilder, OxenError> {
    let (_scheme, host) = get_scheme_and_host_from_url(remote_repo.url())?;
    builder_for_host(host, true)
}

pub fn builder_for_url<U: IntoUrl>(url: U) -> Result<ClientBuilder, OxenError> {
    let (_scheme, host) = get_scheme_and_host_from_url(url)?;
    builder_for_host(host, true)
}

fn builder_for_host<S: AsRef<str>>(
    host: S,
    should_add_user_agent: bool,
) -> Result<ClientBuilder, OxenError> {
    let builder = if should_add_user_agent {
        builder()
    } else {
        Ok(builder_no_user_agent())
    };

    let config = match AuthConfig::get() {
        Ok(config) => config,
        Err(err) => {
            log::debug!("remote::client::new_for_host error getting config: {}", err);

            return Err(OxenError::must_supply_valid_api_key());
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
        Ok(builder?.default_headers(headers))
    } else {
        log::trace!("No auth token found for host: {}", host.as_ref());
        builder
    }
}

fn builder_for_host_with_bearer_token<S: AsRef<str>>(
    host: S,
    bearer_token: &str,
    should_add_user_agent: bool,
) -> Result<ClientBuilder, OxenError> {
    let builder = if should_add_user_agent {
        builder()
    } else {
        Ok(builder_no_user_agent())
    };

    log::debug!("Setting bearer token for host: {}", host.as_ref());
    let auth_header = format!("Bearer {bearer_token}");
    let mut auth_value = match header::HeaderValue::from_str(auth_header.as_str()) {
        Ok(header) => header,
        Err(err) => {
            log::debug!("remote::client::new invalid header value: {}", err);
            return Err(OxenError::basic_str(
                "Error setting request auth. Please check your bearer token.",
            ));
        }
    };
    auth_value.set_sensitive(true);
    let mut headers = header::HeaderMap::new();
    headers.insert(header::AUTHORIZATION, auth_value);
    Ok(builder?.default_headers(headers))
}

fn builder() -> Result<ClientBuilder, OxenError> {
    let user_agent = build_user_agent()?;
    Ok(Client::builder().user_agent(user_agent))
}

fn builder_no_user_agent() -> ClientBuilder {
    Client::builder()
}

fn build_user_agent() -> Result<String, OxenError> {
    let config = RuntimeConfig::get()?;
    let host_platform = config.host_platform.display_name();

    let runtime_name = match config.runtime_name {
        Runtime::CLI => config.runtime_name.display_name().to_string(),
        _ => format!(
            "{} {}",
            config.runtime_name.display_name(),
            config.runtime_version
        ),
    };

    Ok(format!(
        "{USER_AGENT}/{VERSION} ({host_platform}; {runtime_name})"
    ))
}

/// Performs an extra parse to validate that the response is success
pub async fn parse_json_body(url: &str, res: reqwest::Response) -> Result<String, OxenError> {
    let type_override = "unauthenticated";
    let err_msg = "You are unauthenticated.\n\nObtain an API Key at https://oxen.ai or ask your system admin. Set your auth token with the command:\n\n  oxen config --auth hub.oxen.ai YOUR_AUTH_TOKEN\n";

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

    log::debug!("url: {url}\nstatus: {status}\nbody: {body}");

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

pub async fn handle_non_json_response(
    url: &str,
    res: reqwest::Response,
) -> Result<reqwest::Response, OxenError> {
    if res.status().is_success() || res.status().is_redirection() {
        // If the response is successful, return it as-is. We don't want to do any parsing here.
        return Ok(res);
    }

    // If the response was an error, try to handle it as a standard json response.
    // We assume it's an error here because we checked the success status above.
    Err(parse_json_body(url, res).await.unwrap_err())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_for_url_with_bearer_token() {
        let test_url = "https://test.example.com/api";
        let bearer_token = "test_token_123";
        
        let client = new_for_url_with_bearer_token(test_url, bearer_token);
        assert!(client.is_ok());
    }

    #[test]
    fn test_builder_for_host_with_bearer_token() {
        let host = "test.example.com";
        let bearer_token = "test_token_123";
        
        let builder = builder_for_host_with_bearer_token(host, bearer_token, false);
        assert!(builder.is_ok());
        
        let client = builder.unwrap().build();
        assert!(client.is_ok());
    }

    #[test]
    fn test_builder_for_host_with_bearer_token_sets_auth_header() {
        let host = "test.example.com";
        let bearer_token = "test_token_123";
        
        let builder = builder_for_host_with_bearer_token(host, bearer_token, false);
        assert!(builder.is_ok());
        
        // Build the client and verify it was created successfully
        let client = builder.unwrap().build();
        assert!(client.is_ok());
        
        // The actual header verification would require accessing private fields
        // which isn't directly testable, but we can verify the client builds correctly
    }

    #[test]
    fn test_builder_for_host_with_bearer_token_with_user_agent() {
        let host = "test.example.com";
        let bearer_token = "test_token_123";
        
        let builder = builder_for_host_with_bearer_token(host, bearer_token, true);
        assert!(builder.is_ok());
        
        let client = builder.unwrap().build();
        assert!(client.is_ok());
    }

    #[test]
    fn test_builder_for_host_with_bearer_token_invalid_token() {
        let host = "test.example.com";
        let bearer_token = "invalid\ntoken"; // Invalid token with newline
        
        let builder = builder_for_host_with_bearer_token(host, bearer_token, false);
        assert!(builder.is_err());
        
        let err = builder.unwrap_err();
        assert!(err.to_string().contains("Error setting request auth"));
    }

    #[test]
    fn test_get_scheme_and_host_from_url() {
        let test_cases = vec![
            ("https://example.com", ("https".to_string(), "example.com".to_string())),
            ("http://localhost:8080", ("http".to_string(), "localhost:8080".to_string())),
            ("https://test.example.com:8443", ("https".to_string(), "test.example.com:8443".to_string())),
        ];

        for (url, expected) in test_cases {
            let result = get_scheme_and_host_from_url(url);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), expected);
        }
    }

    #[test]
    fn test_get_scheme_and_host_from_url_invalid() {
        let invalid_url = "not-a-url";
        let result = get_scheme_and_host_from_url(invalid_url);
        assert!(result.is_err());
    }
}
