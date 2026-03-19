//! # API Client - For interacting with repositories on a remote machine
//!

use crate::config::AuthConfig;
use crate::config::RuntimeConfig;
use crate::config::runtime_config::runtime::Runtime;
use crate::constants;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::OxenResponse;
use crate::view::http;
pub use reqwest::Url;
use reqwest::retry;
use reqwest::{Client, ClientBuilder, header};
use std::time;

pub mod branches;
pub mod commits;
pub mod compare;
pub mod data_frames;
pub mod diff;
pub mod dir;
pub mod entries;
pub mod export;
pub mod file;
pub mod import;
pub(crate) mod internal_types;
pub mod merger;
pub mod metadata;
pub mod oxen_version;
pub mod prune;
pub mod repositories;
pub mod revisions;
pub mod schemas;
pub mod stats;
pub mod tree;
pub mod versions;
pub mod workspaces;

const VERSION: &str = crate::constants::OXEN_VERSION;
const USER_AGENT: &str = "Oxen";

pub fn get_scheme_and_host_from_url(url: &str) -> Result<(String, String), OxenError> {
    let parsed_url = Url::parse(url)?;
    let mut host_str = parsed_url.host_str().unwrap_or_default().to_string();
    if let Some(port) = parsed_url.port() {
        host_str = format!("{host_str}:{port}");
    }
    Ok((parsed_url.scheme().to_owned(), host_str))
}

// TODO: we probably want to create a pool of clients instead of constructing a
// new one for each request so we can take advantage of keep-alive
pub fn new_for_url(url: &str) -> Result<Client, OxenError> {
    let (_scheme, host) = get_scheme_and_host_from_url(url)?;
    new_for_host(host, true)
}

pub fn new_for_url_no_user_agent(url: &str) -> Result<Client, OxenError> {
    let (_scheme, host) = get_scheme_and_host_from_url(url)?;
    new_for_host(host, false)
}

fn new_for_host(host: String, should_add_user_agent: bool) -> Result<Client, OxenError> {
    match builder_for_host(host, should_add_user_agent)?
        .timeout(time::Duration::from_secs(constants::timeout()))
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

pub fn builder_for_url(url: &str) -> Result<ClientBuilder, OxenError> {
    let (_scheme, host) = get_scheme_and_host_from_url(url)?;
    builder_for_host(host, true)
}

fn builder_for_host(host: String, should_add_user_agent: bool) -> Result<ClientBuilder, OxenError> {
    let mut builder = Client::builder();
    if should_add_user_agent {
        let config = RuntimeConfig::get()?;
        builder = builder.user_agent(build_user_agent(&config));
    }

    // Bump max retries for this oxen-server host from 2 to 3. Exponential backoff is used by default.
    let retry_policy = retry::for_host(host.clone())
        .max_retries_per_request(3)
        .classify_fn(|req_rep| {
            // Still retry on low-level network errors
            if req_rep.error().is_some() {
                return req_rep.retryable();
            }
            // Have reqwest retry all application-level server errors*, not just network-level errors
            // that reqwest considers retryable by default. This assumes that oxen-server endpoints are
            // safe to retry if the server returned any error mid-operation. We can tighten this up
            // to only retry specific server errors in the future if that is not true.
            //
            // * info (100's), success (200's), redirection (300's), and client errors (400's)
            //   don't make sense to retry. We'll only retry server errors (500's).
            match req_rep.status() {
                Some(status_code) if status_code.is_server_error() => req_rep.retryable(), // retry
                _ => req_rep.success(), // this means don't retry, and is the only other valid return value from the closure
            }
        });
    builder = builder.retry(retry_policy);

    // If auth_config.toml isn't found, return without authorizing
    let config = match AuthConfig::get() {
        Ok(config) => config,
        Err(e) => {
            log::debug!(
                "Error getting config: {}. No auth token found for host {}",
                e,
                host
            );
            return Ok(builder);
        }
    };
    if let Some(auth_token) = config.auth_token_for_host(host.as_str()) {
        log::debug!("Setting auth token for host: {}", host);
        let auth_header = format!("Bearer {auth_token}");
        let mut auth_value = match header::HeaderValue::from_str(auth_header.as_str()) {
            Ok(header) => header,
            Err(e) => {
                log::debug!("Invalid header value: {e}");
                return Err(OxenError::basic_str(
                    "Error setting request auth. Please check your Oxen config.",
                ));
            }
        };
        auth_value.set_sensitive(true);
        let mut headers = header::HeaderMap::new();
        headers.insert(header::AUTHORIZATION, auth_value);
        builder = builder.default_headers(headers);
    } else {
        log::trace!("No auth token found for host: {}", host);
    }
    Ok(builder)
}

fn build_user_agent(config: &RuntimeConfig) -> String {
    let host_platform = config.host_platform.display_name();
    let runtime_name = match config.runtime_name {
        Runtime::CLI => config.runtime_name.display_name().to_string(),
        _ => format!(
            "{} {}",
            config.runtime_name.display_name(),
            config.runtime_version
        ),
    };
    format!("{USER_AGENT}/{VERSION} ({host_platform}; {runtime_name})")
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
                log::debug!("Error getting config: {err}");
                return Err(OxenError::must_supply_valid_api_key());
            }
        };
    }

    parse_json_body_with_err_msg(url, res, Some(type_override), Some(err_msg)).await
}

/// Used to override error message when parsing json body
async fn parse_json_body_with_err_msg(
    url: &str,
    res: reqwest::Response,
    response_type: Option<&str>,
    response_msg_override: Option<&str>,
) -> Result<String, OxenError> {
    let status = res.status();
    let body = res.text().await?;

    log::debug!("url: {url}\nstatus: {status}");

    let response: Result<OxenResponse, serde_json::Error> = serde_json::from_str(&body);
    log::debug!("response: {response:?}");
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
            log::debug!("Err: {err}");
            Err(OxenError::basic_str(format!(
                "Could not deserialize response from [{url}]\n{status}"
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

            if let Some(msg) = response_msg_override
                && let Some(response_type) = response_type
                && response.desc_or_msg() == response_type
            {
                return Err(OxenError::basic_str(msg));
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
