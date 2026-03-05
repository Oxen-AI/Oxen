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

pub struct ClientConfig {

  /// Overall timeout for the entire request (connect + send + response).
  pub timeout: time::Duration,

  /// Timeout for establishing the TCP connection only.
  pub connect_timeout: time::Duration,

  /// Timeout for individual read operations on the response body (added in 0.12+).
  pub read_timeout: time::Duration,

  /// How long idle connections stay in the pool.
  pub pool_idle_timeout: time::Duration,

  /// The client's maximum number of retries for failed requests.
  pub max_retries: usize,

  /// Whether the client should add the user agent to requests.
  pub should_add_user_agent: bool,

}

/// Parses the URL into a scheme and hostname pair. Hostname includes port if specified.
pub fn get_scheme_and_host_from_url<U: IntoUrl>(url: U) -> Result<(String, String), OxenError> {
    let parsed_url = url.into_url()?;
    let host = parsed_url.host_str().unwrap_or_default().to_string();
    let hostname = if let Some(port) = parsed_url.port() {
        format!("{host}:{port}");
    } else {
      host
    };
    Ok((parsed_url.scheme().to_owned(), hostname))
}

// TODO: we probably want to create a pool of clients instead of constructing a
// new one for each request so we can take advantage of keep-alive
pub fn new_for_url<U: IntoUrl>(url: U, config: &ClientConfig) -> Result<Client, OxenError> {
    let (_scheme, host) = get_scheme_and_host_from_url(url)?;
    new_for_host(host, config)
}

pub fn new_for_url_no_user_agent<U: IntoUrl>(url: U, config: &ClientConfig) -> Result<Client, OxenError> {
    let (_scheme, host) = get_scheme_and_host_from_url(url)?;
    new_for_host(host, config)
}

fn new_for_host<S: AsRef<str>>(host: S, config: &ClientConfig) -> Result<Client, OxenError> {
    match builder_for_host(host.as_ref(), config.should_add_user_agent)?
        .timeout(config.timeout)
        .build()
    {
        Ok(client) => Ok(client),
        Err(reqwest_err) => Err(reqwest_err.into()),
    }
}

pub fn builder_for_url<U: IntoUrl>(url: U, config: &ClientConfig) -> Result<ClientBuilder, OxenError> {
    let (_, host) = get_scheme_and_host_from_url(url)?;
    builder_for_host(&host, config)
}

fn builder_for_host(host: &str, config: &ClientConfig) -> Result<ClientBuilder, OxenError> {

    let mut b = builder(config)?;

    if config.should_add_user_agent {
      b = b.user_agent(build_user_agent()?);
    }

    // If auth_config.toml isn't found, return without authorizing
    match AuthConfig::get() {
        Ok(config) => {
          if let Some(auth_token) = config.auth_token_for_host(host) {
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
              b = b.default_headers(headers);
          } else {
              log::trace!("No auth token found for host: {}", host.as_ref());
          }
        },
        Err(e) => {
          log::debug!(
              "Error getting config: {}. No auth token found for host {}",
              e,
              host.as_ref()
          );
        },
    };

    b = b.timeout(config.timeout);

    b = b.connect_timeout(config.connect_timeout);

    b = b.read_timeout(config.read_timeout);

    Ok(b)
}

fn builder(config: &ClientConfig) -> Result<ClientBuilder, OxenError> {




    Ok(b)
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
