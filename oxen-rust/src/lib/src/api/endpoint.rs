//! # Endpoint - Helpers for creating urls for the remote API
//!

use crate::error::OxenError;
use crate::model::{Remote, RemoteRepository, RepoNew};
use url::Url;

pub const API_NAMESPACE: &str = "/api/repos";

pub fn get_scheme(host: impl AsRef<str>) -> String {
    RepoNew::scheme_default(host.as_ref())
}

pub fn url_from_host_and_scheme(
    host: impl AsRef<str>,
    uri: impl AsRef<str>,
    scheme: impl AsRef<str>,
) -> String {
    format!(
        "{}://{}{API_NAMESPACE}{}",
        scheme.as_ref(),
        host.as_ref(),
        uri.as_ref()
    )
}

pub fn url_from_host(host: &str, uri: &str) -> String {
    format!("{}://{host}{API_NAMESPACE}{uri}", get_scheme(host))
}

pub fn remote_url_from_namespace_name(host: &str, namespace: &str, name: &str) -> String {
    format!("{}://{host}/{namespace}/{name}", get_scheme(host))
}

pub fn remote_url_from_namespace_name_scheme(
    host: &str,
    namespace: &str,
    name: &str,
    scheme: &str,
) -> String {
    format!("{scheme}://{host}/{namespace}/{name}")
}

pub fn remote_url_from_name(host: &str, name: &str) -> String {
    format!("{}://{host}/{name}", get_scheme(host))
}

pub fn remote_url_from_name_and_scheme(host: &str, name: &str, scheme: &str) -> String {
    format!("{scheme}://{host}/{name}")
}

pub fn url_from_remote_url(url: &str) -> Result<String, OxenError> {
    log::debug!("creating url_from_remote_url {url:?}");
    match Url::parse(url) {
        Ok(mut parsed_url) => {
            let new_path = format!("{}{}", API_NAMESPACE, parsed_url.path());
            parsed_url.set_path(&new_path);
            Ok(parsed_url.to_string())
        }
        Err(e) => {
            log::warn!("Invalid remote url: {url:?}\n{e:?}");
            Err(OxenError::invalid_set_remote_url(url))
        }
    }
}

pub fn url_from_remote(remote: &Remote, uri: &str) -> Result<String, OxenError> {
    // log::info!("url_from_remote creating url_from_remote {remote:?} -> {uri:?}");
    match Url::parse(&remote.url) {
        Ok(mut parsed_url) => {
            // TODO: this is a workaround because to_string was URL encoding characters that we didn't want encoded
            // log::info!("url_from_remote parsed_url: {}", parsed_url);
            let new_path = format!("{}{}{}", API_NAMESPACE, parsed_url.path(), uri);

            parsed_url.set_path("");
            // log::info!("url_from_remote parsed_url after set path: {}", parsed_url);

            let mut remote_url = parsed_url.to_string();
            remote_url.pop(); // to_string adds a trailing slash we don't want
                              // log::info!("url_from_remote new_path: {}", new_path);
                              // log::info!("url_from_remote remote_url: {}", remote_url);
            Ok(format!("{remote_url}{new_path}"))
        }
        Err(e) => {
            log::warn!("Invalid remote url: {:?}\n{:?}", remote.url, e);
            Err(OxenError::invalid_set_remote_url(&remote.url))
        }
    }
}

pub fn url_from_repo(repo: &RemoteRepository, uri: &str) -> Result<String, OxenError> {
    url_from_remote(&repo.remote, uri)
}
