use crate::api::client;
use crate::api::endpoint;
use crate::error::OxenError;
use crate::view::version::VersionResponse;
use crate::view::StatusMessage;

pub async fn get_remote_version(host: &str) -> Result<String, OxenError> {
    let scheme = endpoint::get_scheme(host);
    let url = format!("{scheme}://{host}/api/version");
    log::debug!("Checking version at url {}", url);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        log::debug!("get_remote_version got status: {}", res.status());
        let body = client::parse_json_body(&url, res).await?;
        log::debug!("get_remote_version got body: {}", body);
        let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(val) => Ok(val.oxen_version.unwrap()),
            Err(_) => Err(OxenError::basic_str(format!(
                "api::version::get_remote_version {url} Err parsing response \n\n{body}"
            ))),
        }
    } else {
        let err = format!("api::version::get_remote_version Err request failed: {url}");
        Err(OxenError::basic_str(err))
    }
}

pub async fn get_min_oxen_version(host: &str) -> Result<String, OxenError> {
    let scheme = endpoint::get_scheme(host);
    let url = format!("{scheme}://{host}/api/min_version");
    log::debug!("Checking min cli version at url {}", url);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        log::debug!("get_remote_version got status: {}", res.status());
        let body = client::parse_json_body(&url, res).await?;
        log::debug!("get_remote_version got body: {}", body);
        let response: Result<VersionResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(val) => Ok(val.version),
            Err(_) => Err(OxenError::basic_str(format!(
                "api::version::get_min_oxen_version {url} Err parsing response \n\n{body}"
            ))),
        }
    } else {
        let err = format!("api::version::get_min_oxen_version Err request failed: {url}");
        Err(OxenError::basic_str(err))
    }
}
