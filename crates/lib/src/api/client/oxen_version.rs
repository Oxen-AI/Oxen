use crate::api::client;
use crate::error::OxenError;
use crate::view::oxen_version::OxenVersionResponse;

pub async fn get_remote_version(scheme: &str, host: &str) -> Result<String, OxenError> {
    let url = format!("{scheme}://{host}/api/version");
    log::debug!("Checking version at url {url}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    log::debug!("get_remote_version got status: {}", res.status());
    let body = client::parse_json_body(&url, res).await?;
    log::debug!("get_remote_version got body: {body}");
    let response: OxenVersionResponse = serde_json::from_str(&body)?;
    Ok(response.version)
}

pub async fn get_min_oxen_version(scheme: &str, host: &str) -> Result<String, OxenError> {
    let url = format!("{scheme}://{host}/api/min_version");
    log::debug!("Checking min cli version at url {url}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    log::debug!("get_min_oxen_version got status: {}", res.status());
    let body = client::parse_json_body(&url, res).await?;
    log::debug!("get_min_oxen_version got body: {body}");
    let response: OxenVersionResponse = serde_json::from_str(&body)?;
    Ok(response.version)
}
