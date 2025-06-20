use regex::Regex;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::LazyLock;

use liboxen::error::OxenError;
use liboxen::model::{Branch, Commit, LocalRepository, ParsedResource};
use liboxen::resource::parse_resource_from_path;
use liboxen::{constants, repositories};

use actix_web::HttpRequest;
use liboxen::util::oxen_version::OxenVersion;

use crate::app_data::OxenAppData;
use crate::errors::OxenHttpError;
use percent_encoding::percent_decode;

pub mod aggregate_query;
pub use aggregate_query::AggregateQuery;

pub mod name_param;
pub use name_param::NameParam;

pub mod page_num_query;
pub use page_num_query::PageNumQuery;
pub use page_num_query::PageNumVersionQuery;

pub mod df_opts_query;
pub use df_opts_query::DFOptsQuery;

pub mod tree_depth;
pub use tree_depth::TreeDepthQuery;

static REGEX_USER_AGENT_VERSION_NUMBER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d+\.\d+\.\d+").unwrap());

pub fn app_data(req: &HttpRequest) -> Result<&OxenAppData, OxenHttpError> {
    log::debug!(
        "Get user agent from app data (app_data) {:?}",
        req.headers().get("user-agent")
    );

    log::debug!(
        "Request URL: {:?}, Query: {:?}",
        req.uri(),
        req.query_string()
    );

    let user_agent = req.headers().get("user-agent");
    let Some(user_agent) = user_agent else {
        // No user agent, so we can't check the version
        return get_app_data(req);
    };

    let Ok(user_agent_str) = user_agent.to_str() else {
        // Invalid user agent, so we can't check the version
        return get_app_data(req);
    };

    if user_cli_is_out_of_date(user_agent_str) {
        return Err(OxenHttpError::UpdateRequired(
            constants::MIN_OXEN_VERSION.as_str().into(),
        ));
    }

    req.app_data::<OxenAppData>()
        .ok_or(OxenHttpError::AppDataDoesNotExist)
}

fn get_app_data(req: &HttpRequest) -> Result<&OxenAppData, OxenHttpError> {
    req.app_data::<OxenAppData>()
        .ok_or(OxenHttpError::AppDataDoesNotExist)
}

pub fn path_param(req: &HttpRequest, param: &str) -> Result<String, OxenHttpError> {
    Ok(req
        .match_info()
        .get(param)
        .ok_or(OxenHttpError::PathParamDoesNotExist(param.into()))?
        .to_string())
}

pub fn path_param_to_vec(
    req: &HttpRequest,
    param_name: &str,
) -> Result<Vec<String>, OxenHttpError> {
    let param_value = path_param(req, param_name)?;
    let values: Vec<String> = param_value.split(',').map(|s| s.to_string()).collect();
    Ok(values)
}

fn decode_resource_path(resource_path_str: &str) -> String {
    percent_decode(resource_path_str.as_bytes())
        .decode_utf8_lossy()
        .into_owned()
}

pub fn parse_resource(
    req: &HttpRequest,
    repo: &LocalRepository,
) -> Result<ParsedResource, OxenHttpError> {
    let resource: PathBuf = PathBuf::from(req.match_info().query("resource"));
    let resource_path_str = resource.to_string_lossy();

    // Decode the URL, handling both %20 and + as spaces
    let decoded_path = decode_resource_path(&resource_path_str);

    let decoded_resource = PathBuf::from(decoded_path);
    log::debug!(
        "parse_resource_from_path looking for resource: {:?} decoded_resource: {:?}",
        resource,
        decoded_resource
    );
    parse_resource_from_path(repo, &decoded_resource)?
        .ok_or(OxenError::path_does_not_exist(resource).into())
}

/// Split the base..head string into base and head strings
pub fn parse_base_head(base_head: impl AsRef<str>) -> Result<(String, String), OxenError> {
    let mut split = base_head.as_ref().split("..");
    if let (Some(base), Some(head)) = (split.next(), split.next()) {
        Ok((base.to_string(), head.to_string()))
    } else {
        Err(OxenError::basic_str(
            "Could not parse commits. Format should be base..head",
        ))
    }
}

pub fn resolve_base_head_branches(
    repo: &LocalRepository,
    base: &str,
    head: &str,
) -> Result<(Option<Branch>, Option<Branch>), OxenError> {
    let base = resolve_branch(repo, base)?;
    let head = resolve_branch(repo, head)?;
    Ok((base, head))
}

/// Resolve the commits from the base and head strings (which can be either commit ids or branch names)
pub fn resolve_base_head(
    repo: &LocalRepository,
    base: &str,
    head: &str,
) -> Result<(Option<Commit>, Option<Commit>), OxenError> {
    let base = resolve_revision(repo, base)?;
    let head = resolve_revision(repo, head)?;
    Ok((base, head))
}

pub fn resolve_revision(
    repo: &LocalRepository,
    revision: &str,
) -> Result<Option<Commit>, OxenError> {
    // Lookup commit by id or branch name
    repositories::revisions::get(repo, revision)
}

pub fn resolve_branch(repo: &LocalRepository, name: &str) -> Result<Option<Branch>, OxenError> {
    // Lookup branch name
    repositories::branches::get_by_name(repo, name)
}

fn user_cli_is_out_of_date(user_agent: &str) -> bool {
    // Bypass for postman requests - TODO, make this more robust or only in dev
    if user_agent.contains("Postman") {
        return false;
    }

    // check if the user agent contains oxen
    if !user_agent.to_lowercase().contains("oxen") {
        // Not an oxen user agent
        return false;
    }

    // And if the version is less than the minimum version
    let parts: Vec<&str> = user_agent.split('/').collect();

    if parts.len() <= 1 {
        // Can't parse version from user agent
        return true;
    }

    let Some(version) = REGEX_USER_AGENT_VERSION_NUMBER
        .find(parts[1])
        .map(|m| m.as_str())
    else {
        // Can't parse version from user agent
        return true;
    };

    let user_cli_version = match OxenVersion::from_str(version) {
        Ok(v) => v,
        Err(_) => return true,
    };

    let min_oxen_version = match OxenVersion::from_str(constants::MIN_OXEN_VERSION.as_str()) {
        Ok(v) => v,
        Err(_) => return true,
    };

    if min_oxen_version > user_cli_version {
        return true;
    }
    false
}
