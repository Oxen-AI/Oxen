use regex::Regex;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::LazyLock;

use liboxen::error::OxenError;
use liboxen::model::{Branch, Commit, LocalRepository, ParsedResource};
use liboxen::resource::{parse_resource_from_path, parse_resource_from_path_async};
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
            constants::MIN_OXEN_CLIENT_VERSION.into(),
        ));
    }

    req.app_data::<OxenAppData>()
        .ok_or(OxenHttpError::AppDataDoesNotExist)
}

fn get_app_data(req: &HttpRequest) -> Result<&OxenAppData, OxenHttpError> {
    req.app_data::<OxenAppData>()
        .ok_or(OxenHttpError::AppDataDoesNotExist)
}

/// Dynamically access a path parameter by name.
///
/// When the `otel` feature is enabled, records the parameter as an OpenTelemetry
/// attribute (`http.path.{param}`) on the current span. This bypasses the tracing
/// field system and adds tags directly to the OTel span, so the values appear in
/// OTel collectors (e.g. Jaeger) but **not** in stderr/file log events for spans.
pub fn path_param<'a>(request: &'a HttpRequest, param: &str) -> Result<&'a str, OxenHttpError> {
    let value = request
        .match_info()
        .get(param)
        .ok_or_else(|| OxenHttpError::PathParamDoesNotExist(param.into()))?;

    #[cfg(feature = "otel")]
    {
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        // TODO: Replace this dynamic approach with statically typed `web::Path<T>` extractors
        tracing::Span::current().set_attribute(format!("http.path.{param}"), value.to_string());
    }

    Ok(value)
}

/// Dynamically accesses a query parameter by name.
///
/// Unlike path params, this returns an empty string if the query parameter is not found in the request.
///
/// When the `otel` feature is enabled, records the parameter as an OpenTelemetry
/// attribute (`http.query.{param}`) on the current span. This bypasses the tracing
/// field system and adds tags directly to the OTel span, so the values appear in
/// OTel collectors (e.g. Jaeger) but **not** in stderr/file log events for spans.
pub fn query_param<'a>(request: &'a HttpRequest, param: &str) -> &'a str {
    let value = request.match_info().query(param);

    #[cfg(feature = "otel")]
    {
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        // TODO: Replace this dynamic approach with statically typed `web::Query<T>` extractors
        tracing::Span::current().set_attribute(format!("http.query.{param}"), value.to_string());
    }

    value
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
    let resource: PathBuf = PathBuf::from(query_param(req, "resource"));
    let resource_path_str = resource.to_string_lossy();

    // Decode the URL, handling both %20 and + as spaces
    let decoded_path = decode_resource_path(&resource_path_str);

    let decoded_resource = PathBuf::from(decoded_path);
    log::debug!(
        "parse_resource_from_path looking for resource: {resource:?} decoded_resource: {decoded_resource:?}"
    );
    parse_resource_from_path(repo, &decoded_resource)?
        .ok_or_else(|| OxenError::path_does_not_exist(resource).into())
}

/// Resolve the `resource` query param against `repo`, off the async worker.
pub async fn parse_resource_async(
    req: &HttpRequest,
    repo: &LocalRepository,
) -> Result<ParsedResource, OxenHttpError> {
    let resource: PathBuf = PathBuf::from(query_param(req, "resource"));
    let resource_path_str = resource.to_string_lossy();

    // Decode the URL, handling both %20 and + as spaces
    let decoded_path = decode_resource_path(&resource_path_str);

    let decoded_resource = PathBuf::from(decoded_path);
    log::debug!(
        "parse_resource_from_path looking for resource: {resource:?} decoded_resource: {decoded_resource:?}"
    );
    parse_resource_from_path_async(repo, &decoded_resource)
        .await?
        .ok_or_else(|| OxenError::path_does_not_exist(resource).into())
}

/// Split a `base..head` or `base...head` string into base, head, and whether it
/// used three-dot syntax. Three dots select the merge base of base and head
/// (git's `base...head`); two dots compare the two revisions directly.
pub fn parse_base_head(base_head: &str) -> Result<(String, String, bool), OxenError> {
    // Check for the three-dot separator first, since it contains the two-dot one.
    let (separator, three_dot) = if base_head.contains("...") {
        ("...", true)
    } else {
        ("..", false)
    };
    let mut split = base_head.splitn(2, separator);
    if let (Some(base), Some(head)) = (split.next(), split.next()) {
        Ok((base.to_string(), head.to_string(), three_dot))
    } else {
        Err(OxenError::basic_str(
            "Could not parse commits. Format should be base..head or base...head",
        ))
    }
}

/// Split a `base..head` string into base and head, rejecting `base...head`. For callers that
/// compare the two revisions directly and have no merge-base behavior to offer, so that a three-dot
/// request is answered rather than silently treated as two-dot.
pub fn parse_two_dot(base_head: &str) -> Result<(String, String), OxenHttpError> {
    let (base, head, three_dot) = parse_base_head(base_head)?;
    if three_dot {
        return Err(OxenHttpError::BadRequest(
            format!("Three-dot syntax is not supported here, use {base}..{head}").into(),
        ));
    }
    Ok((base, head))
}

/// Split an optional `base..head` range. A bare revision yields no head. Three-dot syntax is
/// rejected, as in [`parse_two_dot`].
pub fn maybe_parse_two_dot(base_head: &str) -> Result<(String, Option<String>), OxenHttpError> {
    if !base_head.contains("..") {
        return Ok((base_head.to_string(), None));
    }
    let (base, head) = parse_two_dot(base_head)?;
    Ok((base, Some(head)))
}

/// The commit a diff should compare from. Three dots select the merge base of the two revisions
/// (git's `base...head`), so commits that landed on base after the fork don't read as part of
/// head's changes.
pub async fn resolve_diff_base(
    repo: &LocalRepository,
    base_commit: Commit,
    head_commit: &Commit,
    three_dot: bool,
) -> Result<Commit, OxenHttpError> {
    if !three_dot {
        return Ok(base_commit);
    }
    // Sync core: the merge base walks both commit histories, so keep it off the actix worker.
    let repo = repo.clone();
    let head_commit = head_commit.clone();
    let merge_base = tokio::task::spawn_blocking(move || -> Result<Commit, OxenError> {
        repositories::merge::lowest_common_ancestor_from_commits(&repo, &base_commit, &head_commit)?
            .ok_or_else(|| OxenError::NoMergeBase {
                base: base_commit.id.clone(),
                head: head_commit.id.clone(),
            })
    })
    .await
    .map_err(OxenError::from)??;
    Ok(merge_base)
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
    match repositories::branches::get_by_name(repo, name) {
        Ok(branch) => Ok(Some(branch)),
        Err(OxenError::BranchNotFound(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Whether the User-Agent identifies an Oxen client (not Postman, not some other tool).
fn is_oxen_user_agent(user_agent: &str) -> bool {
    // Bypass for postman requests - TODO, make this more robust or only in dev
    if user_agent.contains("Postman") {
        return false;
    }
    user_agent.to_lowercase().contains("oxen")
}

/// Parse the client's Oxen version from its User-Agent. Returns `None` when the agent is not an
/// Oxen client or its version can't be parsed.
fn client_oxen_version(user_agent: &str) -> Option<OxenVersion> {
    if !is_oxen_user_agent(user_agent) {
        return None;
    }

    let parts: Vec<&str> = user_agent.split('/').collect();
    let version = REGEX_USER_AGENT_VERSION_NUMBER
        .find(parts.get(1)?)
        .map(|m| m.as_str())?;
    OxenVersion::from_str(version).ok()
}

fn user_cli_is_out_of_date(user_agent: &str) -> bool {
    if !is_oxen_user_agent(user_agent) {
        // Not an oxen user agent; nothing to gate.
        return false;
    }

    // An oxen client whose version can't be parsed is treated as out of date.
    let Some(user_cli_version) = client_oxen_version(user_agent) else {
        return true;
    };

    let min_oxen_version = match OxenVersion::from_str(constants::MIN_OXEN_CLIENT_VERSION) {
        Ok(v) => v,
        Err(_) => return true,
    };

    min_oxen_version > user_cli_version
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_oxen_version_parses_oxen_agent() {
        let version = client_oxen_version("Oxen/0.51.0 (macos; tokio)");
        assert_eq!(version, Some(OxenVersion::from_str("0.51.0").unwrap()));
    }

    #[test]
    fn test_client_oxen_version_ignores_non_oxen_agent() {
        assert_eq!(client_oxen_version("Mozilla/5.0 (browser)"), None);
        assert_eq!(client_oxen_version("PostmanRuntime/7.0.0"), None);
    }

    #[test]
    fn test_parse_base_head_two_dots() {
        let (base, head, three_dot) = parse_base_head("main..feature").unwrap();
        assert_eq!(base, "main");
        assert_eq!(head, "feature");
        assert!(!three_dot);
    }

    #[test]
    fn test_parse_base_head_three_dots() {
        let (base, head, three_dot) = parse_base_head("main...feature").unwrap();
        assert_eq!(base, "main");
        assert_eq!(head, "feature");
        assert!(three_dot);
    }

    /// Builds `base` -> `main_tip` on main and `base` -> `feature_tip` on a branch, so the merge
    /// base of the two tips is the fork point rather than either tip.
    async fn forked_repo(repo: &LocalRepository) -> Result<(Commit, Commit, Commit), OxenError> {
        let main = repositories::branches::current_branch(repo)?.unwrap();
        let base_path = repo.path.join("base.txt");
        liboxen::util::fs::write_to_path(&base_path, "base")?;
        repositories::add(repo, &base_path).await?;
        let fork_point = repositories::commit(repo, "fork point")?;

        repositories::branches::create_checkout(repo, "feature")?;
        let feature_path = repo.path.join("feature.txt");
        liboxen::util::fs::write_to_path(&feature_path, "feature")?;
        repositories::add(repo, &feature_path).await?;
        let feature_tip = repositories::commit(repo, "feature work")?;

        repositories::checkout(repo, &main.name).await?;
        let main_path = repo.path.join("main.txt");
        liboxen::util::fs::write_to_path(&main_path, "main")?;
        repositories::add(repo, &main_path).await?;
        let main_tip = repositories::commit(repo, "main work")?;

        Ok((fork_point, main_tip, feature_tip))
    }

    #[tokio::test]
    async fn test_resolve_diff_base_two_dots_keeps_the_base_tip() -> Result<(), OxenError> {
        liboxen::test::run_one_commit_local_repo_test_async(|repo| async move {
            let (_, main_tip, feature_tip) = forked_repo(&repo).await?;
            let resolved = resolve_diff_base(&repo, main_tip.clone(), &feature_tip, false)
                .await
                .unwrap();
            assert_eq!(resolved.id, main_tip.id);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_resolve_diff_base_three_dots_uses_the_fork_point() -> Result<(), OxenError> {
        liboxen::test::run_one_commit_local_repo_test_async(|repo| async move {
            let (fork_point, main_tip, feature_tip) = forked_repo(&repo).await?;
            // Without this the diff would carry main's post-fork commits as if they were
            // feature's changes.
            let resolved = resolve_diff_base(&repo, main_tip.clone(), &feature_tip, true)
                .await
                .unwrap();
            assert_eq!(resolved.id, fork_point.id);
            assert_ne!(resolved.id, main_tip.id);
            Ok(())
        })
        .await
    }

    #[test]
    fn test_parse_two_dot_accepts_two_dots() {
        let (base, head) = parse_two_dot("main..feature").unwrap();
        assert_eq!(base, "main");
        assert_eq!(head, "feature");
    }

    #[test]
    fn test_parse_two_dot_rejects_three_dots() {
        // Endpoints that compare the revisions directly have no merge-base behavior to offer, so
        // a three-dot request has to be answered rather than quietly downgraded.
        let err = parse_two_dot("main...feature").unwrap_err();
        assert!(
            matches!(err, OxenHttpError::BadRequest(_)),
            "expected a bad request, got {err:?}"
        );
    }

    #[test]
    fn test_parse_base_head_does_not_leak_dot_into_head() {
        // The three-dot separator must be matched before the two-dot one, or the
        // extra dot bleeds into head as ".feature".
        let (_, head, _) = parse_base_head("main...feature").unwrap();
        assert_eq!(head, "feature");
    }
}
