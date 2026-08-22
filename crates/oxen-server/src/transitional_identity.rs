//! Identity a control plane states alongside a request.
//!
//! Where a control plane owns namespaces it passes UUIDs in both name positions of the URL, so a
//! repository's human-readable names reach the server only in these headers. A server that owns
//! its own namespaces is given real names in the URL and needs none of this. The headers are
//! transitional; see `docs/deprecations.md`.

use std::future::Future;

use actix_web::HttpRequest;
use actix_web::http::header::HeaderMap;
use liboxen::error::StringError;
use uuid::Uuid;

use crate::errors::OxenHttpError;

pub const REPO_UUID_HEADER: &str = "x-oxen-transitional-repo-uuid";
pub const REPO_NAME_HEADER: &str = "x-oxen-transitional-repo-name";
pub const NAMESPACE_HEADER: &str = "x-oxen-transitional-namespace";

/// What a request states about the repository it addresses, over and above the URL.
///
/// Every field is absent unless the caller sent it, so a request carrying none of these is
/// indistinguishable from one made before the headers existed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransitionalIdentity {
    /// The repository's immutable UUID, which the URL cannot carry while it addresses by name.
    pub repo_uuid: Option<Uuid>,
    /// The namespace's human-readable name.
    pub namespace: Option<String>,
    /// The repository's human-readable name.
    pub name: Option<String>,
}

/// What the request being handled states about the repository it addresses, and whether it may
/// change anything.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatedRequest {
    pub identity: TransitionalIdentity,
    /// Hints are refreshed on writes only: doing it on a read path is a write during a GET.
    pub is_write: bool,
}

tokio::task_local! {
    /// Bound once per request by `TransitionalIdentityMiddleware`, so a repository lookup can act
    /// on it without every call site threading the request through.
    static STATED: StatedRequest;
}

/// What the request being handled states, or nothing outside a request.
pub fn stated_request() -> StatedRequest {
    STATED.try_with(Clone::clone).unwrap_or_default()
}

/// Run `future` with `stated` as what the request states.
pub fn with_stated_request<F: Future>(
    stated: StatedRequest,
    future: F,
) -> impl Future<Output = F::Output> {
    STATED.scope(stated, future)
}

impl TransitionalIdentity {
    /// Read the transitional identity headers off a request.
    ///
    /// # Errors
    /// [`OxenHttpError::BadRequest`] when a header is present but unusable: a repo UUID that is not
    /// a UUID, a name that is empty, or any of the three that is not valid UTF-8. A malformed
    /// header is refused rather than ignored, so a caller that meant to state identity is told it
    /// failed instead of silently getting the behavior of having sent nothing.
    pub fn from_request(req: &HttpRequest) -> Result<Self, OxenHttpError> {
        Self::from_headers(req.headers())
    }

    /// Read the transitional identity headers off a header map.
    ///
    /// # Errors
    /// Same as [`Self::from_request`].
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, OxenHttpError> {
        Ok(TransitionalIdentity {
            repo_uuid: header_str(headers, REPO_UUID_HEADER)?
                .map(|raw| {
                    Uuid::parse_str(raw).map_err(|_| {
                        OxenHttpError::BadRequest(StringError::from(format!(
                            "{REPO_UUID_HEADER} is not a UUID"
                        )))
                    })
                })
                .transpose()?,
            namespace: header_str(headers, NAMESPACE_HEADER)?.map(str::to_string),
            name: header_str(headers, REPO_NAME_HEADER)?.map(str::to_string),
        })
    }

    /// Whether the request stated nothing at all.
    pub fn is_empty(&self) -> bool {
        self == &TransitionalIdentity::default()
    }

    /// Check that `recorded` is the repository this request says it is addressing.
    ///
    /// # Errors
    /// [`OxenHttpError::BadRequest`] when the request states a repo UUID and the repository found
    /// carries a different one. Serving it anyway would act on a repository the caller did not
    /// address, under a name that happened to resolve to it.
    pub fn check_addresses(&self, recorded: Option<Uuid>) -> Result<(), OxenHttpError> {
        match (self.repo_uuid, recorded) {
            (Some(stated), Some(recorded)) if stated != recorded => {
                Err(OxenHttpError::BadRequest(StringError::from(format!(
                    "{REPO_UUID_HEADER} names a different repository than the one addressed"
                ))))
            }
            _ => Ok(()),
        }
    }
}

/// The header's value, absent when the header is, and an error when it cannot be used as a value.
fn header_str<'a>(headers: &'a HeaderMap, header: &str) -> Result<Option<&'a str>, OxenHttpError> {
    let Some(value) = headers.get(header) else {
        return Ok(None);
    };
    let bad =
        |reason: &str| OxenHttpError::BadRequest(StringError::from(format!("{header} {reason}")));
    let value = value.to_str().map_err(|_| bad("is not valid UTF-8"))?;
    if value.is_empty() {
        return Err(bad("is empty"));
    }
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test::TestRequest;

    #[test]
    fn a_request_with_no_headers_states_nothing() {
        let identity =
            TransitionalIdentity::from_request(&TestRequest::default().to_http_request())
                .expect("no headers is not an error");
        assert!(identity.is_empty());
    }

    #[test]
    fn all_three_headers_are_read() {
        let repo_uuid = Uuid::new_v4();
        let req = TestRequest::default()
            .insert_header((REPO_UUID_HEADER, repo_uuid.to_string()))
            .insert_header((NAMESPACE_HEADER, "ox"))
            .insert_header((REPO_NAME_HEADER, "cats"))
            .to_http_request();

        let identity = TransitionalIdentity::from_request(&req).expect("well-formed headers parse");

        assert_eq!(identity.repo_uuid, Some(repo_uuid));
        assert_eq!(identity.namespace.as_deref(), Some("ox"));
        assert_eq!(identity.name.as_deref(), Some("cats"));
        assert!(!identity.is_empty());
    }

    /// Storage and index keys compare UUIDs as strings, so an unhyphenated one still has to arrive
    /// at the canonical form.
    #[test]
    fn a_repo_uuid_is_canonicalized_whatever_form_arrived() {
        let req = TestRequest::default()
            .insert_header((REPO_UUID_HEADER, "5abd211ee25c494bba0f44ad542443d7"))
            .to_http_request();

        let identity =
            TransitionalIdentity::from_request(&req).expect("an unhyphenated UUID parses");

        assert_eq!(
            identity.repo_uuid.map(|uuid| uuid.to_string()).as_deref(),
            Some("5abd211e-e25c-494b-ba0f-44ad542443d7")
        );
    }

    /// Falling back to name resolution would silently serve a different repository than the caller
    /// addressed.
    #[test]
    fn a_malformed_repo_uuid_is_refused_rather_than_ignored() {
        let req = TestRequest::default()
            .insert_header((REPO_UUID_HEADER, "not-a-uuid"))
            .to_http_request();

        assert!(matches!(
            TransitionalIdentity::from_request(&req),
            Err(OxenHttpError::BadRequest(_))
        ));
    }

    #[test]
    fn a_stated_uuid_matching_the_repository_is_addressed_correctly() {
        let repo_uuid = Uuid::new_v4();
        let identity = TransitionalIdentity {
            repo_uuid: Some(repo_uuid),
            ..Default::default()
        };
        assert!(identity.check_addresses(Some(repo_uuid)).is_ok());
    }

    /// The name in the URL resolved to some repository; if the caller named a different one, that
    /// request was meant for somewhere else.
    #[test]
    fn a_stated_uuid_naming_another_repository_is_refused() {
        let identity = TransitionalIdentity {
            repo_uuid: Some(Uuid::new_v4()),
            ..Default::default()
        };
        assert!(matches!(
            identity.check_addresses(Some(Uuid::new_v4())),
            Err(OxenHttpError::BadRequest(_))
        ));
    }

    /// Either side may legitimately be absent: a caller that states nothing, and a repository
    /// whose identity backfill has not run.
    #[test]
    fn a_check_with_either_side_absent_passes() {
        let stated = TransitionalIdentity {
            repo_uuid: Some(Uuid::new_v4()),
            ..Default::default()
        };
        assert!(stated.check_addresses(None).is_ok());
        assert!(
            TransitionalIdentity::default()
                .check_addresses(Some(Uuid::new_v4()))
                .is_ok()
        );
    }

    #[test]
    fn an_empty_header_is_refused() {
        for header in [REPO_UUID_HEADER, NAMESPACE_HEADER, REPO_NAME_HEADER] {
            let req = TestRequest::default()
                .insert_header((header, ""))
                .to_http_request();

            assert!(
                matches!(
                    TransitionalIdentity::from_request(&req),
                    Err(OxenHttpError::BadRequest(_))
                ),
                "{header} must be refused when empty"
            );
        }
    }
}
