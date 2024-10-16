use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::error::OxenError;
use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::repositories;
use liboxen::util;

use actix_files::NamedFile;
use actix_web::{http::header, web, HttpRequest, HttpResponse};

/// Download file content
pub async fn get(
    req: HttpRequest,
    query: web::Query<ImgResize>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("get file path {:?}", req.path());

    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;

    log::debug!(
        "{} resource {namespace}/{repo_name}/{resource}",
        liboxen::current_function!()
    );
    let path = resource.path.clone();
    let entry = repositories::entries::get_file(&repo, &commit, &path)?;
    let entry = entry.ok_or(OxenError::path_does_not_exist(path.clone()))?;

    let version_path = util::fs::version_path_from_hash(&repo, entry.hash.to_string());

    log::debug!("version path {version_path:?}",);

    // TODO: refactor out of here and check for type,
    // but seeing if it works to resize the image and cache it to disk if we have a resize query
    let img_resize = query.into_inner();
    if img_resize.width.is_some() || img_resize.height.is_some() {
        log::debug!("img_resize {:?}", img_resize);

        let resized_path = util::fs::resized_path_for_file_node(
            &repo,
            &entry,
            img_resize.width,
            img_resize.height,
        )?;

        util::fs::resize_cache_image(&version_path, &resized_path, img_resize)?;

        log::debug!("In the resize cache! {:?}", resized_path);
        return Ok(NamedFile::open(resized_path)?.into_response(&req));
    } else {
        log::debug!("did not hit the resize cache");
    }

    log::debug!(
        "get_file_for_commit_id looking for {:?} -> {:?}",
        path,
        version_path
    );

    let file = NamedFile::open(version_path)?;
    let mut response = file.into_response(&req);

    let last_commit_id = entry.last_commit_id.to_string();
    response.headers_mut().insert(
        header::HeaderName::from_static("oxen-revision-id"),
        header::HeaderValue::from_str(&last_commit_id).unwrap(),
    );

    Ok(response)
}
