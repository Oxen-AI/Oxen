pub mod chunks;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_multipart::Multipart;
use actix_web::Error;
use actix_web::{HttpRequest, HttpResponse};
use flate2::read::GzDecoder;
use futures_util::TryStreamExt as _;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::view::versions::{VersionFile, VersionFileResponse};
use liboxen::view::{ErrorFileInfo, FilesHashResponse, StatusMessage};
use mime;
use std::io::Read as StdRead;

pub async fn metadata(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let version_id = path_param(&req, "version_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let exists = repo.version_store()?.version_exists(&version_id)?;
    if !exists {
        return Err(OxenHttpError::NotFound);
    }

    let data = repo.version_store()?.get_version(&version_id)?;
    Ok(HttpResponse::Ok().json(VersionFileResponse {
        status: StatusMessage::resource_found(),
        version: VersionFile {
            hash: version_id,
            size: data.len() as u64,
        },
    }))
}

pub async fn download(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let version_id = path_param(&req, "version_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    log::debug!(
        "download file for repo: {:?}, file_hash: {}",
        repo.path,
        version_id
    );

    let version_store = repo.version_store()?;

    // TODO: stream the file
    let file_data = version_store.get_version(&version_id)?;
    Ok(HttpResponse::Ok().body(file_data))
}

pub async fn batch_upload(
    req: HttpRequest,
    payload: Multipart,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;

    log::debug!("batch upload file for repo: {:?}", repo.path);

    let err_files = save_multiparts(payload, &repo).await?;
    Ok(HttpResponse::Ok().json(FilesHashResponse {
        status: StatusMessage::resource_created(),
        err_files,
    }))
}

pub async fn save_multiparts(
    mut payload: Multipart,
    repo: &LocalRepository,
) -> Result<Vec<ErrorFileInfo>, Error> {
    let version_store = repo.version_store().map_err(|oxen_err: OxenError| {
        log::error!("Failed to get version store: {:?}", oxen_err);
        actix_web::error::ErrorInternalServerError(oxen_err.to_string())
    })?;
    let gzip_mime: mime::Mime = "application/gzip".parse().unwrap();

    let mut err_files: Vec<ErrorFileInfo> = vec![];

    while let Some(mut field) = payload.try_next().await? {
        let Some(content_disposition) = field.content_disposition() else {
            continue;
        };

        if let Some(name) = content_disposition.get_name() {
            if "file[]" == name {
                let upload_filehash = content_disposition.get_filename().map_or_else(
                    || {
                        Err(actix_web::error::ErrorBadRequest(
                            "Missing filename in multipart request",
                        ))
                    },
                    |fhash_os_str| Ok(fhash_os_str.to_string()),
                )?;

                let mut field_bytes = Vec::new();
                while let Some(chunk) = field.try_next().await? {
                    field_bytes.extend_from_slice(&chunk);
                }

                let version_store_copy = version_store.clone();
                let upload_filehash_copy = upload_filehash.clone();
                let is_gzipped = field
                    .content_type()
                    .map(|mime| {
                        mime.type_() == gzip_mime.type_() && mime.subtype() == gzip_mime.subtype()
                    })
                    .unwrap_or(false);

                match actix_web::web::block(move || {
                    let data_to_store = if is_gzipped {
                        log::debug!(
                            "Decompressing gzipped data for hash: {}",
                            &upload_filehash_copy
                        );
                        let mut decoder = GzDecoder::new(&field_bytes[..]);
                        let mut decompressed_bytes = Vec::new();
                        decoder.read_to_end(&mut decompressed_bytes).map_err(|e| {
                            OxenError::basic_str(format!(
                                "Failed to decompress gzipped data: {}",
                                e
                            ))
                        })?;
                        decompressed_bytes
                    } else {
                        log::debug!("Data for hash {} is not gzipped.", &upload_filehash_copy);
                        field_bytes
                    };

                    version_store_copy.store_version(&upload_filehash_copy, &data_to_store)
                })
                .await
                {
                    Ok(Ok(_)) => {
                        log::info!("Successfully stored version for hash: {}", &upload_filehash);
                    }
                    Ok(Err(e)) => {
                        log::error!(
                            "Failed to store version for hash {}: {}",
                            &upload_filehash,
                            e
                        );
                        err_files.push(ErrorFileInfo {
                            hash: upload_filehash.clone(),
                            error: format!("Failed to store version: {}", e),
                        });
                        continue;
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to execute blocking task for hash {}: {}",
                            &upload_filehash,
                            e
                        );
                        err_files.push(ErrorFileInfo {
                            hash: upload_filehash.clone(),
                            error: format!("Failed to execute blocking task: {}", e),
                        });
                        continue;
                    }
                }
            }
        }
    }
    Ok(err_files)
}

#[cfg(test)]
mod tests {
    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;
    use actix_multipart::test::create_form_data_payload_and_headers;
    use actix_web::{web, web::Bytes, App};
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;
    use liboxen::view::FilesHashResponse;
    use mime;
    use std::io::Write;

    #[actix_web::test]
    async fn test_controllers_versions_download() -> Result<(), OxenError> {
        test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        // create test file and commit
        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        let file_content = "Hello";
        util::fs::write_to_path(&hello_file, file_content)?;
        repositories::add(&repo, &hello_file)?;
        repositories::commit(&repo, "First commit")?;

        // get file version id
        let file_hash = util::hasher::hash_str(file_content);

        // test download
        let uri = format!("/oxen/{namespace}/{repo_name}/versions/{file_hash}");
        let req = actix_web::test::TestRequest::get()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/versions/{version_id}",
                    web::get().to(controllers::versions::download),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        assert_eq!(bytes, "Hello");

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_versions_batch_upload() -> Result<(), OxenError> {
        test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "hello")?;
        repositories::add(&repo, path)?;
        repositories::commit(&repo, "first commit")?;

        let file_content = "Test Content";
        let file_hash = util::hasher::hash_str(file_content);

        // compress file content
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(file_content.as_bytes())?;
        let compressed_bytes = encoder.finish()?;

        // create multipart request
        let (body, headers) = create_form_data_payload_and_headers(
            "file[]",
            Some(file_hash.clone()),
            Some("application/gzip".parse::<mime::Mime>().unwrap()),
            Bytes::from(compressed_bytes),
        );
        let uri = format!("/oxen/{namespace}/{repo_name}/versions");

        let req = actix_web::test::TestRequest::post()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()));

        let req = headers
            .into_iter()
            .fold(req, |req, hdr| req.insert_header(hdr))
            .set_payload(body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/versions",
                    web::post().to(controllers::versions::batch_upload),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        println!("resp: {:?}", resp);
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let response: FilesHashResponse = serde_json::from_slice(&bytes)?;
        assert_eq!(response.status.status, "success");
        assert!(response.err_files.is_empty());

        // verify file is stored correctly
        let version_store = repo.version_store()?;
        let stored_data = version_store.get_version(&file_hash)?;
        assert_eq!(stored_data, file_content.as_bytes());

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }
}
