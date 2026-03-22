use actix_web::{HttpRequest, HttpResponse};
use liboxen::{
    migrations,
    view::{ListRepositoryResponse, StatusMessage},
};

use crate::{
    errors::OxenHttpError,
    params::{app_data, path_param},
};

pub async fn list_unmigrated(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    log::debug!("in the list_unmigrated controller");
    let app_data = app_data(&req)?;
    let migration_tstamp = path_param(&req, "migration_tstamp")?;

    let unmigrated_repos =
        migrations::list_unmigrated(&app_data.path, migration_tstamp.to_string())?;

    let view = ListRepositoryResponse {
        status: StatusMessage::resource_found(),
        repositories: unmigrated_repos,
    };

    Ok(HttpResponse::Ok().json(view))
}
