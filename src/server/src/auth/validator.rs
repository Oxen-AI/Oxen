use crate::app_data::OxenAppData;
use crate::auth;

use actix_web::dev::ServiceRequest;
use actix_web_httpauth::extractors::bearer::BearerAuth;

pub async fn validate(
    req: ServiceRequest,
    credentials: BearerAuth,
) -> Result<ServiceRequest, (actix_web::Error, ServiceRequest)> {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    match auth::access_keys::AccessKeyManager::new_read_only(&app_data.path) {
        Ok(keygen) => {
            let token = credentials.token();
            if keygen.token_is_valid(token) {
                Ok(req)
            } else {
                Err((actix_web::error::ErrorUnauthorized("unauthorized"), req))
            }
        }
        Err(err) => {
            log::error!("AUTH DEBUG: Failed to create AccessKeyManager: {:?}", err);
            Err((
                actix_web::error::ErrorInternalServerError(format!("Err could not get keygen: {err}")),
                req,
            ))
        }
    }
}
