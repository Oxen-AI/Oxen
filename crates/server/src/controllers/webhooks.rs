use actix_web::{HttpRequest, HttpResponse};
use serde_json::Value;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use liboxen::core::db::webhooks::WebhookDB;
use liboxen::model::webhook::{WebhookAddRequest, WebhookResponse};
use liboxen::repositories;

pub async fn create(
    req: HttpRequest,
    body: actix_web::web::Json<Value>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let webhook_req: WebhookAddRequest = serde_json::from_value(body.into_inner())?;

    // Validate webhook URL scheme
    let url_parsed = url::Url::parse(&webhook_req.webhook_url)
        .map_err(|_| OxenHttpError::BadRequest("Invalid webhook URL".into()))?;
    let scheme = url_parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(OxenHttpError::BadRequest(
            "Only http and https webhook URLs are allowed".into(),
        ));
    }

    // Validate current_oxen_revision if provided
    if let Some(ref claimed_revision) = webhook_req.current_oxen_revision {
        let head_commit = repositories::commits::head_commit_maybe(&repo)?;
        let matches = head_commit
            .as_ref()
            .map(|c| c.id == *claimed_revision)
            .unwrap_or(false);
        if !matches {
            let actual = head_commit.map(|c| c.id).unwrap_or("none".to_string());
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": "revision_mismatch",
                "field": "current_oxen_revision",
                "claimed": claimed_revision,
                "actual": actual,
            })));
        }
    }

    let db = WebhookDB::new(&repo.path.join(".oxen"))?;
    let webhook = db.add_webhook(webhook_req)?;

    Ok(HttpResponse::Ok().json(webhook))
}

pub async fn list(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let db = WebhookDB::new(&repo.path.join(".oxen"))?;
    let webhooks: Vec<WebhookResponse> = db
        .list_all_webhooks()?
        .into_iter()
        .map(WebhookResponse::from)
        .collect();

    Ok(HttpResponse::Ok().json(serde_json::json!({"webhooks": webhooks})))
}

pub async fn delete(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let webhook_id = path_param(&req, "webhook_id")?;

    let db = WebhookDB::new(&repo.path.join(".oxen"))?;
    db.remove_webhook(webhook_id)?;

    Ok(HttpResponse::Ok().json(serde_json::json!({"status": "deleted"})))
}

pub async fn stats(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let db = WebhookDB::new(&repo.path.join(".oxen"))?;
    let stats = db.stats()?;

    Ok(HttpResponse::Ok().json(serde_json::json!({"stats": stats})))
}

pub async fn get_config(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let db = WebhookDB::new(&repo.path.join(".oxen"))?;
    let config = db.get_config()?;

    Ok(HttpResponse::Ok().json(config))
}

pub async fn update_config(
    req: HttpRequest,
    body: actix_web::web::Json<Value>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let config: liboxen::model::webhook::WebhookConfig = serde_json::from_value(body.into_inner())?;
    let db = WebhookDB::new(&repo.path.join(".oxen"))?;
    db.set_config(&config)?;

    Ok(HttpResponse::Ok().json(serde_json::json!({"status": "updated"})))
}
