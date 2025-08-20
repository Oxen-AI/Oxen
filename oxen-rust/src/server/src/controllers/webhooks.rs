use actix_web::{web, HttpRequest, HttpResponse, Result};
use liboxen::core::db::webhooks::WebhookDB;
use liboxen::core::webhook_dispatcher::{WebhookConfig, WebhookDispatcher};
use liboxen::model::{LocalRepository, User, WebhookAddRequest};
use liboxen::repositories;
use serde_json::json;

use crate::app_data::OxenAppData;
use crate::auth::access_keys::AccessKeyManager;
use crate::errors::OxenHttpError;
use crate::helpers;
use crate::params::app_data;

// Helper function to extract authenticated user from bearer token
fn get_authenticated_user(req: &HttpRequest, app_data_path: &std::path::Path) -> Result<Option<User>, OxenHttpError> {
    let auth_header = req.headers().get("authorization");
    
    if let Some(auth_value) = auth_header {
        if let Ok(auth_str) = auth_value.to_str() {
            if auth_str.starts_with("Bearer ") {
                let token = &auth_str[7..]; // Remove "Bearer " prefix
                
                log::debug!("🔑 Validating bearer token for webhook operation");
                
                match AccessKeyManager::new_read_only(app_data_path) {
                    Ok(keygen) => {
                        match keygen.get_claim(token) {
                            Ok(Some(claim)) => {
                                log::debug!("🔑 ✅ Token validated successfully for webhook operation");
                                return Ok(Some(User {
                                    name: claim.name().to_string(),
                                    email: claim.email().to_string(),
                                }));
                            }
                            Ok(None) => {
                                log::debug!("🔑 ❌ Token validation returned None for webhook operation");
                            }
                            Err(e) => {
                                log::debug!("🔑 ❌ Token validation error for webhook operation: {:?}", e);
                            }
                        }
                    }
                    Err(err) => {
                        log::debug!("🔑 ❌ AccessKeyManager creation failed for webhook operation: {:?}", err);
                    }
                }
            } else {
                log::debug!("🔑 ❌ Authorization header does not start with 'Bearer ' for webhook operation");
            }
        } else {
            log::debug!("🔑 ❌ Could not parse authorization header as string for webhook operation");
        }
    } else {
        log::debug!("🔑 ❌ No authorization header found for webhook operation");
    }
    
    Ok(None)
}

pub async fn add_webhook(
    req: HttpRequest,
    query: web::Json<WebhookAddRequest>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    
    // Require authentication for webhook registration
    let _authenticated_user = match get_authenticated_user(&req, &app_data.path)? {
        Some(user) => user,
        None => {
            return Ok(HttpResponse::Unauthorized().json(json!({
                "error": "Bearer token required for webhook registration"
            })));
        }
    };

    let namespace = req.match_info().get("namespace").unwrap();
    let repo_name = req.match_info().get("repo_name").unwrap();
    
    
    let repo_path = helpers::get_repo_path(&app_data.path, namespace, repo_name);
    let repo = match LocalRepository::from_dir(&repo_path) {
        Ok(repo) => repo,
        Err(err) => {
            let error_msg = format!("Repository not found: {}", err);
            return Ok(HttpResponse::NotFound().json(json!({"error": error_msg})));
        }
    };

    // Validate that the current revision matches
    let current_revision = match repositories::commits::head_commit_maybe(&repo) {
        Ok(Some(commit)) => commit.id,
        Ok(None) => {
            return Ok(HttpResponse::BadRequest().json(json!({
                "error": "Repository has no commits"
            })));
        }
        Err(err) => {
            let error_msg = format!("Could not get current revision: {}", err);
            return Ok(HttpResponse::InternalServerError().json(json!({"error": error_msg})));
        }
    };

    let provided_revision = query.get_current_oxen_revision().unwrap_or("");
    if provided_revision != current_revision {
        return Ok(HttpResponse::BadRequest().json(json!({"error": "no"})));
    }

    // Create webhook database path
    let webhook_db_path = repo_path.join(".oxen").join("webhooks");
    std::fs::create_dir_all(&webhook_db_path).map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to create webhook directory: {}", e))
    })?;

    let webhook_db = WebhookDB::new(&webhook_db_path).map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to open webhook database: {}", e))
    })?;

    match webhook_db.add_webhook(query.into_inner()) {
        Ok(webhook) => Ok(HttpResponse::Ok().json(webhook)),
        Err(err) => {
            let error_msg = format!("Failed to add webhook: {}", err);
            Ok(HttpResponse::InternalServerError().json(json!({"error": error_msg})))
        }
    }
}

pub async fn list_webhooks(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    
    // Require authentication for listing webhooks
    let _authenticated_user = match get_authenticated_user(&req, &app_data.path)? {
        Some(user) => user,
        None => {
            return Ok(HttpResponse::Unauthorized().json(json!({
                "error": "Bearer token required for webhook operations"
            })));
        }
    };

    let namespace = req.match_info().get("namespace").unwrap();
    let repo_name = req.match_info().get("repo_name").unwrap();
    
    let repo_path = helpers::get_repo_path(&app_data.path, namespace, repo_name);
    
    if !repo_path.exists() {
        return Ok(HttpResponse::NotFound().json(json!({"error": "Repository not found"})));
    }

    let webhook_db_path = repo_path.join(".oxen").join("webhooks");
    
    if !webhook_db_path.exists() {
        return Ok(HttpResponse::Ok().json(json!({"webhooks": []})));
    }

    let webhook_db = WebhookDB::new(&webhook_db_path).map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to open webhook database: {}", e))
    })?;

    match webhook_db.list_all_webhooks() {
        Ok(webhooks) => Ok(HttpResponse::Ok().json(json!({"webhooks": webhooks}))),
        Err(err) => {
            let error_msg = format!("Failed to list webhooks: {}", err);
            Ok(HttpResponse::InternalServerError().json(json!({"error": error_msg})))
        }
    }
}

pub async fn webhook_stats(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    
    // Require authentication for webhook stats
    let _authenticated_user = match get_authenticated_user(&req, &app_data.path)? {
        Some(user) => user,
        None => {
            return Ok(HttpResponse::Unauthorized().json(json!({
                "error": "Bearer token required for webhook operations"
            })));
        }
    };

    let namespace = req.match_info().get("namespace").unwrap();
    let repo_name = req.match_info().get("repo_name").unwrap();
    
    let repo_path = helpers::get_repo_path(&app_data.path, namespace, repo_name);
    let webhook_db_path = repo_path.join(".oxen").join("webhooks");
    
    if !webhook_db_path.exists() {
        return Ok(HttpResponse::Ok().json(json!({"stats": {
            "total_webhooks": 0,
            "total_notifications": 0,
            "active_webhooks": 0
        }})));
    }

    let webhook_db = WebhookDB::new(&webhook_db_path).map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to open webhook database: {}", e))
    })?;

    match webhook_db.get_webhook_stats() {
        Ok(stats) => Ok(HttpResponse::Ok().json(json!({"stats": stats}))),
        Err(err) => {
            let error_msg = format!("Failed to get webhook stats: {}", err);
            Ok(HttpResponse::InternalServerError().json(json!({"error": error_msg})))
        }
    }
}

pub async fn remove_webhook(
    req: HttpRequest,
    app_data: web::Data<OxenAppData>,
) -> Result<HttpResponse> {
    let namespace = req.match_info().get("namespace").unwrap();
    let repo_name = req.match_info().get("repo_name").unwrap();
    let webhook_id = req.match_info().get("webhook_id").unwrap();
    
    let repo_path = helpers::get_repo_path(&app_data.path, namespace, repo_name);
    let webhook_db_path = repo_path.join(".oxen").join("webhooks");
    
    if !webhook_db_path.exists() {
        return Ok(HttpResponse::NotFound().json(json!({"error": "No webhooks found"})));
    }

    let webhook_db = WebhookDB::new(&webhook_db_path).map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to open webhook database: {}", e))
    })?;

    match webhook_db.remove_webhook(webhook_id) {
        Ok(true) => Ok(HttpResponse::Ok().json(json!({"success": true, "message": "Webhook removed"}))),
        Ok(false) => Ok(HttpResponse::NotFound().json(json!({"error": "Webhook not found"}))),
        Err(err) => {
            let error_msg = format!("Failed to remove webhook: {}", err);
            Ok(HttpResponse::InternalServerError().json(json!({"error": error_msg})))
        }
    }
}

pub async fn cleanup_webhooks(
    req: HttpRequest,
    query: web::Query<CleanupQuery>,
    app_data: web::Data<OxenAppData>,
) -> Result<HttpResponse> {
    let namespace = req.match_info().get("namespace").unwrap();
    let repo_name = req.match_info().get("repo_name").unwrap();
    
    let repo_path = helpers::get_repo_path(&app_data.path, namespace, repo_name);
    let webhook_db_path = repo_path.join(".oxen").join("webhooks");
    
    if !webhook_db_path.exists() {
        return Ok(HttpResponse::Ok().json(json!({"removed_count": 0})));
    }

    let webhook_db = WebhookDB::new(&webhook_db_path).map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to open webhook database: {}", e))
    })?;

    let max_age_days = query.max_age_days.unwrap_or(30); // Default to 30 days
    match webhook_db.cleanup_old_webhooks(max_age_days) {
        Ok(removed_count) => Ok(HttpResponse::Ok().json(json!({"removed_count": removed_count}))),
        Err(err) => {
            let error_msg = format!("Failed to cleanup webhooks: {}", err);
            Ok(HttpResponse::InternalServerError().json(json!({"error": error_msg})))
        }
    }
}

#[derive(serde::Deserialize)]
pub struct CleanupQuery {
    pub max_age_days: Option<u64>,
}

pub async fn get_webhook_config(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    
    // Require authentication for webhook config access
    let _authenticated_user = match get_authenticated_user(&req, &app_data.path)? {
        Some(user) => user,
        None => {
            return Ok(HttpResponse::Unauthorized().json(json!({
                "error": "Bearer token required for webhook config access"
            })));
        }
    };

    let namespace = req.match_info().get("namespace").unwrap();
    let repo_name = req.match_info().get("repo_name").unwrap();
    
    let repo_path = helpers::get_repo_path(&app_data.path, namespace, repo_name);
    let repo = match LocalRepository::from_dir(&repo_path) {
        Ok(repo) => repo,
        Err(err) => {
            let error_msg = format!("Repository not found: {}", err);
            return Ok(HttpResponse::NotFound().json(json!({"error": error_msg})));
        }
    };

    match WebhookDispatcher::from_repo(&repo) {
        Ok(dispatcher) => Ok(HttpResponse::Ok().json(&dispatcher.config)),
        Err(err) => {
            let error_msg = format!("Failed to load webhook config: {}", err);
            Ok(HttpResponse::InternalServerError().json(json!({"error": error_msg})))
        }
    }
}

pub async fn set_webhook_config(
    req: HttpRequest,
    config: web::Json<WebhookConfig>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    
    // Require authentication for webhook config updates
    let _authenticated_user = match get_authenticated_user(&req, &app_data.path)? {
        Some(user) => user,
        None => {
            return Ok(HttpResponse::Unauthorized().json(json!({
                "error": "Bearer token required for webhook config updates"
            })));
        }
    };

    let namespace = req.match_info().get("namespace").unwrap();
    let repo_name = req.match_info().get("repo_name").unwrap();
    
    let repo_path = helpers::get_repo_path(&app_data.path, namespace, repo_name);
    let repo = match LocalRepository::from_dir(&repo_path) {
        Ok(repo) => repo,
        Err(err) => {
            let error_msg = format!("Repository not found: {}", err);
            return Ok(HttpResponse::NotFound().json(json!({"error": error_msg})));
        }
    };

    let dispatcher = WebhookDispatcher::new(config.into_inner());
    match dispatcher.save_config(&repo) {
        Ok(_) => Ok(HttpResponse::Ok().json(json!({
            "message": "Webhook configuration updated successfully"
        }))),
        Err(err) => {
            let error_msg = format!("Failed to save webhook config: {}", err);
            Ok(HttpResponse::InternalServerError().json(json!({"error": error_msg})))
        }
    }
}