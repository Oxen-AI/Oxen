use crate::{errors::OxenHttpError, params::{app_data, path_param}};
use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::core::webhook_dispatcher::WebhookDispatcher;
use liboxen::model::Branch;
use liboxen::repositories;
use liboxen::view::http::STATUS_SUCCESS;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ActionResponse {
    action: String,
    status: String,
    state: String,
}

#[derive(Serialize, Deserialize)]
pub struct PushCompleteBody {
    pub branch: Branch,
}

pub async fn completed(req: HttpRequest, body: Option<web::Json<PushCompleteBody>>) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let action = path_param(&req, "action")?;
    
    log::debug!("{} action completed", action);
    
    // If this is a push completion, trigger webhooks
    if action == "push" {
        if let Some(push_data) = body {
            // Extract namespace and repo from URL path
            if let (Some(namespace), Some(repo_name)) = (
                req.match_info().get("namespace"),
                req.match_info().get("repo_name")
            ) {
                
                match repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name) {
                    Ok(Some(repo)) => {
                        match repositories::commits::get_by_id(&repo, &push_data.branch.commit_id) {
                            Ok(Some(commit)) => {
                                match WebhookDispatcher::from_repo(&repo) {
                                    Ok(dispatcher) => {
                                        let repo_clone = repo.clone();
                                        let commit_clone = commit.clone();
                                        tokio::spawn(async move {
                                            match dispatcher.dispatch_webhook_event(&repo_clone, &commit_clone).await {
                                                Ok(_) => {
                                                    log::debug!("Webhook notifications dispatched successfully for push action");
                                                }
                                                Err(e) => {
                                                    log::error!("Failed to dispatch webhook notifications for push action: {}", e);
                                                }
                                            }
                                        });
                                    }
                                    Err(e) => {
                                        log::error!("Failed to create webhook dispatcher for push action: {}", e);
                                    }
                                }
                            }
                            Ok(None) => {
                                log::error!("Could not find commit {} for push webhook", push_data.branch.commit_id);
                            }
                            Err(e) => {
                                log::error!("Error finding commit {} for push webhook: {}", push_data.branch.commit_id, e);
                            }
                        }
                    }
                    Ok(None) => {
                        log::error!("Could not find repository {}/{} for push webhook", namespace, repo_name);
                    }
                    Err(e) => {
                        log::error!("Error finding repository {}/{} for push webhook: {}", namespace, repo_name, e);
                    }
                }
            } else {
            }
        } else {
        }
    }
    
    let resp = ActionResponse {
        action: action.to_string(),
        state: "completed".to_string(),
        status: STATUS_SUCCESS.to_string(),
    };
    Ok(HttpResponse::Ok().json(resp))
}

pub async fn started(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let action = path_param(&req, "action")?;
    let resp = ActionResponse {
        action: action.to_string(),
        status: STATUS_SUCCESS.to_string(),
        state: "started".to_string(),
    };
    Ok(HttpResponse::Ok().json(resp))
}
