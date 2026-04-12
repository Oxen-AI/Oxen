use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Webhook {
    pub id: String,
    pub path: String,
    pub webhook_url: String,
    pub webhook_secret: String,
    pub purpose: String,
    pub contact: String,
    pub consecutive_failures: u32,
}

/// Response type that omits the webhook_secret for list/get endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookResponse {
    pub id: String,
    pub path: String,
    pub webhook_url: String,
    pub purpose: String,
    pub contact: String,
    pub consecutive_failures: u32,
}

impl From<Webhook> for WebhookResponse {
    fn from(w: Webhook) -> Self {
        WebhookResponse {
            id: w.id,
            path: w.path,
            webhook_url: w.webhook_url,
            purpose: w.purpose,
            contact: w.contact,
            consecutive_failures: w.consecutive_failures,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WebhookAddRequest {
    pub path: String,
    pub webhook_url: String,
    #[serde(alias = "currentOxenRevision")]
    pub current_oxen_revision: Option<String>,
    pub purpose: String,
    pub contact: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub mode: WebhookMode,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WebhookMode {
    Inline,
    Queue,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        WebhookConfig {
            mode: WebhookMode::Inline,
            enabled: true,
            queue_path: None,
        }
    }
}
