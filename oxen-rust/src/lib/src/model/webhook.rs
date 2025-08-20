use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Webhook {
    pub id: String,
    pub path: String,
    pub webhook_url: String,
    pub webhook_secret: String,
    pub current_oxen_revision: String,
    pub purpose: String,
    pub contact: String,
    pub created_at: SystemTime,
    pub last_notified: Option<SystemTime>,
    pub notification_count: u64,
    pub consecutive_failures: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WebhookAddRequest {
    pub path: String,
    pub webhook_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_oxen_revision: Option<String>,
    #[serde(rename = "currentOxenRevision")]
    pub current_oxen_revision_camel: Option<String>,
    pub purpose: String,
    pub contact: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WebhookNotification {
    pub path: String,
}

impl WebhookAddRequest {
    pub fn get_current_oxen_revision(&self) -> Option<&str> {
        self.current_oxen_revision_camel
            .as_deref()
            .or(self.current_oxen_revision.as_deref())
    }
}

impl Webhook {
    pub fn new(request: WebhookAddRequest) -> Option<Self> {
        let current_oxen_revision = request.get_current_oxen_revision()?.to_string();
        // Generate a secure random secret for webhook authentication
        let webhook_secret = uuid::Uuid::new_v4().to_string();
        Some(Self {
            id: uuid::Uuid::new_v4().to_string(),
            path: request.path,
            webhook_url: request.webhook_url,
            webhook_secret,
            current_oxen_revision,
            purpose: request.purpose,
            contact: request.contact,
            created_at: SystemTime::now(),
            last_notified: None,
            notification_count: 0,
            consecutive_failures: 0,
        })
    }

    pub fn should_notify(&self, current_revision: &str) -> bool {
        current_revision != self.current_oxen_revision
    }

    pub fn should_auto_remove(&self) -> bool {
        self.consecutive_failures >= 5
    }
}