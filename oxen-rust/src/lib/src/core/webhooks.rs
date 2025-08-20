use crate::core::db::webhooks::WebhookDB;
use crate::error::OxenError;
use crate::model::WebhookNotification;
use std::collections::HashMap;
use std::path::Path;

pub struct WebhookNotifier {
    client: reqwest::Client,
    stats: HashMap<String, u64>,
    rate_limit_seconds: u64,
}

impl Default for WebhookNotifier {
    fn default() -> Self {
        Self::new()
    }
}

impl WebhookNotifier {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
            stats: HashMap::new(),
            rate_limit_seconds: 60, // Default 60-second rate limit
        }
    }

    pub fn new_with_rate_limit(rate_limit_seconds: u64) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
            stats: HashMap::new(),
            rate_limit_seconds,
        }
    }

    pub fn set_rate_limit(&mut self, rate_limit_seconds: u64) {
        self.rate_limit_seconds = rate_limit_seconds;
    }

    pub async fn notify_path_changed(&mut self, repo_path: &Path, changed_path: &str) -> Result<usize, OxenError> {
        let webhook_db_path = repo_path.join(".oxen").join("webhooks");
        
        if !webhook_db_path.exists() {
            return Ok(0);
        }

        let webhook_db = WebhookDB::new(&webhook_db_path)?;
        
        
        let webhooks = webhook_db.list_webhooks_for_path(changed_path)?;
        
        let mut notified_count = 0;
        let mut removed_count = 0;
        let notification = WebhookNotification {
            path: changed_path.to_string(),
        };

        for webhook in &webhooks {
            // Implement rate limiting - skip if notified too recently
            if let Some(last_notified) = webhook.last_notified {
                let now = std::time::SystemTime::now();
                if let Ok(elapsed) = now.duration_since(last_notified) {
                    if elapsed.as_secs() < self.rate_limit_seconds {
                        continue;
                    }
                }
            }

            match self.send_webhook_notification(webhook, &notification).await {
                Ok(_) => {
                    webhook_db.update_notification_stats(&webhook.id)?;
                    notified_count += 1;
                    
                    // Update internal stats
                    let counter = self.stats.entry("notifications_sent".to_string()).or_insert(0);
                    *counter += 1;
                }
                Err(err) => {
                    log::warn!("Failed to send webhook to {}: {}", webhook.contact, err);
                    
                    // Record failure and potentially remove webhook
                    match webhook_db.record_notification_failure(&webhook.id) {
                        Ok(was_removed) => {
                            if was_removed {
                                removed_count += 1;
                                let counter = self.stats.entry("webhooks_auto_removed".to_string()).or_insert(0);
                                *counter += 1;
                                log::info!("Auto-removed webhook {} after consecutive failures", webhook.id);
                            }
                        }
                        Err(db_err) => {
                            log::error!("Failed to record webhook failure: {}", db_err);
                        }
                    }
                    
                    let counter = self.stats.entry("notifications_failed".to_string()).or_insert(0);
                    *counter += 1;
                }
            }
        }

        if removed_count > 0 {
            log::info!("Auto-removed {} failing webhooks for path {}", removed_count, changed_path);
        }
        

        Ok(notified_count)
    }

    async fn send_webhook_notification(&self, webhook: &crate::model::Webhook, notification: &WebhookNotification) -> Result<(), OxenError> {
        let payload = serde_json::to_string(notification)
            .map_err(|e| OxenError::basic_str(format!("Failed to serialize payload: {}", e)))?;
        
        // Generate HMAC-SHA256 signature for webhook authentication
        let signature = self.generate_webhook_signature(&webhook.webhook_secret, &payload)?;
        
        
        let response = self
            .client
            .post(&webhook.webhook_url)
            .header("Content-Type", "application/json")
            .header("X-Oxen-Signature", format!("sha256={}", signature))
            .header("X-Oxen-Delivery", uuid::Uuid::new_v4().to_string())
            .body(payload)
            .send()
            .await
            .map_err(|e| OxenError::basic_str(format!("HTTP request failed: {}", e)))?;


        if !response.status().is_success() {
            return Err(OxenError::basic_str(format!(
                "Webhook returned status: {}",
                response.status()
            )));
        }

        Ok(())
    }
    
    fn generate_webhook_signature(&self, secret: &str, payload: &str) -> Result<String, OxenError> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        
        type HmacSha256 = Hmac<Sha256>;
        
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .map_err(|e| OxenError::basic_str(format!("Invalid secret key: {}", e)))?;
        mac.update(payload.as_bytes());
        let result = mac.finalize();
        Ok(hex::encode(result.into_bytes()))
    }

    pub fn get_stats(&self) -> &HashMap<String, u64> {
        &self.stats
    }

    pub async fn cleanup_old_webhooks(&self, repo_path: &Path, max_age_days: u64) -> Result<usize, OxenError> {
        let webhook_db_path = repo_path.join(".oxen").join("webhooks");
        
        if !webhook_db_path.exists() {
            return Ok(0);
        }

        let webhook_db = WebhookDB::new(&webhook_db_path)?;
        webhook_db.cleanup_old_webhooks(max_age_days)
    }
}