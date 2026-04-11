use crate::core::db::webhooks::WebhookDB;
use crate::error::OxenError;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const MAX_CONSECUTIVE_FAILURES: u32 = 5;

pub struct WebhookNotifier {
    rate_limits: Mutex<HashMap<String, Instant>>,
    rate_limit_duration: Duration,
    client: reqwest::Client,
}

impl Default for WebhookNotifier {
    fn default() -> Self {
        Self::new()
    }
}

impl WebhookNotifier {
    pub fn new() -> Self {
        WebhookNotifier {
            rate_limits: Mutex::new(HashMap::new()),
            rate_limit_duration: Duration::from_secs(1),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }

    pub fn new_with_rate_limit(rate_limit_secs: u64) -> Self {
        WebhookNotifier {
            rate_limits: Mutex::new(HashMap::new()),
            rate_limit_duration: Duration::from_secs(rate_limit_secs),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }

    /// Notify all webhooks that match the given changed path.
    /// Returns the number of webhooks that were notified (not rate-limited).
    pub async fn notify_path_changed(
        &self,
        repo_path: &Path,
        changed_path: &str,
    ) -> Result<usize, OxenError> {
        let oxen_dir = repo_path.join(".oxen");
        let db = WebhookDB::new(&oxen_dir)?;
        let webhooks = db.list_webhooks_for_path(changed_path)?;

        let mut notified_count = 0;

        for webhook in &webhooks {
            // Check rate limiting
            if self.is_rate_limited(&webhook.id) {
                continue;
            }

            let payload = serde_json::json!({
                "path": changed_path,
            });

            let result = self
                .client
                .post(&webhook.webhook_url)
                .json(&payload)
                .send()
                .await;

            match result {
                Ok(resp) if resp.status().is_success() => {
                    db.reset_failure(&webhook.id)?;
                    self.update_rate_limit(&webhook.id);
                    notified_count += 1;
                }
                _ => {
                    let count = db.increment_failure(&webhook.id)?;
                    if count >= MAX_CONSECUTIVE_FAILURES {
                        log::warn!(
                            "Webhook {} exceeded {} consecutive failures, removing",
                            webhook.id,
                            MAX_CONSECUTIVE_FAILURES
                        );
                        db.remove_webhook(&webhook.id)?;
                    }
                    self.update_rate_limit(&webhook.id);
                    notified_count += 1;
                }
            }
        }

        Ok(notified_count)
    }

    fn is_rate_limited(&self, webhook_id: &str) -> bool {
        if self.rate_limit_duration.is_zero() {
            return false;
        }
        let rate_limits = self.rate_limits.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(last_notified) = rate_limits.get(webhook_id) {
            last_notified.elapsed() < self.rate_limit_duration
        } else {
            false
        }
    }

    fn update_rate_limit(&self, webhook_id: &str) {
        let mut rate_limits = self.rate_limits.lock().unwrap_or_else(|e| e.into_inner());
        rate_limits.insert(webhook_id.to_string(), Instant::now());
    }
}
