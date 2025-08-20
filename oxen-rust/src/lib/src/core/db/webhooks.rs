use crate::core::db::key_val::{opts, str_json_db};
use crate::error::OxenError;
use crate::model::{Webhook, WebhookAddRequest};

use std::collections::HashMap;
use std::path::Path;
use std::time::SystemTime;
use rocksdb::{DBWithThreadMode, MultiThreaded};

pub struct WebhookDB {
    db: DBWithThreadMode<MultiThreaded>,
}

impl WebhookDB {
    pub fn new(db_path: &Path) -> Result<Self, OxenError> {
        let opts = opts::default();
        let db = DBWithThreadMode::open(&opts, db_path)?;
        Ok(Self { db })
    }

    pub fn add_webhook(&self, request: WebhookAddRequest) -> Result<Webhook, OxenError> {
        // Check for existing webhook with same path and webhook_url (deduplication)
        let all_webhooks: Vec<Webhook> = str_json_db::list_vals(&self.db)?;
        for existing in &all_webhooks {
            if existing.path == request.path && existing.webhook_url == request.webhook_url {
                // Update existing webhook with fresh secret and current revision
                let mut updated_webhook = existing.clone();
                updated_webhook.webhook_secret = uuid::Uuid::new_v4().to_string();
                if let Some(revision) = request.get_current_oxen_revision() {
                    updated_webhook.current_oxen_revision = revision.to_string();
                }
                str_json_db::put(&self.db, &updated_webhook.id, &updated_webhook)?;
                return Ok(updated_webhook);
            }
        }
        
        let webhook = Webhook::new(request)
            .ok_or_else(|| OxenError::basic_str("Invalid webhook request: missing revision"))?;
        str_json_db::put(&self.db, &webhook.id, &webhook)?;
        Ok(webhook)
    }

    pub fn list_webhooks_for_path(&self, path: &str) -> Result<Vec<Webhook>, OxenError> {
        let all_webhooks: Vec<Webhook> = str_json_db::list_vals(&self.db)?;
        let filtered: Vec<Webhook> = all_webhooks
            .into_iter()
            .filter(|webhook| self.path_matches_webhook(&webhook.path, path))
            .collect();
        Ok(filtered)
    }

    /// Check if a changed path should trigger a webhook based on hierarchical matching.
    /// A webhook registered for a parent path will be triggered by changes in child paths.
    /// 
    /// Examples:
    /// - Webhook for "/" matches any path
    /// - Webhook for "/data/" matches "/data/file.txt" and "/data/subfolder/file.txt" 
    /// - Webhook for "/data/file.txt" matches only "/data/file.txt"
    fn path_matches_webhook(&self, webhook_path: &str, changed_path: &str) -> bool {
        // Normalize paths to ensure consistent comparison
        let webhook_path = self.normalize_path(webhook_path);
        let changed_path = self.normalize_path(changed_path);
        
        // For root path, match everything
        if webhook_path == "/" {
            return true;
        }
        
        // Check if the changed path starts with the webhook path
        // This implements hierarchical matching where parent paths catch child changes
        changed_path.starts_with(&webhook_path)
    }

    /// Normalize paths for consistent comparison.
    /// Ensures paths start with "/" and removes trailing "/" except for root.
    fn normalize_path(&self, path: &str) -> String {
        let mut normalized = path.to_string();
        
        // Ensure path starts with "/"
        if !normalized.starts_with('/') {
            normalized = format!("/{}", normalized);
        }
        
        // Remove trailing "/" except for root path
        if normalized.len() > 1 && normalized.ends_with('/') {
            normalized.pop();
        }
        
        normalized
    }

    pub fn list_all_webhooks(&self) -> Result<Vec<Webhook>, OxenError> {
        str_json_db::list_vals(&self.db)
    }

    pub fn remove_webhook(&self, webhook_id: &str) -> Result<bool, OxenError> {
        match str_json_db::get::<MultiThreaded, &str, Webhook>(&self.db, webhook_id)? {
            Some(_) => {
                str_json_db::delete(&self.db, webhook_id)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub fn update_notification_stats(&self, webhook_id: &str) -> Result<(), OxenError> {
        if let Some(mut webhook) = str_json_db::get::<MultiThreaded, &str, Webhook>(&self.db, webhook_id)? {
            webhook.last_notified = Some(SystemTime::now());
            webhook.notification_count += 1;
            webhook.consecutive_failures = 0; // Reset on success
            str_json_db::put(&self.db, webhook_id, &webhook)?;
        }
        Ok(())
    }

    pub fn record_notification_failure(&self, webhook_id: &str) -> Result<bool, OxenError> {
        if let Some(mut webhook) = str_json_db::get::<MultiThreaded, &str, Webhook>(&self.db, webhook_id)? {
            webhook.consecutive_failures += 1;
            
            if webhook.should_auto_remove() {
                // Remove webhook after 5 consecutive failures
                str_json_db::delete(&self.db, webhook_id)?;
                log::info!("Auto-removed webhook {} after {} consecutive failures", webhook_id, webhook.consecutive_failures);
                return Ok(true); // Indicates webhook was removed
            } else {
                str_json_db::put(&self.db, webhook_id, &webhook)?;
                return Ok(false); // Webhook still exists
            }
        }
        Ok(false)
    }

    pub fn cleanup_old_webhooks(&self, max_age_days: u64) -> Result<usize, OxenError> {
        let cutoff = SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(max_age_days * 24 * 60 * 60))
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let all_webhooks: Vec<Webhook> = str_json_db::list_vals(&self.db)?;
        let mut removed_count = 0;

        for webhook in all_webhooks {
            if webhook.created_at < cutoff {
                str_json_db::delete(&self.db, &webhook.id)?;
                removed_count += 1;
            }
        }

        Ok(removed_count)
    }

    pub fn get_webhook_stats(&self) -> Result<HashMap<String, u64>, OxenError> {
        let webhooks: Vec<Webhook> = str_json_db::list_vals(&self.db)?;
        let mut stats = HashMap::new();
        
        stats.insert("total_webhooks".to_string(), webhooks.len() as u64);
        
        let total_notifications: u64 = webhooks.iter().map(|w| w.notification_count).sum();
        stats.insert("total_notifications".to_string(), total_notifications);

        let active_webhooks = webhooks.iter()
            .filter(|w| w.last_notified.is_some())
            .count() as u64;
        stats.insert("active_webhooks".to_string(), active_webhooks);

        let failing_webhooks = webhooks.iter()
            .filter(|w| w.consecutive_failures > 0)
            .count() as u64;
        stats.insert("failing_webhooks".to_string(), failing_webhooks);

        let at_risk_webhooks = webhooks.iter()
            .filter(|w| w.consecutive_failures >= 3)
            .count() as u64;
        stats.insert("at_risk_webhooks".to_string(), at_risk_webhooks);

        Ok(stats)
    }
}