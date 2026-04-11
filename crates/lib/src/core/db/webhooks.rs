use crate::error::OxenError;
use crate::model::webhook::{Webhook, WebhookAddRequest, WebhookConfig};
use std::path::{Path, PathBuf};

pub struct WebhookDB {
    webhooks_path: PathBuf,
    config_path: PathBuf,
}

impl WebhookDB {
    pub fn new(repo_dot_oxen_path: &Path) -> Result<Self, OxenError> {
        let dir = repo_dot_oxen_path.join("webhooks");
        std::fs::create_dir_all(&dir)?;
        Ok(WebhookDB {
            webhooks_path: dir.join("webhooks.json"),
            config_path: dir.join("config.json"),
        })
    }

    pub fn add_webhook(&self, req: WebhookAddRequest) -> Result<Webhook, OxenError> {
        let mut webhooks = self.list_all_webhooks()?;
        let webhook = Webhook {
            id: uuid::Uuid::new_v4().to_string(),
            path: req.path,
            webhook_url: req.webhook_url,
            webhook_secret: uuid::Uuid::new_v4().to_string(),
            purpose: req.purpose,
            contact: req.contact,
            consecutive_failures: 0,
        };
        webhooks.push(webhook.clone());
        self.save_webhooks(&webhooks)?;
        Ok(webhook)
    }

    pub fn list_all_webhooks(&self) -> Result<Vec<Webhook>, OxenError> {
        if !self.webhooks_path.exists() {
            return Ok(vec![]);
        }
        let content = std::fs::read_to_string(&self.webhooks_path)?;
        if content.trim().is_empty() {
            return Ok(vec![]);
        }
        let webhooks: Vec<Webhook> = serde_json::from_str(&content)?;
        Ok(webhooks)
    }

    pub fn remove_webhook(&self, id: &str) -> Result<(), OxenError> {
        let mut webhooks = self.list_all_webhooks()?;
        webhooks.retain(|w| w.id != id);
        self.save_webhooks(&webhooks)?;
        Ok(())
    }

    /// Find all webhooks whose path prefix matches the given changed file path.
    /// A webhook registered at `/data` will match changes to `/data/file.txt`,
    /// `/data/subdir/file.txt`, etc. A root webhook (`/` or empty) matches everything.
    pub fn list_webhooks_for_path(&self, changed_path: &str) -> Result<Vec<Webhook>, OxenError> {
        let webhooks = self.list_all_webhooks()?;
        let normalized_changed = normalize_webhook_path(changed_path);
        Ok(webhooks
            .into_iter()
            .filter(|w| {
                let normalized_webhook = normalize_webhook_path(&w.path);
                if normalized_webhook.is_empty() {
                    return true; // Root webhook matches everything
                }
                normalized_changed.starts_with(&normalized_webhook)
                    && (normalized_changed.len() == normalized_webhook.len()
                        || normalized_changed.as_bytes().get(normalized_webhook.len())
                            == Some(&b'/'))
            })
            .collect())
    }

    pub fn increment_failure(&self, id: &str) -> Result<u32, OxenError> {
        let mut webhooks = self.list_all_webhooks()?;
        let mut count = 0;
        for w in &mut webhooks {
            if w.id == id {
                w.consecutive_failures += 1;
                count = w.consecutive_failures;
            }
        }
        self.save_webhooks(&webhooks)?;
        Ok(count)
    }

    pub fn reset_failure(&self, id: &str) -> Result<(), OxenError> {
        let mut webhooks = self.list_all_webhooks()?;
        for w in &mut webhooks {
            if w.id == id {
                w.consecutive_failures = 0;
            }
        }
        self.save_webhooks(&webhooks)?;
        Ok(())
    }

    pub fn get_config(&self) -> Result<WebhookConfig, OxenError> {
        if !self.config_path.exists() {
            return Ok(WebhookConfig::default());
        }
        let content = std::fs::read_to_string(&self.config_path)?;
        let config: WebhookConfig = serde_json::from_str(&content)?;
        Ok(config)
    }

    pub fn set_config(&self, config: &WebhookConfig) -> Result<(), OxenError> {
        let content = serde_json::to_string_pretty(config)?;
        std::fs::write(&self.config_path, content)?;
        Ok(())
    }

    pub fn stats(&self) -> Result<serde_json::Value, OxenError> {
        let webhooks = self.list_all_webhooks()?;
        Ok(serde_json::json!({
            "total_webhooks": webhooks.len(),
        }))
    }

    fn save_webhooks(&self, webhooks: &[Webhook]) -> Result<(), OxenError> {
        let content = serde_json::to_string_pretty(webhooks)?;
        std::fs::write(&self.webhooks_path, content)?;
        Ok(())
    }
}

fn normalize_webhook_path(path: &str) -> String {
    path.trim_start_matches('/')
        .trim_end_matches('/')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::webhook::WebhookMode;
    use tempfile::TempDir;

    fn create_test_db() -> (TempDir, WebhookDB) {
        let dir = TempDir::new().expect("Failed to create temp dir");
        let db = WebhookDB::new(dir.path()).expect("Failed to create WebhookDB");
        (dir, db)
    }

    fn add_test_webhook(db: &WebhookDB, path: &str, url: &str) -> Webhook {
        db.add_webhook(WebhookAddRequest {
            path: path.to_string(),
            webhook_url: url.to_string(),
            current_oxen_revision: None,
            purpose: "test".to_string(),
            contact: "test@example.com".to_string(),
        })
        .expect("Failed to add webhook")
    }

    #[test]
    fn test_webhook_db_add_and_list() {
        let (_dir, db) = create_test_db();
        let webhook = add_test_webhook(&db, "/data", "http://example.com/hook");

        let webhooks = db.list_all_webhooks().expect("Failed to list");
        assert_eq!(webhooks.len(), 1);
        assert_eq!(webhooks[0].id, webhook.id);
        assert_eq!(webhooks[0].path, "/data");
    }

    #[test]
    fn test_webhook_db_remove() {
        let (_dir, db) = create_test_db();
        let webhook = add_test_webhook(&db, "/data", "http://example.com/hook");

        db.remove_webhook(&webhook.id).expect("Failed to remove");
        let webhooks = db.list_all_webhooks().expect("Failed to list");
        assert!(webhooks.is_empty());
    }

    #[test]
    fn test_webhook_db_hierarchical_matching() {
        let (_dir, db) = create_test_db();
        add_test_webhook(&db, "/", "http://example.com/root");
        add_test_webhook(&db, "/a", "http://example.com/a");
        add_test_webhook(&db, "/a/b", "http://example.com/ab");
        add_test_webhook(&db, "/a/b/c", "http://example.com/abc");

        let matches = db
            .list_webhooks_for_path("/a/b/c/d.txt")
            .expect("Failed to match");
        assert_eq!(matches.len(), 4, "All parent webhooks should match");
    }

    #[test]
    fn test_webhook_db_sibling_isolation() {
        let (_dir, db) = create_test_db();
        add_test_webhook(&db, "/data", "http://example.com/data");
        add_test_webhook(&db, "/logs", "http://example.com/logs");
        add_test_webhook(&db, "/config", "http://example.com/config");

        let matches = db
            .list_webhooks_for_path("/data/new_file.txt")
            .expect("Failed to match");
        assert_eq!(matches.len(), 1, "Only /data webhook should match");
        assert_eq!(matches[0].path, "/data");
    }

    #[test]
    fn test_webhook_db_path_normalization() {
        let (_dir, db) = create_test_db();

        // Test various path formats
        let test_cases = vec![
            ("data/", true),
            ("data", true),
            ("/data/", true),
            ("/data", true),
            ("other", false),
        ];

        for (webhook_path, should_match) in test_cases {
            // Clear existing webhooks
            for w in db.list_all_webhooks().expect("list") {
                db.remove_webhook(&w.id).expect("remove");
            }
            add_test_webhook(&db, webhook_path, "http://example.com/hook");

            let matches = db
                .list_webhooks_for_path("/data/subdir/file.txt")
                .expect("match");
            assert_eq!(
                !matches.is_empty(),
                should_match,
                "webhook_path='{}' should {}match '/data/subdir/file.txt'",
                webhook_path,
                if should_match { "" } else { "NOT " }
            );
        }
    }

    #[test]
    fn test_webhook_db_root_webhook() {
        let (_dir, db) = create_test_db();
        add_test_webhook(&db, "/", "http://example.com/root");

        let test_paths = vec!["/file.txt", "/data/file.txt", "/deep/nested/path/file.txt"];

        for path in test_paths {
            let matches = db.list_webhooks_for_path(path).expect("match");
            assert_eq!(matches.len(), 1, "Root webhook should match '{}'", path);
        }
    }

    #[test]
    fn test_webhook_db_increment_failure() {
        let (_dir, db) = create_test_db();
        let webhook = add_test_webhook(&db, "/data", "http://example.com/hook");

        for i in 1..=5 {
            let count = db.increment_failure(&webhook.id).expect("increment");
            assert_eq!(count, i);
        }

        let webhooks = db.list_all_webhooks().expect("list");
        assert_eq!(webhooks[0].consecutive_failures, 5);
    }

    #[test]
    fn test_webhook_db_config() {
        let (_dir, db) = create_test_db();

        let default_config = db.get_config().expect("get config");
        assert_eq!(default_config.mode, WebhookMode::Inline);
        assert!(default_config.enabled);

        let new_config = WebhookConfig {
            mode: WebhookMode::Queue,
            enabled: true,
            queue_path: Some("/tmp/webhook_events".to_string()),
        };
        db.set_config(&new_config).expect("set config");

        let loaded = db.get_config().expect("get config");
        assert_eq!(loaded.mode, WebhookMode::Queue);
        assert_eq!(loaded.queue_path, Some("/tmp/webhook_events".to_string()));
    }
}
