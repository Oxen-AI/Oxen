use crate::error::OxenError;
use crate::model::{LocalRepository, Commit};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WebhookMode {
    Inline,    // Current behavior - direct HTTP in background thread
    Queue,     // Write events to queue for external processor
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub mode: WebhookMode,
    pub queue_path: Option<String>,  // For queue mode - where to write events
    pub enabled: bool,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            mode: WebhookMode::Inline,
            queue_path: None,
            enabled: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEvent {
    pub repo_path: String,
    pub commit_id: String,
    pub timestamp: SystemTime,
    pub event_type: String,
}

pub struct WebhookDispatcher {
    pub config: WebhookConfig,
}

impl WebhookDispatcher {
    pub fn new(config: WebhookConfig) -> Self {
        Self { config }
    }

    pub fn from_repo(repo: &LocalRepository) -> Result<Self, OxenError> {
        let config = Self::load_config(repo)?;
        Ok(Self::new(config))
    }

    fn load_config(repo: &LocalRepository) -> Result<WebhookConfig, OxenError> {
        let config_path = repo.path.join(".oxen").join("webhook_config.json");
        
        if config_path.exists() {
            let content = std::fs::read_to_string(&config_path)?;
            let config: WebhookConfig = serde_json::from_str(&content)
                .map_err(|e| OxenError::basic_str(format!("Invalid webhook config: {}", e)))?;
            Ok(config)
        } else {
            Ok(WebhookConfig::default())
        }
    }

    pub async fn dispatch_webhook_event(&self, repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
        if !self.config.enabled {
            log::debug!("Webhooks disabled, skipping notification");
            return Ok(());
        }

        match self.config.mode {
            WebhookMode::Inline => {
                log::debug!("Dispatching webhook inline");
                self.dispatch_inline(repo, commit).await
            }
            WebhookMode::Queue => {
                log::debug!("Dispatching webhook to queue");
                self.dispatch_to_queue(repo, commit).await
            }
        }
    }

    async fn dispatch_inline(&self, repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
        // Use the existing inline webhook notification system
        use crate::core::webhooks::WebhookNotifier;
        
        let repo_path = repo.path.clone();
        let commit_id = commit.id.clone();
        
        // Spawn async task to handle webhook notifications without blocking commit
        std::thread::spawn(move || {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    log::error!("Failed to create async runtime for webhooks: {}", e);
                    return;
                }
            };
            
            rt.block_on(async move {
                let mut notifier = WebhookNotifier::new();
                
                // For now, notify for the root path of the repository
                let changed_path = "/";  
                
                match notifier.notify_path_changed(&repo_path, changed_path).await {
                    Ok(count) => {
                        if count > 0 {
                            log::info!("Sent {} webhook notifications for commit {}", count, commit_id);
                            println!("🔔 {} webhook callbacks done for commit {}", count, commit_id);
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to send webhook notifications for commit {}: {}", commit_id, e);
                    }
                }
            });
        });
        
        Ok(())
    }

    async fn dispatch_to_queue(&self, repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
        let event = WebhookEvent {
            repo_path: repo.path.to_string_lossy().to_string(),
            commit_id: commit.id.clone(),
            timestamp: SystemTime::now(),
            event_type: "commit".to_string(),
        };

        let queue_path = self.config.queue_path
            .as_ref()
            .map(|p| Path::new(p).to_path_buf())
            .unwrap_or_else(|| repo.path.join(".oxen").join("webhook_events"));

        // Ensure queue directory exists
        if let Some(parent) = queue_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Append event to queue file (simple file-based queue for now)
        let event_json = serde_json::to_string(&event)
            .map_err(|e| OxenError::basic_str(format!("Failed to serialize webhook event: {}", e)))?;
        
        use std::fs::OpenOptions;
        use std::io::Write;
        
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&queue_path)
            .map_err(|e| OxenError::basic_str(format!("Failed to open webhook queue: {}", e)))?;
        
        writeln!(file, "{}", event_json)
            .map_err(|e| OxenError::basic_str(format!("Failed to write webhook event: {}", e)))?;
        
        log::info!("Queued webhook event for commit {} in {}", commit.id, queue_path.display());
        Ok(())
    }

    pub fn save_config(&self, repo: &LocalRepository) -> Result<(), OxenError> {
        let config_path = repo.path.join(".oxen").join("webhook_config.json");
        let config_json = serde_json::to_string_pretty(&self.config)
            .map_err(|e| OxenError::basic_str(format!("Failed to serialize webhook config: {}", e)))?;
        
        std::fs::write(&config_path, config_json)
            .map_err(|e| OxenError::basic_str(format!("Failed to write webhook config: {}", e)))?;
        
        Ok(())
    }
}