use liboxen::{
    core::v0_10_0::cache::commit_cacher,
    core::webhook_dispatcher::WebhookDispatcher,
    model::{Commit, LocalRepository},
};
use serde::{Deserialize, Serialize};

use super::Runnable;

#[derive(Serialize, Deserialize, Debug)]
pub struct PostPushComplete {
    pub commit: Commit,
    pub repo: LocalRepository,
}

impl Runnable for PostPushComplete {
    fn run(&self) {
        log::debug!(
            "Running cachers for commit {:?} on repo {:?} from redis queue",
            self.commit.id,
            &self.repo.path
        );
        let force = false;
        match commit_cacher::run_all(&self.repo, &self.commit, force) {
            Ok(_) => {
                log::debug!(
                    "Cachers ran successfully for commit {:?} on repo {:?} from redis queue",
                    self.commit.id,
                    &self.repo.path
                );
            }
            Err(e) => {
                log::error!(
                    "Cachers failed to run for commit {:?} on repo {:?} from redis queue",
                    self.commit.id,
                    &self.repo.path
                );
                log::error!("Error: {:?}", e);
            }
        }
        
        // Trigger webhook notifications for the push
        match WebhookDispatcher::from_repo(&self.repo) {
            Ok(dispatcher) => {
                log::debug!("Triggering webhook notifications for push complete");
                let rt = match tokio::runtime::Runtime::new() {
                    Ok(rt) => rt,
                    Err(e) => {
                        log::error!("Failed to create async runtime for webhook dispatch: {}", e);
                        return;
                    }
                };
                
                rt.block_on(async {
                    match dispatcher.dispatch_webhook_event(&self.repo, &self.commit).await {
                        Ok(_) => {
                            log::debug!("Webhook notifications dispatched successfully for push complete");
                        }
                        Err(e) => {
                            log::error!("Failed to dispatch webhook notifications for push complete: {}", e);
                        }
                    }
                });
            }
            Err(e) => {
                log::error!("Failed to create webhook dispatcher for push complete: {}", e);
            }
        }
    }
}
