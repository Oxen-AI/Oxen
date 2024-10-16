use liboxen::{
    core::v0_10_0::cache::commit_cacher,
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
        // sleep to debug
        println!("Here is the commit id: {}", self.commit.id);
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
    }
}
