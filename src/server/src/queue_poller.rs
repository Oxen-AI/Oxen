use liboxen::core::v0_10_0::cache::cacher_status::CacherStatus;
use liboxen::core::v0_10_0::cache::commit_cacher;
use std::time::Duration;
use tokio::time::sleep;

use crate::helpers::get_redis_connection;
use crate::queues::{InMemoryTaskQueue, RedisTaskQueue, TaskQueue};
use crate::tasks::{Runnable, Task};

pub async fn poll_queue(mut queue: TaskQueue) {
    log::debug!("Starting queue poller");
    loop {
        match queue.pop() {
            Some(task) => {
                log::debug!("Got queue item: {:?}", task);

                // to ensure we don't block the poller, we run the task in an OS thread.
                tokio::task::spawn_blocking(move || {
                    let result = std::panic::catch_unwind(|| {
                        task.run();
                    });
                    if let Err(e) = result {
                        log::error!("Error or panic processing task {:?}", e);
                        // Handle task failure
                        // Set the task to failed
                        match task {
                            Task::PostPushComplete(post_push_complete) => {
                                let repo = post_push_complete.repo;
                                let commit = post_push_complete.commit;

                                match commit_cacher::set_all_cachers_status(
                                    &repo,
                                    &commit,
                                    CacherStatus::failed("Panic in task execution"),
                                ) {
                                    Ok(_) => log::debug!("Set all cachers to failed status"),
                                    Err(e) => log::error!(
                                        "Error setting all cachers to failed status: {:?}",
                                        e
                                    ),
                                }
                            }
                        }
                    }
                });
            }
            None => {
                // log::debug!("No queue items found, sleeping");
                sleep(Duration::from_millis(1000)).await;
            }
        }
    }
}

// If redis connection is available, use redis queue, else in-memory
pub fn init_queue() -> TaskQueue {
    match get_redis_connection() {
        Ok(pool) => {
            println!("connecting to redis established, initializing queue");
            TaskQueue::Redis(RedisTaskQueue { pool })
        }
        Err(_) => {
            println!("Failed to connect to Redis. Falling back to in-memory queue.");
            TaskQueue::InMemory(InMemoryTaskQueue::new())
        }
    }
}
