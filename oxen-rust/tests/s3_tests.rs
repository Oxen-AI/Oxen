use liboxen::storage::{S3VersionStore, VersionStore};

use aws_sdk_s3::Client;
use std::sync::Arc;
use tokio;
use uuid::Uuid;

pub const DEFAULT_TEST_S3_BUCKET: &str = "oxen-server-s3-integration-test";

#[tokio::test]
async fn test_s3_init() {
    // Use the existing S3 bucket for testing
    let bucket = test_s3_bucket();

    // Use a random prefix to isolate the test files
    let prefix = format!("integration-test/{}", Uuid::new_v4());

    // Create S3 version store
    let s3_version_store = S3VersionStore::new(bucket.clone(), prefix.clone());
    let client = s3_version_store.init_client().await;

    // Test init()
    let result = s3_version_store.init().await;

    assert!(result.is_ok(), "S3 init() failed: {:?}", result.err());

    cleanup_bucket(&client.unwrap(), &bucket, &prefix).await;
}

pub fn test_s3_bucket() -> String {
    match std::env::var("OXEN_TEST_S3_BUCKET") {
        Ok(bucket) => bucket,
        Err(_err) => String::from(DEFAULT_TEST_S3_BUCKET),
    }
}

async fn cleanup_bucket(client: &Arc<Client>, bucket: &str, prefix: &str) {
    let objs = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await
        .unwrap();

    if let Some(contents) = objs.contents {
        for obj in contents {
            if let Some(key) = obj.key {
                let _ = client.delete_object().bucket(bucket).key(key).send().await;
            }
        }
    }
    println!("Cleaned up test object");
}
