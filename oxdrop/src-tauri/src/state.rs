use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct UploadJob {
    pub id: String,
    pub cancelled: Arc<std::sync::atomic::AtomicBool>,
    pub workspace_id: Option<String>,
    pub repo: String,
}

pub struct AppState {
    pub host: String,
    pub scheme: String,
    pub upload_jobs: Mutex<HashMap<String, UploadJob>>,
}

impl AppState {
    pub fn new() -> Self {
        let (host, scheme) = if cfg!(debug_assertions) {
            ("localhost:3001".to_string(), "http".to_string())
        } else {
            ("hub.oxen.ai".to_string(), "https".to_string())
        };

        AppState {
            host,
            scheme,
            upload_jobs: Mutex::new(HashMap::new()),
        }
    }
}
