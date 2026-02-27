use crate::error::OxDropError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct Preferences {
    #[serde(default)]
    pub recent_uploads: Vec<RecentUpload>,
    pub last_used_repo: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RecentUpload {
    pub folder_name: String,
    pub repo: String,
    pub remote_path: String,
    pub file_count: u64,
    pub total_size: u64,
    pub timestamp: String,
}

fn prefs_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("~/.config"))
        .join("oxen")
        .join("desktop.toml")
}

#[tauri::command]
pub async fn get_preferences() -> Result<Preferences, OxDropError> {
    let path = prefs_path();
    if !path.exists() {
        return Ok(Preferences::default());
    }
    let content = std::fs::read_to_string(&path)?;
    let prefs: Preferences = toml::from_str(&content)?;
    Ok(prefs)
}

#[tauri::command]
pub async fn save_preferences(prefs: Preferences) -> Result<(), OxDropError> {
    let path = prefs_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let content = toml::to_string_pretty(&prefs)?;
    std::fs::write(&path, content)?;
    Ok(())
}
