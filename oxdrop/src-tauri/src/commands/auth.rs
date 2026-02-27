use crate::error::OxDropError;
use crate::state::AppState;
use liboxen::config::AuthConfig;
use liboxen::config::UserConfig;
use serde::{Deserialize, Serialize};
use tauri::State;

#[derive(Serialize)]
pub struct AuthInfo {
    pub token: String,
    pub user: UserInfo,
    pub host: String,
}

#[derive(Serialize)]
pub struct UserInfo {
    pub name: String,
    pub email: String,
}

#[derive(Deserialize)]
struct LoginResponse {
    status: String,
    user: LoginUser,
}

#[derive(Deserialize)]
struct LoginUser {
    api_key: String,
    name: String,
    email: String,
}

#[tauri::command]
pub async fn check_auth(state: State<'_, AppState>) -> Result<Option<AuthInfo>, OxDropError> {
    let auth_config = match AuthConfig::get() {
        Ok(config) => config,
        Err(_) => return Ok(None),
    };

    let host = &state.host;
    let token = match auth_config.auth_token_for_host(host) {
        Some(t) => t,
        None => return Ok(None),
    };

    let user_config = UserConfig::get_or_create().map_err(OxDropError::from)?;

    Ok(Some(AuthInfo {
        token,
        user: UserInfo {
            name: user_config.name,
            email: user_config.email,
        },
        host: host.clone(),
    }))
}

#[tauri::command]
pub async fn login(
    state: State<'_, AppState>,
    identifier: String,
    password: String,
    host: Option<String>,
) -> Result<AuthInfo, OxDropError> {
    let host = host.unwrap_or_else(|| state.host.clone());
    let scheme = &state.scheme;
    let url = format!("{}://{}/api/login", scheme, host);

    let client = reqwest::Client::new();
    let res = client
        .post(&url)
        .json(&serde_json::json!({
            "identifier": identifier,
            "password": password,
        }))
        .send()
        .await
        .map_err(|e| OxDropError {
            message: format!("Connection error: {}", e),
            code: "CONNECTION_ERROR".to_string(),
        })?;

    if res.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Err(OxDropError {
            message: "Invalid username or password.".to_string(),
            code: "BAD_CREDENTIALS".to_string(),
        });
    }

    if !res.status().is_success() {
        return Err(OxDropError {
            message: format!("Server returned {}", res.status()),
            code: "SERVER_ERROR".to_string(),
        });
    }

    let login_res: LoginResponse = res.json().await.map_err(|e| OxDropError {
        message: format!("Failed to parse response: {}", e),
        code: "PARSE_ERROR".to_string(),
    })?;

    if login_res.status != "success" {
        return Err(OxDropError {
            message: "Login was not successful.".to_string(),
            code: "LOGIN_FAILED".to_string(),
        });
    }

    // Save the API key to auth config
    let mut auth_config = AuthConfig::get_or_create().map_err(OxDropError::from)?;
    auth_config.add_host_auth_token(&host, &login_res.user.api_key);
    auth_config.save_default().map_err(OxDropError::from)?;

    Ok(AuthInfo {
        token: login_res.user.api_key,
        user: UserInfo {
            name: login_res.user.name,
            email: login_res.user.email,
        },
        host,
    })
}

#[tauri::command]
pub async fn save_auth_token(
    state: State<'_, AppState>,
    token: String,
    host: Option<String>,
) -> Result<(), OxDropError> {
    let host = host.unwrap_or_else(|| state.host.clone());
    let mut auth_config = AuthConfig::get_or_create().map_err(OxDropError::from)?;
    auth_config.add_host_auth_token(&host, &token);
    auth_config.save_default().map_err(OxDropError::from)?;
    Ok(())
}

#[tauri::command]
pub async fn clear_auth_token(state: State<'_, AppState>) -> Result<(), OxDropError> {
    let host = &state.host;
    let mut auth_config = AuthConfig::get_or_create().map_err(OxDropError::from)?;
    auth_config.add_host_auth_token(host, &String::new());
    auth_config.save_default().map_err(OxDropError::from)?;
    Ok(())
}
