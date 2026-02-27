mod commands;
mod error;
mod state;

#[cfg(test)]
mod tests;

use state::AppState;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .manage(AppState::new())
        .invoke_handler(tauri::generate_handler![
            commands::auth::check_auth,
            commands::auth::login,
            commands::auth::save_auth_token,
            commands::auth::clear_auth_token,
            commands::files::scan_files,
            commands::repos::list_repos,
            commands::repos::create_repo,
            commands::upload::start_upload,
            commands::upload::cancel_upload,
            commands::preferences::get_preferences,
            commands::preferences::save_preferences,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
