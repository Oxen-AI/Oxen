use dotenv::dotenv;
use dotenv::from_filename;
use liboxen::config::UserConfig;
use liboxen::model::User;
use liboxen::util;

pub mod app_data;
pub mod auth;
pub mod controllers;
pub mod errors;
pub mod helpers;
pub mod middleware;
pub mod params;
pub mod queue_poller;
pub mod queues;
pub mod routes;
pub mod services;
pub mod tasks;
pub mod test;

extern crate log;
extern crate lru;

use actix_web::middleware::{Condition, Logger};
use actix_web::{web, App, HttpServer};
use actix_web_httpauth::middleware::HttpAuthentication;

use clap::{Arg, Command};

use std::env;
use std::path::{Path, PathBuf};

const VERSION: &str = liboxen::constants::OXEN_VERSION;

const ADD_USER_USAGE: &str =
    "Usage: `oxen-server add-user -e <email> -n <name> -o user_config.toml`";

const START_SERVER_USAGE: &str = "Usage: `oxen-server start -i 0.0.0.0 -p 3000`";

const INVALID_PORT_MSG: &str = "Port must a valid number between 0-65535";

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();

    match from_filename("src/server/.env.local") {
        Ok(_) => log::debug!("Loaded .env file from current directory"),
        Err(e) => log::debug!("Failed to load .env file: {}", e),
    }

    util::logging::init_logging();

    let sync_dir = match env::var("SYNC_DIR") {
        Ok(dir) => dir,
        Err(_) => String::from("data"),
    };

    let command = Command::new("oxen-server")
        .version(VERSION)
        .about("Oxen Server")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(
            Command::new("start")
                .about(START_SERVER_USAGE)
                .arg(
                    Arg::new("ip")
                        .long("ip")
                        .short('i')
                        .default_value("0.0.0.0")
                        .default_missing_value("always")
                        .help("What host to bind the server to")
                        .action(clap::ArgAction::Set),
                )
                .arg(
                    Arg::new("port")
                        .long("port")
                        .short('p')
                        .default_value("3000")
                        .default_missing_value("always")
                        .help("What port to bind the server to")
                        .action(clap::ArgAction::Set),
                )
                .arg(
                    Arg::new("auth")
                        .long("auth")
                        .short('a')
                        .help("Start the server with token-based authentication enforced")
                        .action(clap::ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("add-user")
                .about(ADD_USER_USAGE)
                .arg(
                    Arg::new("email")
                        .long("email")
                        .short('e')
                        .help("Users email address")
                        .required(true)
                        .action(clap::ArgAction::Set),
                )
                .arg(
                    Arg::new("name")
                        .long("name")
                        .short('n')
                        .help("Users name that will show up in the commits")
                        .required(true)
                        .action(clap::ArgAction::Set),
                )
                .arg(
                    Arg::new("output")
                        .long("output")
                        .short('o')
                        .default_value("user_config.toml")
                        .default_missing_value("always")
                        .help("Where to write the output config file to give to the user")
                        .action(clap::ArgAction::Set),
                ),
        );
    let matches = command.get_matches();

    match matches.subcommand() {
        Some(("start", sub_matches)) => {
            match (
                sub_matches.get_one::<String>("ip"),
                sub_matches.get_one::<String>("port"),
            ) {
                (Some(host), Some(port)) => {
                    let port: u16 = port.parse::<u16>().expect(INVALID_PORT_MSG);
                    println!("🐂 v{VERSION}");
                    println!("Running on {host}:{port}");
                    println!("Syncing to directory: {sync_dir}");
                    let enable_auth = sub_matches.get_flag("auth");

                    log::debug!("initializing queue");
                    let queue = queue_poller::init_queue();
                    log::debug!("initialized queue");
                    let data = app_data::OxenAppData::new(PathBuf::from(sync_dir), queue.clone());
                    // Poll for post-commit tasks in background
                    log::debug!("initialized app data, spawning polling worker");
                    tokio::spawn(async move { queue_poller::poll_queue(queue.clone()).await });

                    HttpServer::new(move || {
                        App::new()
                            .app_data(data.clone())
                            .route("/api/version", web::get().to(controllers::version::index))
                            .route(
                                "/api/min_version",
                                web::get().to(controllers::version::min_version),
                            )
                            .route("/api/health", web::get().to(controllers::health::index))
                            .route(
                                "/api/namespaces",
                                web::get().to(controllers::namespaces::index),
                            )
                            .route(
                                "/api/namespaces/{namespace}",
                                web::get().to(controllers::namespaces::show),
                            )
                            .route(
                                "/api/migrations/{migration_tstamp}",
                                web::get().to(controllers::migrations::list_unmigrated),
                            )
                            .wrap(Condition::new(
                                enable_auth,
                                HttpAuthentication::bearer(auth::validator::validate),
                            ))
                            .service(web::scope("/api/repos").configure(routes::config))
                            .default_service(web::route().to(controllers::not_found::index))
                            .wrap(Logger::default())
                            .wrap(Logger::new("user agent is %a %{User-Agent}i"))
                    })
                    .bind((host.to_owned(), port))?
                    .run()
                    .await
                }
                _ => {
                    eprintln!("{START_SERVER_USAGE}");
                    Ok(())
                }
            }
        }
        Some(("add-user", sub_matches)) => {
            match (
                sub_matches.get_one::<String>("email"),
                sub_matches.get_one::<String>("name"),
                sub_matches.get_one::<String>("output"),
            ) {
                (Some(email), Some(name), Some(output)) => {
                    let path = Path::new(&sync_dir);
                    log::debug!("Saving to sync dir: {:?}", path);
                    if let Ok(keygen) = auth::access_keys::AccessKeyManager::new(path) {
                        let new_user = User {
                            name: name.to_string(),
                            email: email.to_string(),
                        };
                        match keygen.create(&new_user) {
                            Ok((user, token)) => {
                                let cfg = UserConfig::from_user(&user);
                                match cfg.save(Path::new(output)) {
                                    Ok(_) => {
                                        println!("User access token created:\n\n{token}\n\nTo give user access have them run the command `oxen config --auth <HOST> <TOKEN>`")
                                    }
                                    Err(error) => {
                                        eprintln!("Err: {error:?}");
                                    }
                                }
                            }
                            Err(err) => {
                                eprintln!("Err: {err}")
                            }
                        }
                    }
                }
                _ => {
                    eprintln!("{ADD_USER_USAGE}")
                }
            }

            Ok(())
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachabe!()
    }
}
