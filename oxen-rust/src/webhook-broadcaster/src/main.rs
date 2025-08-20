use clap::Parser;
use liboxen::core::db::webhooks::WebhookDB;
use liboxen::core::webhook_dispatcher::WebhookEvent;
use liboxen::core::webhooks::WebhookNotifier;
use liboxen::model::LocalRepository;
use serde_json;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Parser)]
#[command(name = "oxen-webhook-broadcaster")]
#[command(about = "Webhook broadcasting service for Oxen VCS")]
struct Cli {
    /// Path to monitor for webhook events
    #[arg(short, long, default_value = ".")]
    path: PathBuf,
    
    /// Polling interval in milliseconds
    #[arg(short, long, default_value = "1000")]
    interval: u64,
    
    /// Queue file to monitor for events
    #[arg(short, long, default_value = "webhook_events")]
    queue_file: String,
    
    /// Verbose logging
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    
    if cli.verbose {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
    } else {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    }

    log::info!("Starting Oxen Webhook Broadcaster");
    log::info!("Monitoring path: {}", cli.path.display());
    log::info!("Polling interval: {}ms", cli.interval);

    let mut notifier = WebhookNotifier::new();
    let mut last_position = 0;

    loop {
        match process_webhook_events(&cli, &mut notifier, &mut last_position).await {
            Ok(processed) => {
                if processed > 0 {
                    log::info!("Processed {} webhook events", processed);
                }
            }
            Err(e) => {
                log::error!("Error processing webhook events: {}", e);
            }
        }

        sleep(Duration::from_millis(cli.interval)).await;
    }
}

async fn process_webhook_events(
    cli: &Cli,
    notifier: &mut WebhookNotifier,
    last_position: &mut usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    let queue_path = cli.path.join(".oxen").join(&cli.queue_file);
    
    if !queue_path.exists() {
        return Ok(0);
    }

    let content = tokio::fs::read_to_string(&queue_path).await?;
    let lines: Vec<&str> = content.lines().collect();
    
    let mut processed = 0;
    
    // Process new lines since last position
    for line in lines.iter().skip(*last_position) {
        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<WebhookEvent>(line) {
            Ok(event) => {
                log::debug!("Processing event: {:?}", event);
                match process_single_event(event, notifier).await {
                    Ok(_) => {
                        processed += 1;
                        log::debug!("Successfully processed webhook event");
                    }
                    Err(e) => {
                        log::error!("Failed to process webhook event: {}", e);
                    }
                }
            }
            Err(e) => {
                log::warn!("Failed to parse webhook event: {}, line: {}", e, line);
            }
        }
    }
    
    *last_position = lines.len();
    Ok(processed)
}

async fn process_single_event(
    event: WebhookEvent,
    notifier: &mut WebhookNotifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo_path = PathBuf::from(&event.repo_path);
    
    // For commit events, trigger webhooks for root path
    match event.event_type.as_str() {
        "commit" => {
            let changed_path = "/";
            match notifier.notify_path_changed(&repo_path, changed_path).await {
                Ok(count) => {
                    if count > 0 {
                        log::info!("Sent {} webhook notifications for commit {}", count, event.commit_id);
                    }
                }
                Err(e) => {
                    log::error!("Failed to send webhook notifications for commit {}: {}", event.commit_id, e);
                    return Err(Box::new(e));
                }
            }
        }
        _ => {
            log::warn!("Unknown event type: {}", event.event_type);
        }
    }

    Ok(())
}