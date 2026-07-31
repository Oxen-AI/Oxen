//! Task spawning that carries the caller's crash-reporting context onto the new task.
//!
//! `sentry-actix` installs a per-request Sentry hub — the route, the request, the breadcrumbs — as a
//! thread-local for the duration of each poll of the request future. A closure handed to
//! `tokio::task::spawn_blocking` runs on a pool thread that never saw that thread-local, so a panic
//! inside it reports with no route and no request. Spawning through this module hands the task a hub
//! inheriting the caller's scope, so its report names the request it came from.
//!
//! Use [`spawn_blocking`] in place of `tokio::task::spawn_blocking`, and [`inherit_hub`] around a
//! future handed to `tokio::spawn`, `JoinSet::spawn`, or a streaming response body. A task
//! deliberately detached from its request — one that outlives the response, like a background repo
//! delete — keeps using tokio's own spawn: stamping it with an already-answered request is more
//! misleading than reporting it with none.

use std::future::Future;
use std::sync::Arc;

use sentry::{Hub, SentryFuture, SentryFutureExt};
use tokio::task::JoinHandle;

/// Run a blocking closure on the blocking pool, reporting a panic inside it against the caller's
/// request. Otherwise identical to `tokio::task::spawn_blocking`.
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let hub = inherited_hub();
    tokio::task::spawn_blocking(move || Hub::run(hub, f))
}

/// Bind the caller's crash-reporting context to `future`, so a panic while it runs — on any thread,
/// at any later time — reports against the caller's request.
///
/// Call it where the request is still in scope (inside the handler), not from inside the task that
/// will run the future.
pub fn inherit_hub<F: Future>(future: F) -> SentryFuture<F> {
    future.bind_hub(inherited_hub())
}

/// A hub carrying a snapshot of the calling thread's Sentry scope, for a task that will run
/// somewhere that scope does not reach.
fn inherited_hub() -> Arc<Hub> {
    Arc::new(Hub::new_from_top(Hub::current()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test::{TestRequest, call_service, init_service};
    use actix_web::{App, HttpResponse, web};
    use sentry::protocol::Event;
    use sentry::test::{TestTransport, with_captured_events};
    use sentry::{ClientOptions, Level, capture_message, configure_scope};
    use tokio::runtime::Builder;

    /// The route name `sentry-actix` puts on the scope of every request hub, which is what a task
    /// spawned through this module has to end up reporting under.
    const ROUTE: &str = "GET /api/repos/{namespace}/{repo_name}";

    /// Captures the events `f` reports, with `ROUTE` on the scope. `f` runs on a current-thread
    /// runtime so the hub `with_captured_events` installs on this thread is the one a task inherits.
    fn events_from_async<F>(f: F) -> Vec<Event<'static>>
    where
        F: Future<Output = ()>,
    {
        with_captured_events(|| {
            configure_scope(|scope| scope.set_transaction(Some(ROUTE)));
            Builder::new_current_thread()
                .build()
                .expect("failed to build a current-thread runtime")
                .block_on(f);
        })
    }

    #[test]
    fn spawn_blocking_reports_against_the_callers_request() {
        let events = events_from_async(async {
            spawn_blocking(|| capture_message("from the blocking pool", Level::Error))
                .await
                .expect("the blocking task should not have panicked");
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].transaction.as_deref(), Some(ROUTE));
    }

    #[test]
    fn inherit_hub_reports_against_the_callers_request() {
        let events = events_from_async(async {
            let task = inherit_hub(async { capture_message("from a task", Level::Error) });
            tokio::spawn(task)
                .await
                .expect("the spawned task should not have panicked");
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].transaction.as_deref(), Some(ROUTE));
    }

    /// The bare spawn this module exists to replace: the pool thread resolves its own hub, which
    /// carries no route — the baseline the two assertions above are measured against.
    #[test]
    fn a_bare_spawn_loses_the_callers_request() {
        let events = events_from_async(async {
            tokio::task::spawn_blocking(|| capture_message("from the blocking pool", Level::Error))
                .await
                .expect("the blocking task should not have panicked");
        });

        assert!(events.iter().all(|event| event.transaction.is_none()));
    }

    /// The whole chain the server relies on: a real request through the same Sentry middleware
    /// configuration as `main`, reporting from a task the handler spawned.
    #[actix_web::test]
    async fn a_handler_task_reports_under_the_route_actix_matched() {
        async fn handler() -> HttpResponse {
            spawn_blocking(|| capture_message("from the blocking pool", Level::Error))
                .await
                .expect("the blocking task should not have panicked");
            HttpResponse::Ok().finish()
        }

        // The middleware derives each request hub from the one given here, so the test client is
        // reachable without binding it to any hub the rest of the test binary shares.
        let transport = TestTransport::new();
        let options = ClientOptions {
            dsn: Some(
                "https://public@sentry.invalid/1"
                    .parse()
                    .expect("the test DSN should parse"),
            ),
            transport: Some(Arc::new(Arc::clone(&transport))),
            ..Default::default()
        };
        let hub = Arc::new(Hub::new(
            Some(Arc::new(options.into())),
            Arc::new(Default::default()),
        ));

        let app = init_service(
            App::new()
                .route("/api/repos/{namespace}/{repo_name}", web::get().to(handler))
                .wrap(
                    sentry_actix::Sentry::builder()
                        .capture_server_errors(false)
                        .with_hub(hub)
                        .finish(),
                ),
        )
        .await;
        let request = TestRequest::get()
            .uri("/api/repos/ox/Cat-Dog-Classifier")
            .to_request();
        assert!(call_service(&app, request).await.status().is_success());

        let events = transport.fetch_and_clear_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].transaction.as_deref(), Some(ROUTE));
        let url = events[0]
            .request
            .as_ref()
            .and_then(|request| request.url.as_ref())
            .map(ToString::to_string);
        assert_eq!(
            url.as_deref(),
            Some("http://localhost:8080/api/repos/ox/Cat-Dog-Classifier")
        );
    }
}
