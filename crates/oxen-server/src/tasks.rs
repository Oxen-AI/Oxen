//! Task spawning that carries the caller's crash-reporting and tracing context onto the new task.
//!
//! Both contexts are thread-locals bound only around polls of the request future. `sentry-actix`
//! installs a per-request hub — the route, the request, the breadcrumbs. `tracing` keeps the
//! current span, which the HTTP root span occupies for the length of the request. A closure handed
//! to `tokio::task::spawn_blocking` runs on a pool thread that saw neither, so a panic inside it
//! reports with no route and no request, and any span it opens is exported as a root of its own
//! rather than as part of the request's trace. Since essentially every sizeable piece of work in
//! this server runs on the blocking pool, that is the difference between a trace of the request and
//! an empty one.
//!
//! Spawning through this module hands the task a hub inheriting the caller's scope and re-enters
//! the caller's span around it.
//!
//! Use [`spawn_blocking`] in place of `tokio::task::spawn_blocking`, and [`inherit_hub`] around a
//! future handed to `tokio::spawn`, `JoinSet::spawn`, or a streaming response body. A task
//! deliberately detached from its request — one that outlives the response, like a background repo
//! delete — keeps using tokio's own spawn: stamping it with an already-answered request is more
//! misleading than reporting it with none.

use std::future::Future;
use std::panic::Location;
use std::sync::Arc;

use sentry::{Hub, SentryFuture, SentryFutureExt};
use tokio::task::JoinHandle;
use tracing::instrument::Instrumented;
use tracing::{Instrument, Span};

/// Run a blocking closure on the blocking pool, reporting a panic inside it against the caller's
/// request and timing it as a span under the caller's. Otherwise identical to
/// `tokio::task::spawn_blocking`.
///
/// The span is named for the call site, so a trace distinguishes one blocking operation from
/// another, and it opens at the spawn rather than at the closure's first line — a span far longer
/// than its work is a saturated blocking pool.
///
/// This is the right choice for one coherent operation per request. For work dispatched once per
/// item in a loop, use [`spawn_blocking_per_item`] instead.
#[track_caller]
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let caller = Location::caller();
    spawn_blocking_in(
        tracing::info_span!(
            "blocking task",
            code.file.path = caller.file(),
            code.line.number = caller.line(),
        ),
        f,
    )
}

/// [`spawn_blocking`] for work dispatched once per item in a loop: it stays inside the caller's
/// span rather than opening one of its own.
///
/// A span per item is the wrong granularity for a bulk endpoint. One upload request carries as
/// many files as fit in a transfer segment — thousands of small ones — and a span each would bury
/// the operation's own spans and overrun the exporter's queue on a single push. The work is still
/// timed, as part of the operation that dispatched it, and a panic still reports against the
/// caller's request.
pub fn spawn_blocking_per_item<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    spawn_blocking_in(Span::current(), f)
}

/// Run `f` on the blocking pool inside `span`, with a hub inheriting the caller's scope.
///
/// Both must be resolved on the calling thread: a pool thread sees neither.
fn spawn_blocking_in<F, R>(span: Span, f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let hub = inherited_hub();
    tokio::task::spawn_blocking(move || span.in_scope(|| Hub::run(hub, f)))
}

/// Bind the caller's crash-reporting and tracing context to `future`, so a panic while it runs — on
/// any thread, at any later time — reports against the caller's request, and the spans it opens
/// belong to the caller's trace.
///
/// Call it where the request is still in scope (inside the handler), not from inside the task that
/// will run the future.
pub fn inherit_hub<F: Future>(future: F) -> Instrumented<SentryFuture<F>> {
    future.bind_hub(inherited_hub()).instrument(Span::current())
}

/// A hub carrying a snapshot of the calling thread's Sentry scope, for a task that will run
/// somewhere that scope does not reach.
fn inherited_hub() -> Arc<Hub> {
    Arc::new(Hub::new_from_top(Hub::current()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::body::to_bytes;
    use actix_web::test::{TestRequest, call_service, init_service};
    use actix_web::{App, HttpResponse, web};
    use liboxen::error::OxenError;
    use sentry::protocol::Event;
    use sentry::test::TestTransport;
    use sentry::{ClientOptions, Level, capture_message};

    use crate::helpers::stream_with_heartbeat;

    /// The route actix matches, the URI that matches it, and the transaction name `sentry-actix`
    /// derives from the two — what a task spawned anywhere under this request must report against.
    const ROUTE_PATTERN: &str = "/api/repos/{namespace}/{repo_name}";
    const REQUEST_URI: &str = "/api/repos/ox/Cat-Dog-Classifier";
    const ROUTE: &str = "GET /api/repos/{namespace}/{repo_name}";

    /// Drives one GET through the same Sentry middleware configuration as `main` and returns the
    /// events `handler`'s tasks reported. The response body is read to the end, so work deferred
    /// behind a streaming body has run by the time the events are collected.
    ///
    /// The middleware derives each request hub from the one built here, so the capturing client is
    /// never bound to a hub the rest of the test binary shares.
    async fn events_from_request<F, Fut>(handler: F) -> Vec<Event<'static>>
    where
        F: Fn() -> Fut + Clone + 'static,
        Fut: Future<Output = HttpResponse> + 'static,
    {
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
                .route(ROUTE_PATTERN, web::get().to(handler))
                .wrap(
                    sentry_actix::Sentry::builder()
                        .capture_server_errors(false)
                        .with_hub(hub)
                        .finish(),
                ),
        )
        .await;
        let response = call_service(&app, TestRequest::get().uri(REQUEST_URI).to_request()).await;
        assert!(response.status().is_success());
        to_bytes(response.into_body())
            .await
            .expect("the response body should read to the end");

        transport.fetch_and_clear_events()
    }

    /// How the nine converted `spawn_blocking` call sites spawn: the handler awaits the task before
    /// it responds.
    #[actix_web::test]
    async fn a_task_the_handler_awaits_reports_under_the_route() {
        let events = events_from_request(|| async {
            spawn_blocking(|| capture_message("from the blocking pool", Level::Error))
                .await
                .expect("the blocking task should not have panicked");
            HttpResponse::Ok().finish()
        })
        .await;

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].transaction.as_deref(), Some(ROUTE));
        let url = events[0]
            .request
            .as_ref()
            .and_then(|request| request.url.as_ref())
            .map(ToString::to_string);
        let expected_url = format!("http://localhost:8080{REQUEST_URI}");
        assert_eq!(url.as_deref(), Some(expected_url.as_str()));
    }

    /// How `create_nodes` spawns, and the hardest case: the handler returns at once and the work
    /// runs while the response body streams, after the middleware's own hub binding is gone. Nests
    /// `inherit_hub` (applied by `stream_with_heartbeat`) into `spawn_blocking`, so it also covers
    /// a hub inherited through two levels.
    #[actix_web::test]
    async fn a_task_deferred_behind_a_streaming_body_reports_under_the_route() {
        let events = events_from_request(|| async {
            stream_with_heartbeat(async {
                spawn_blocking(|| capture_message("from the streamed body", Level::Error))
                    .await
                    .expect("the blocking task should not have panicked");
                Ok::<_, OxenError>(())
            })
        })
        .await;

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].transaction.as_deref(), Some(ROUTE));
    }

    /// The bare spawn this module exists to replace: the pool thread resolves its own hub, which
    /// carries no route — the baseline the two assertions above are measured against. The tracing
    /// half of the same contract is asserted in `tests/task_span_propagation.rs`.
    #[actix_web::test]
    async fn a_bare_spawn_loses_the_route() {
        let events = events_from_request(|| async {
            tokio::task::spawn_blocking(|| capture_message("from the blocking pool", Level::Error))
                .await
                .expect("the blocking task should not have panicked");
            HttpResponse::Ok().finish()
        })
        .await;

        assert!(events.iter().all(|event| event.transaction.is_none()));
    }
}
