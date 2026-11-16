//! Locks in what Sentry receives from work liboxen spawns under a request once oxen-server has
//! registered its hub capture: events and panics report under the request's route, where the same
//! work spawned the bare way reports under none. A task oxen-server spawns detached reports under
//! none on purpose, since it outlives the request. A task a response body spawns as it is read,
//! as an S3 download's part tasks are, reports under the route too.
//!
//! Lives as its own integration test binary because `sentry::init` binds the process hub, which a
//! pool thread outside any request falls back to. Without it, the bare spawn's event would reach
//! no client, and the baseline would pass on no events at all.

use std::sync::Arc;

use actix_web::body::to_bytes;
use actix_web::middleware::from_fn;
use actix_web::test::{TestRequest, call_service, init_service};
use actix_web::{App, HttpResponse, web};
use bytes::Bytes;
use futures_util::stream;
use liboxen::error::OxenError;
use liboxen::tasks;
use oxen_server::errors::OxenHttpError;
use oxen_server::middleware::{read_body_under_request_hub, run_request_as_task};
use oxen_server::tasks::{register_liboxen_hub_capture, spawn_detached};
use sentry::test::TestTransport;
use sentry::{ClientOptions, Level, capture_message};

async fn bare_spawn() -> HttpResponse {
    #[allow(
        clippy::disallowed_methods,
        reason = "the bare spawn liboxen's helpers are measured against"
    )]
    let reported = tokio::task::spawn_blocking(|| capture_message("bare", Level::Error)).await;
    reported.expect("the bare task should not have panicked");
    HttpResponse::Ok().finish()
}

async fn liboxen_spawns() -> HttpResponse {
    tasks::spawn_blocking(|| capture_message("liboxen blocking task", Level::Error))
        .await
        .expect("the blocking task should not have panicked");
    tasks::spawn(async { capture_message("liboxen task", Level::Error) })
        .await
        .expect("the task should not have panicked");
    HttpResponse::Ok().finish()
}

/// A body that spawns its task while it is read, after the handler has returned.
async fn streamed_spawn() -> HttpResponse {
    let body = stream::once(async {
        tasks::spawn(async { capture_message("spawned as read", Level::Error) })
            .await
            .map_err(std::io::Error::other)?;
        Ok::<_, std::io::Error>(Bytes::new())
    });
    HttpResponse::Ok().streaming(body)
}

async fn detached_spawn() -> HttpResponse {
    spawn_detached(async { capture_message("detached task", Level::Error) })
        .await
        .expect("the detached task should not have panicked");
    HttpResponse::Ok().finish()
}

async fn liboxen_panic() -> Result<HttpResponse, OxenHttpError> {
    let joined: Result<(), OxenError> = tasks::spawn_blocking(|| panic!("a liboxen task panicked"))
        .await
        .map_err(OxenError::from);
    joined?;
    Ok(HttpResponse::Ok().finish())
}

/// One test, since the process hub is process-global.
#[actix_web::test]
async fn liboxen_spawns_report_under_the_request() {
    let transport = TestTransport::new();
    let mut options = ClientOptions::default();
    options.dsn = Some(
        "https://public@sentry.invalid/1"
            .parse()
            .expect("the test DSN should parse"),
    );
    options.transport = Some(Arc::new(Arc::clone(&transport)));
    let _sentry = sentry::init(options);
    register_liboxen_hub_capture();

    let app = init_service(
        App::new()
            .route("/bare", web::get().to(bare_spawn))
            .route("/liboxen", web::get().to(liboxen_spawns))
            .route("/panic", web::get().to(liboxen_panic))
            .route("/detached", web::get().to(detached_spawn))
            .route("/streamed", web::get().to(streamed_spawn))
            .wrap(from_fn(read_body_under_request_hub))
            .wrap(
                sentry_actix::Sentry::builder()
                    .capture_server_errors(false)
                    .finish(),
            )
            .wrap(from_fn(run_request_as_task)),
    )
    .await;

    call_service(&app, TestRequest::get().uri("/bare").to_request()).await;
    let bare = transport.fetch_and_clear_events();
    assert_eq!(
        bare.len(),
        1,
        "the bare spawn's event reaches the process hub"
    );
    assert_eq!(
        bare[0].transaction, None,
        "a bare spawn reports under no route"
    );

    call_service(&app, TestRequest::get().uri("/liboxen").to_request()).await;
    let spawned = transport.fetch_and_clear_events();
    assert_eq!(spawned.len(), 2);
    for event in &spawned {
        assert_eq!(event.transaction.as_deref(), Some("GET /liboxen"));
        assert!(
            event.request.is_some(),
            "a request-bound task's event carries the request"
        );
    }

    call_service(&app, TestRequest::get().uri("/detached").to_request()).await;
    let detached = transport.fetch_and_clear_events();
    assert_eq!(
        detached.len(),
        1,
        "the detached task's event reaches the process hub's client"
    );
    assert_eq!(
        detached[0].transaction, None,
        "a detached task reports under no route"
    );
    assert!(
        detached[0].request.is_none(),
        "a detached task's event carries none of the request's scope"
    );

    let streamed = call_service(&app, TestRequest::get().uri("/streamed").to_request()).await;
    to_bytes(streamed.into_body())
        .await
        .expect("the streamed body should read to the end");
    let streamed = transport.fetch_and_clear_events();
    assert_eq!(streamed.len(), 1);
    assert_eq!(
        streamed[0].transaction.as_deref(),
        Some("GET /streamed"),
        "a task the body spawns as it is read reports under the route"
    );

    call_service(&app, TestRequest::get().uri("/panic").to_request()).await;
    let panic = transport
        .fetch_and_clear_events()
        .into_iter()
        .find(|event| event.level == Level::Fatal)
        .expect("the panic should be reported");
    assert_eq!(
        panic.transaction.as_deref(),
        Some("GET /panic"),
        "the panic's own event, which carries its stack, names the route"
    );
    assert!(
        panic
            .exception
            .values
            .iter()
            .any(|exception| exception.stacktrace.is_some())
    );
}
