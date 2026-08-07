//! Locks in the tracing context `oxen_server::tasks` carries onto work that leaves the request
//! thread: a blocking task is timed under the span it was spawned from, a span the work opens on
//! the pool thread lands under that, and a future built in a handler but polled after it returned
//! still belongs to the request. Without this, essentially every sizeable server operation — all
//! of which run on the blocking pool or behind a streaming body — is exported as an unrelated
//! root, leaving a request's trace a lone HTTP span.
//!
//! Lives as its own integration test binary because the assertions read what a *different thread*
//! resolves, which only a process-wide subscriber provides. A scoped one binds the thread that
//! sets it; worse, entering a span on a thread whose default subscriber is a different registry
//! panics inside `tracing-subscriber`. The unit-test harness in `liboxen::test` installs a global
//! subscriber of its own during `init_test_env`, so a test sharing a binary with it cannot install
//! this one.

use std::sync::{Arc, Mutex};

use oxen_server::tasks::{inherit_hub, spawn_blocking};
use tracing::Instrument;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;

/// A span's name paired with the name of the span it hangs under, or `None` if it is a root.
type SpanParent = (String, Option<String>);

/// Records where each new span hangs, so an assertion can read a parent chain without standing up
/// an exporter.
#[derive(Clone, Default)]
struct SpanParents(Arc<Mutex<Vec<SpanParent>>>);

impl SpanParents {
    /// Install as this process's only subscriber. Every test in this binary shares it, so span
    /// names have to be distinct enough to look up.
    fn install() -> Self {
        let recorder = Self::default();
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(recorder.clone()),
        )
        .expect("this binary installs the only subscriber");
        recorder
    }

    fn recorded(&self) -> Vec<SpanParent> {
        self.0
            .lock()
            .expect("the recorder should not be poisoned")
            .clone()
    }

    /// `None` if no span by that name was opened, `Some(None)` if it was opened as a root.
    fn parent_of(&self, name: &str) -> Option<Option<String>> {
        self.recorded()
            .into_iter()
            .find(|(span, _)| span == name)
            .map(|(_, parent)| parent)
    }
}

impl<S> tracing_subscriber::Layer<S> for SpanParents
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        _attrs: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let parent = span.parent().map(|parent| parent.name().to_string());
        if let Ok(mut recorded) = self.0.lock() {
            recorded.push((span.name().to_string(), parent));
        }
    }
}

/// One test, because a process gets one global subscriber and these assertions all read it.
#[actix_web::test]
async fn work_off_the_request_thread_stays_in_the_request_span() {
    let recorder = SpanParents::install();
    let request = tracing::info_span!("request under test");

    // How the converted `spawn_blocking` call sites spawn: the handler awaits the task.
    async {
        spawn_blocking(|| tracing::info_span!("unpacking under test").in_scope(|| ()))
            .await
            .expect("the blocking task should not have panicked");
    }
    .instrument(request.clone())
    .await;

    assert_eq!(
        recorder.parent_of("blocking task"),
        Some(Some("request under test".to_string())),
        "recorded: {:?}",
        recorder.recorded()
    );
    assert_eq!(
        recorder.parent_of("unpacking under test"),
        Some(Some("blocking task".to_string())),
        "a span opened on the pool thread should hang under the blocking task; recorded: {:?}",
        recorder.recorded()
    );

    // How `stream_with_heartbeat` and the streaming-body writers spawn: the future is built while
    // the request is in scope and polled after the handler has returned.
    let deferred = {
        let _entered = request.enter();
        inherit_hub(async { tracing::info_span!("deferred work under test").in_scope(|| ()) })
    };
    deferred.await;

    assert_eq!(
        recorder.parent_of("deferred work under test"),
        Some(Some("request under test".to_string())),
        "recorded: {:?}",
        recorder.recorded()
    );

    // The baseline the three assertions above are measured against: the same future without
    // `inherit_hub` resolves no span at all once the handler's scope is gone.
    let bare = {
        let _entered = request.enter();
        async { tracing::info_span!("bare deferred work under test").in_scope(|| ()) }
    };
    bare.await;

    assert_eq!(
        recorder.parent_of("bare deferred work under test"),
        Some(None),
        "recorded: {:?}",
        recorder.recorded()
    );
}
