//! Task spawning that carries the caller's context onto the new task: its tracing span always, and
//! whatever a program registers through [`register_context_capture`], such as oxen-server's
//! per-request Sentry hub.
//!
//! Spawn through this module rather than `tokio::task::spawn_blocking` or `tokio::spawn`, which the
//! workspace's `clippy.toml` disallows. The helpers open no span of their own, and they run only
//! work the caller waits for: a task that keeps the caller's span holds that span open until the
//! task ends, so a request's span would outlive its response.

use std::any::Any;
use std::future::{Future, poll_fn};
use std::sync::OnceLock;

use tokio::task::JoinHandle;
use tracing::{Instrument, Span};

/// What entering a captured context returns. Dropping it leaves the context.
pub type ContextGuard = Box<dyn Any>;

/// A context captured on the spawning thread. Each call enters it on the current thread.
pub type EnterContext = Box<dyn Fn() -> ContextGuard + Send + Sync>;

static CONTEXT_CAPTURE: OnceLock<fn() -> EnterContext> = OnceLock::new();

/// Have every spawn through this module call `capture` on the spawning thread and run the work
/// inside what it returns. Call it before anything is spawned. Only the first registration takes
/// effect.
pub fn register_context_capture(capture: fn() -> EnterContext) {
    let _ = CONTEXT_CAPTURE.set(capture);
}

fn capture_context() -> Option<EnterContext> {
    CONTEXT_CAPTURE.get().map(|capture| capture())
}

/// `tokio::task::spawn_blocking`, with the closure run in the caller's span and captured context.
#[allow(
    clippy::disallowed_methods,
    reason = "the one blocking spawn the rest of liboxen goes through"
)]
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let span = Span::current();
    let context = capture_context();
    tokio::task::spawn_blocking(move || {
        let _context = context.as_ref().map(|enter| enter());
        span.in_scope(f)
    })
}

/// `tokio::spawn`, with the future polled in the caller's span and captured context.
#[allow(
    clippy::disallowed_methods,
    reason = "the one future spawn the rest of liboxen goes through"
)]
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(inherit_context(future))
}

/// Bind the caller's span and captured context to `future`, for a spawn this module has no helper
/// for, such as `JoinSet::spawn`. Call it on the spawning thread.
pub fn inherit_context<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let context = capture_context();
    let mut future = Box::pin(future.instrument(Span::current()));
    poll_fn(move |cx| {
        let _context = context.as_ref().map(|enter| enter());
        future.as_mut().poll(cx)
    })
}
