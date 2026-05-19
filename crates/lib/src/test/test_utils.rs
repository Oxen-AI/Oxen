//! Utility functions for test helpers.
//!
//!
use tokio::{
    runtime::{Handle, Runtime, RuntimeFlavor},
    task::block_in_place,
};

/// Drive an async future to completion from a sync context.
///
/// Uses the current multi-threaded Tokio runtime, if available.
/// Otherwise, creates a short-lived runtime for the future.
pub fn run_async<F>(fut: F) -> std::io::Result<F::Output>
where
    F: Future + Send,
    F::Output: Send,
{
    // If there's a multi-threaded runtime available, we can tell the runtime to move tasks
    // off of this thread, then block the current thread on running the future.
    if let Ok(handle) = Handle::try_current()
        && handle.runtime_flavor() == RuntimeFlavor::MultiThread
    {
        return Ok(block_in_place(|| handle.block_on(fut)));
    };

    // Either two situations exist:
    //   1. the runtime exists, but it's single threaded, meaning `block_in_place` will panic!
    //   2. there's no runtime that currently exists
    //
    // We need to make a new short-lived runtime to run this future to completion.
    Runtime::new().map(|rt| rt.block_on(fut))
}
