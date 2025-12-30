//! Performance instrumentation module
//!
//! This module provides zero-cost performance logging when compiled without the `perf-logging` feature.
//! When the feature is enabled, it provides detailed timing information for instrumented operations.
//!
//! # Usage
//!
//! ```rust
//! use liboxen::perf_guard;
//!
//! fn my_operation() {
//!     let _guard = perf_guard!("my_operation");
//!     // Your code here
//!     // Timing is logged when guard is dropped
//! }
//! ```
//!
//! # Compile-time flags
//!
//! Build with performance logging:
//! ```bash
//! cargo build --features perf-logging
//! ```
//!
//! Build without (default - zero cost):
//! ```bash
//! cargo build
//! ```

#[cfg(feature = "perf-logging")]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(feature = "perf-logging")]
use std::time::Instant;

/// Global flag to enable/disable performance logging at runtime
/// This can be controlled via the OXEN_PERF_LOGGING environment variable
#[cfg(feature = "perf-logging")]
static PERF_LOGGING_ENABLED: AtomicBool = AtomicBool::new(false);

/// Initialize performance logging based on environment variable
///
/// This should be called once at application startup.
/// If `OXEN_PERF_LOGGING` environment variable is set to "1", "true", or "yes",
/// performance logging will be enabled.
pub fn init_perf_logging() {
    #[cfg(feature = "perf-logging")]
    {
        let enabled = std::env::var("OXEN_PERF_LOGGING")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes"))
            .unwrap_or(false);

        PERF_LOGGING_ENABLED.store(enabled, Ordering::Relaxed);

        if enabled {
            log::info!("Performance logging enabled");
        }
    }

    #[cfg(not(feature = "perf-logging"))]
    {
        // No-op when feature is not enabled
    }
}

/// Check if performance logging is enabled
#[inline]
pub fn is_enabled() -> bool {
    #[cfg(feature = "perf-logging")]
    {
        PERF_LOGGING_ENABLED.load(Ordering::Relaxed)
    }

    #[cfg(not(feature = "perf-logging"))]
    {
        false
    }
}

/// Performance guard that measures execution time
///
/// When dropped, it logs the elapsed time if performance logging is enabled.
/// If the `perf-logging` feature is not enabled at compile time, this becomes
/// a zero-sized type with no runtime overhead.
pub struct PerfGuard {
    #[cfg(feature = "perf-logging")]
    name: &'static str,
    #[cfg(feature = "perf-logging")]
    start: Option<Instant>,
}

impl PerfGuard {
    /// Create a new performance guard
    ///
    /// If performance logging is disabled (either at compile time or runtime),
    /// this returns immediately with minimal overhead.
    #[cfg(feature = "perf-logging")]
    pub fn new(name: &'static str) -> Self {
        if is_enabled() {
            Self {
                name,
                start: Some(Instant::now()),
            }
        } else {
            Self {
                name,
                start: None,
            }
        }
    }

    #[cfg(not(feature = "perf-logging"))]
    pub fn new(_name: &'static str) -> Self {
        Self {}
    }

    /// Create a disabled guard (no-op)
    pub fn disabled() -> Self {
        #[cfg(feature = "perf-logging")]
        {
            Self {
                name: "",
                start: None,
            }
        }

        #[cfg(not(feature = "perf-logging"))]
        {
            Self {}
        }
    }
}

#[cfg(feature = "perf-logging")]
impl Drop for PerfGuard {
    fn drop(&mut self) {
        if let Some(start) = self.start {
            let elapsed = start.elapsed();
            log::info!("[PERF] {} took {:?}", self.name, elapsed);
        }
    }
}

#[cfg(not(feature = "perf-logging"))]
impl Drop for PerfGuard {
    #[inline]
    fn drop(&mut self) {
        // No-op - optimizer will eliminate this
    }
}

/// Convenience macro to create a performance guard with the current function name
///
/// # Example
///
/// ```rust
/// use liboxen::perf_guard;
///
/// fn my_function() {
///     let _perf = perf_guard!("my_function");
///     // function body
/// }
/// ```
#[macro_export]
macro_rules! perf_guard {
    ($name:expr) => {
        $crate::util::perf::PerfGuard::new($name)
    };
}

/// Convenience macro to create a performance guard with an automatically generated name
///
/// # Example
///
/// ```rust
/// use liboxen::perf_guard_auto;
///
/// fn my_function() {
///     let _perf = perf_guard_auto!();
///     // function body - will be logged as "my_function"
/// }
/// ```
#[macro_export]
macro_rules! perf_guard_auto {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f)
            .rsplit("::")
            .find(|&part| part != "f" && part != "{{closure}}")
            .unwrap_or("unknown");
        $crate::util::perf::PerfGuard::new(name)
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_perf_guard_compiles() {
        let _guard = PerfGuard::new("test_operation");
        // Guard will be dropped here
    }

    #[test]
    fn test_disabled_guard() {
        let _guard = PerfGuard::disabled();
        // Should compile and run without errors
    }

    #[test]
    fn test_macro_compiles() {
        let _guard = perf_guard!("test_macro");
        // Should compile and run without errors
    }
}
