#[cfg(feature = "metrics")]
use metrics_exporter_prometheus::{BuildError, PrometheusBuilder};

/// Guard for the Prometheus metrics exporter. Dropping this aborts the
/// background HTTP server that serves the `/metrics` endpoint.
///
/// Use `init_metrics_prometheus` to create an instance.
#[cfg(feature = "metrics")]
pub(crate) struct MetricsGuard {
    handle: tokio::task::JoinHandle<Result<(), String>>,
}

#[cfg(feature = "metrics")]
impl Drop for MetricsGuard {
    /// Aborts the background HTTP server that serves the `/metrics` endpoint.
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// An empty struct used as a placeholder when the `metrics` feature is disabled.
#[cfg(not(feature = "metrics"))]
pub(crate) struct MetricsGuard {}

/// Install the Prometheus metrics exporter, listening on the given port.
///
/// Metrics are available at `http://0.0.0.0:{port}/metrics`.
///
/// Returns a [`MetricsGuard`] that keeps the exporter alive. The HTTP server
/// is shut down when the guard is dropped. The HTTP server runs in its own Tokio
/// background task.
///
/// # Errors
///
/// Returns an error if the port is already in use or if the Prometheus recorder
/// or exporter cannot be initialized.
#[cfg(feature = "metrics")]
pub(crate) fn init_metrics_prometheus(port: u16) -> Result<MetricsGuard, BuildError> {
    // Verify the port is free before committing to the recorder and spawning
    // the background task. The listener is dropped immediately so the exporter
    // can bind to the same port.
    std::net::TcpListener::bind(("0.0.0.0", port)).map_err(|_| {
        BuildError::FailedToCreateHTTPListener(format!(
            "Metrics port {port} is already in use. Change the port with OXEN_METRICS_PORT="
        ))
    })?;

    let (prom_recorder, prom_exporter_fut) = PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], port))
        .build()?;

    metrics::set_global_recorder(prom_recorder)?;

    let handle = tokio::spawn(async move {
        prom_exporter_fut.await.map_err(|e| {
            let msg = format!("{e:?}");
            tracing::error!("Prometheus metrics exporter failed: {msg}");
            msg
        })
    });

    Ok(MetricsGuard { handle })
}
