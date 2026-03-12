use crate::constants;
use crate::error::OxenError;
use rand::Rng;
use std::future::Future;

pub struct RetryConfig {
    pub max_retries: usize,
    pub base_wait_ms: u64,
    pub max_wait_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: constants::max_retries(),
            base_wait_ms: 300,
            max_wait_ms: 10_000,
        }
    }
}

/// True exponential backoff: base_wait_ms * 2^attempt + jitter, clamped to max_wait_ms
pub fn exponential_backoff(base_wait_ms: u64, attempt: usize, max_wait_ms: u64) -> u64 {
    let exp = base_wait_ms.saturating_mul(1u64 << attempt.min(16));
    let jitter = rand::thread_rng().gen_range(0..=500u64);
    exp.saturating_add(jitter).min(max_wait_ms)
}

/// Retry an async operation with exponential backoff.
/// Authentication errors are not retried.
pub async fn with_retry<F, Fut, T>(config: &RetryConfig, mut operation: F) -> Result<T, OxenError>
where
    F: FnMut(usize) -> Fut,
    Fut: Future<Output = Result<T, OxenError>>,
{
    let mut last_err = None;
    for attempt in 0..=config.max_retries {
        match operation(attempt).await {
            Ok(val) => return Ok(val),
            error @ Err(OxenError::Authentication(_)) => return error,
            Err(err) => {
                log::warn!(
                    "Retry attempt {}/{} failed: {err}",
                    attempt + 1,
                    config.max_retries + 1
                );
                last_err = Some(err);
                if attempt < config.max_retries {
                    let wait =
                        exponential_backoff(config.base_wait_ms, attempt, config.max_wait_ms);
                    tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| OxenError::basic_str("Retry failed with no attempts")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff_increases() {
        let b0 = exponential_backoff(300, 0, 100_000);
        let b1 = exponential_backoff(300, 1, 100_000);
        let b2 = exponential_backoff(300, 2, 100_000);
        // With jitter, we can't be exact, but the base doubles each time
        // 300*1=300, 300*2=600, 300*4=1200 (plus jitter 0..500)
        assert!(b0 <= 800, "b0={b0}");
        assert!(b1 >= 300 && b1 <= 1100, "b1={b1}");
        assert!(b2 >= 900 && b2 <= 1700, "b2={b2}");
    }

    #[test]
    fn test_exponential_backoff_clamps_to_max() {
        let val = exponential_backoff(300, 20, 5000);
        assert!(val <= 5000, "val={val}");
    }

    #[tokio::test]
    async fn test_with_retry_immediate_success() {
        let config = RetryConfig {
            max_retries: 3,
            base_wait_ms: 10,
            max_wait_ms: 100,
        };
        let result: Result<i32, OxenError> = with_retry(&config, |_attempt| async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_with_retry_exhaustion() {
        let config = RetryConfig {
            max_retries: 2,
            base_wait_ms: 10,
            max_wait_ms: 50,
        };
        let result: Result<i32, OxenError> = with_retry(&config, |_attempt| async {
            Err(OxenError::basic_str("always fails"))
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_with_retry_succeeds_on_third_attempt() {
        let config = RetryConfig {
            max_retries: 3,
            base_wait_ms: 10,
            max_wait_ms: 50,
        };
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let result: Result<i32, OxenError> = with_retry(&config, move |_attempt| {
            let c = counter_clone.clone();
            async move {
                let prev = c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if prev < 2 {
                    Err(OxenError::basic_str("not yet"))
                } else {
                    Ok(99)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 99);
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 3);
    }
}
