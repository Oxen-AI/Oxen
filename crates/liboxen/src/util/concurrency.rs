use crate::constants;

/// Returns the number of threads to use for a given number of items
/// Can be overridden by setting the environment variable OXEN_NUM_THREADS
/// Defaults to constants::DEFAULT_NUM_WORKERS or the number of CPUs we have if we have less than that
pub fn num_threads_for_items(num_items: usize) -> usize {
    let num_workers = parse_num_threads_env();

    // Cap the worker count at the item count, but never return 0: callers feed this straight into
    // `buffer_unordered` / `for_each_concurrent`, where a concurrency limit of 0 stalls the stream
    // forever instead of completing on an empty input.
    num_workers.min(num_items).max(1)
}

/// Returns the default number of threads to use
/// Can be overridden by setting the environment variable OXEN_NUM_THREADS
/// Defaults to constants::DEFAULT_NUM_WORKERS or the number of CPUs we have if we have less than that
pub fn default_num_threads() -> usize {
    parse_num_threads_env()
}

fn parse_num_threads_env() -> usize {
    if let Ok(num_threads) = std::env::var("OXEN_NUM_THREADS") {
        if let Ok(num_threads) = num_threads.parse::<usize>() {
            num_threads
        } else {
            get_default_num_workers()
        }
    } else {
        get_default_num_workers()
    }
}

fn get_default_num_workers() -> usize {
    // Check how many CPUs we have
    let num_cpus = num_cpus::get();

    // Default to constants::DEFAULT_NUM_WORKERS, but if we have less cpus than that, use that instead
    if constants::DEFAULT_NUM_WORKERS > num_cpus {
        num_cpus
    } else {
        constants::DEFAULT_NUM_WORKERS
    }
}

#[cfg(test)]
mod tests {
    use super::num_threads_for_items;

    #[test]
    fn test_num_threads_for_items_never_zero() {
        // A worker count of 0 fed into `buffer_unordered` / `for_each_concurrent` stalls the
        // stream forever, so an empty work set must still yield at least one worker.
        assert_eq!(num_threads_for_items(0), 1);
    }
}
