use crate::constants;

/// Returns the number of threads to use for a given number of items
/// Can be overridden by setting the environment variable OXEN_NUM_THREADS
/// Defaults to constants::DEFAULT_NUM_WORKERS or the number of CPUs we have if we have less than that
pub fn num_threads_for_items(num_items: usize) -> usize {
    // If the environment variable is set, use that
    if let Ok(num_threads) = std::env::var("OXEN_NUM_THREADS") {
        if let Ok(num_threads) = num_threads.parse::<usize>() {
            return num_threads;
        }
    }

    // Then check how many CPUs we have
    let num_cpus = num_cpus::get();

    // Default to constants::DEFAULT_NUM_WORKERS, but if we have less cpus than that, use that instead
    let num_workers = if constants::DEFAULT_NUM_WORKERS > num_cpus {
        num_cpus
    } else {
        constants::DEFAULT_NUM_WORKERS
    };

    // Finally look at how many items we have, and if we have less items than workers, use that instead
    if num_workers > num_items {
        num_items
    } else {
        num_workers
    }
}
