use std::collections::HashSet;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex, OnceLock};

/// Thread-safe port allocator for integration tests
/// Ensures no port conflicts when tests run in parallel
#[derive(Debug)]
pub struct TestPortAllocator {
    allocated_ports: Arc<Mutex<HashSet<u16>>>,
    port_range_start: u16,
    port_range_end: u16,
}

/// RAII port lease - automatically returns port when dropped
#[derive(Debug)]
pub struct PortLease {
    port: u16,
    allocator: Arc<Mutex<HashSet<u16>>>,
}

impl PortLease {
    fn new(port: u16, allocator: Arc<Mutex<HashSet<u16>>>) -> Self {
        Self { port, allocator }
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for PortLease {
    fn drop(&mut self) {
        // Return the port to the available pool
        if let Ok(mut allocated) = self.allocator.lock() {
            allocated.remove(&self.port);
        }
    }
}

impl TestPortAllocator {
    /// Create a new port allocator with default range 3000-4000
    pub fn new() -> Self {
        Self {
            allocated_ports: Arc::new(Mutex::new(HashSet::new())),
            port_range_start: 3000,
            port_range_end: 4000,
        }
    }

    /// Create a new port allocator with custom range
    #[allow(dead_code)]
    pub fn with_range(start: u16, end: u16) -> Self {
        Self {
            allocated_ports: Arc::new(Mutex::new(HashSet::new())),
            port_range_start: start,
            port_range_end: end,
        }
    }

    /// Get the global singleton instance
    pub fn instance() -> &'static TestPortAllocator {
        static INSTANCE: OnceLock<TestPortAllocator> = OnceLock::new();
        INSTANCE.get_or_init(|| TestPortAllocator::new())
    }

    /// Lease an available port for the duration of the returned PortLease
    /// The port is automatically freed when the PortLease is dropped
    pub fn lease_port(&self) -> Result<PortLease, String> {
        let mut allocated = self
            .allocated_ports
            .lock()
            .map_err(|_| "Failed to acquire port allocator lock")?;

        // Try to find an available port by actually binding to it
        for port in self.port_range_start..=self.port_range_end {
            if allocated.contains(&port) {
                continue; // Already allocated by us
            }

            // Try to bind to this port to see if it's actually available
            match TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))) {
                Ok(_listener) => {
                    // Port is available! Allocate it and return immediately
                    // The listener is dropped here, freeing the port for the actual server
                    allocated.insert(port);
                    return Ok(PortLease::new(port, self.allocated_ports.clone()));
                }
                Err(_) => {
                    // Port is in use by another process, try next one
                    continue;
                }
            }
        }

        Err(format!(
            "No available ports in range {}-{}",
            self.port_range_start, self.port_range_end
        ))
    }

    /// Get currently allocated ports (for debugging)
    pub fn allocated_ports(&self) -> Vec<u16> {
        self.allocated_ports
            .lock()
            .map(|allocated| allocated.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Clear all allocated ports (for testing)
    pub fn clear_all(&self) {
        if let Ok(mut allocated) = self.allocated_ports.lock() {
            allocated.clear();
        }
    }

    /// Check if a specific port is currently allocated
    #[allow(dead_code)]
    pub fn is_allocated(&self, port: u16) -> bool {
        self.allocated_ports
            .lock()
            .map(|allocated| allocated.contains(&port))
            .unwrap_or(false)
    }
}

impl Default for TestPortAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_port_allocation_basic() {
        let allocator = TestPortAllocator::new();
        allocator.clear_all();

        let lease1 = allocator.lease_port().expect("Should get first port");
        let lease2 = allocator.lease_port().expect("Should get second port");

        assert_ne!(lease1.port(), lease2.port(), "Ports should be different");
        assert_eq!(allocator.allocated_ports().len(), 2);

        drop(lease1);
        assert_eq!(allocator.allocated_ports().len(), 1);

        drop(lease2);
        assert_eq!(allocator.allocated_ports().len(), 0);
    }

    #[test]
    fn test_port_allocation_thread_safety() {
        let allocator = Arc::new(TestPortAllocator::new());
        allocator.clear_all();

        let mut handles = Vec::new();

        // Spawn multiple threads trying to allocate ports simultaneously
        for i in 0..5 {
            let allocator_clone = allocator.clone();
            let handle = thread::spawn(move || {
                let lease = allocator_clone
                    .lease_port()
                    .expect(&format!("Thread {} should get a port", i));

                // Hold the port for a bit to simulate real usage
                thread::sleep(Duration::from_millis(10));

                lease.port()
            });
            handles.push(handle);
        }

        // Collect all allocated ports
        let mut ports = Vec::new();
        for handle in handles {
            let port = handle.join().expect("Thread should complete successfully");
            ports.push(port);
        }

        // All ports should be unique
        ports.sort();
        for i in 1..ports.len() {
            assert_ne!(ports[i - 1], ports[i], "All ports should be unique");
        }

        // After all threads complete, all ports should be freed
        // (Give a small delay for Drop to execute)
        thread::sleep(Duration::from_millis(50));
        assert_eq!(
            allocator.allocated_ports().len(),
            0,
            "All ports should be freed"
        );
    }

    #[test]
    fn test_singleton_instance() {
        let instance1 = TestPortAllocator::instance();
        let instance2 = TestPortAllocator::instance();

        // Should be the same instance
        assert!(std::ptr::eq(instance1, instance2));
    }
}
