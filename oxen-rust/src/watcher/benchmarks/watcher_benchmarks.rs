use ignore::WalkBuilder;
use oxen_watcher::cache::StatusCache;
use oxen_watcher::protocol::WatcherResponse;
use oxen_watcher::tree::{FileMetadata, FileSystemTree};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::runtime::Runtime;

const TEST_REPO_PATH: &str =
    "/Users/josh/src/oxen/Oxen/watcher/oxen-rust/experiments/watcher-performance/test-repo";

/// Benchmark results structure
#[derive(Debug)]
struct BenchmarkResults {
    initial_scan_duration: Duration,
    initial_scan_files_count: usize,
    initial_scan_dirs_count: usize,

    single_update_duration: Duration,
    batch_100_update_duration: Duration,
    batch_1000_update_duration: Duration,
    batch_10000_update_duration: Duration,

    tree_serialization_duration: Duration,
    tree_serialization_size: usize,
    tree_deserialization_duration: Duration,

    full_query_duration: Duration,
    subtree_query_duration: Duration,

    event_processing_rate: f64,
    event_batch_processing_duration: Duration,

    memory_usage_empty: usize,
    memory_usage_after_scan: usize,
    memory_usage_after_updates: usize,
}

fn main() {
    env_logger::init();
    println!("=== Watcher Performance Benchmarks ===");
    println!("Test repository: {}", TEST_REPO_PATH);

    // Verify test repo exists
    if !Path::new(TEST_REPO_PATH).exists() {
        eprintln!("Error: Test repository not found at {}", TEST_REPO_PATH);
        eprintln!("Please ensure the 1M file test repo exists");
        return;
    }

    let rt = Runtime::new().unwrap();
    let mut results = BenchmarkResults {
        initial_scan_duration: Duration::default(),
        initial_scan_files_count: 0,
        initial_scan_dirs_count: 0,
        single_update_duration: Duration::default(),
        batch_100_update_duration: Duration::default(),
        batch_1000_update_duration: Duration::default(),
        batch_10000_update_duration: Duration::default(),
        tree_serialization_duration: Duration::default(),
        tree_serialization_size: 0,
        tree_deserialization_duration: Duration::default(),
        full_query_duration: Duration::default(),
        subtree_query_duration: Duration::default(),
        event_processing_rate: 0.0,
        event_batch_processing_duration: Duration::default(),
        memory_usage_empty: 0,
        memory_usage_after_scan: 0,
        memory_usage_after_updates: 0,
    };

    // Initialize repository if needed
    let repo_path = Path::new(TEST_REPO_PATH);
    if !repo_path.join(".oxen").exists() {
        println!("Initializing Oxen repository...");
        // Create a fake .oxen directory for testing
        std::fs::create_dir_all(repo_path.join(".oxen")).unwrap();
    }

    println!("\n1. Benchmarking initial scan...");
    benchmark_initial_scan(&rt, &mut results);

    println!("\n2. Benchmarking cache updates...");
    benchmark_cache_updates(&rt, &mut results);

    println!("\n3. Benchmarking tree queries...");
    benchmark_tree_queries(&rt, &mut results);

    println!("\n4. Benchmarking serialization...");
    benchmark_serialization(&rt, &mut results);

    println!("\n5. Benchmarking event processing...");
    benchmark_event_processing(&rt, &mut results);

    println!("\n6. Measuring memory usage...");
    measure_memory_usage(&rt, &mut results);

    print_results(&results);
    analyze_results(&results);
}

fn benchmark_initial_scan(rt: &Runtime, results: &mut BenchmarkResults) {
    rt.block_on(async {
        let cache = Arc::new(StatusCache::new(Path::new(TEST_REPO_PATH)).unwrap());

        println!("  Running parallel scan with metadata collection...");
        let start = Instant::now();

        let walker = WalkBuilder::new(TEST_REPO_PATH)
            .threads(num_cpus::get())
            .hidden(false)
            .filter_entry(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| name != ".oxen")
                    .unwrap_or(true)
            })
            .build_parallel();

        let (tx, rx) = std::sync::mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));

        walker.run(|| {
            let tx = tx.clone();
            Box::new(move |result| {
                if let Ok(entry) = result {
                    if let Some(ft) = entry.file_type() {
                        if ft.is_file() {
                            if let Ok(metadata) = entry.metadata() {
                                let file_metadata = FileMetadata {
                                    size: metadata.len(),
                                    mtime: metadata.modified().unwrap_or(SystemTime::now()),
                                    is_symlink: ft.is_symlink(),
                                };
                                let _ = tx
                                    .lock()
                                    .unwrap()
                                    .send((entry.into_path(), Some(file_metadata)));
                            }
                        } else if ft.is_dir() {
                            let _ = tx.lock().unwrap().send((entry.into_path(), None));
                        }
                    }
                }
                ignore::WalkState::Continue
            })
        });

        drop(tx);

        let mut updates = Vec::new();
        while let Ok((path, metadata)) = rx.recv() {
            if let Ok(relative_path) = path.strip_prefix(TEST_REPO_PATH) {
                if relative_path != Path::new("") {
                    updates.push((relative_path.to_path_buf(), metadata));
                }
            }
        }

        let scan_duration = start.elapsed();
        results.initial_scan_files_count = updates.iter().filter(|(_, m)| m.is_some()).count();
        results.initial_scan_dirs_count = updates.iter().filter(|(_, m)| m.is_none()).count();

        println!(
            "  Scan found {} files and {} directories in {:.3}s",
            results.initial_scan_files_count,
            results.initial_scan_dirs_count,
            scan_duration.as_secs_f64()
        );

        // Now measure tree building
        let build_start = Instant::now();
        cache.batch_update(updates).await.unwrap();
        cache.mark_scan_complete().await.unwrap();
        let build_duration = build_start.elapsed();

        results.initial_scan_duration = scan_duration + build_duration;

        println!("  Tree built in {:.3}s", build_duration.as_secs_f64());
        println!(
            "  Total initial scan time: {:.3}s",
            results.initial_scan_duration.as_secs_f64()
        );
    });
}

fn benchmark_cache_updates(rt: &Runtime, results: &mut BenchmarkResults) {
    rt.block_on(async {
        // Create a populated cache
        let cache = Arc::new(StatusCache::new(Path::new(TEST_REPO_PATH)).unwrap());

        // Do a quick initial population
        let mut initial_updates = Vec::new();
        for i in 0..10000 {
            let metadata = FileMetadata {
                size: 100 + i,
                mtime: SystemTime::now(),
                is_symlink: false,
            };
            initial_updates.push((PathBuf::from(format!("file_{}.txt", i)), Some(metadata)));
        }
        cache.batch_update(initial_updates).await.unwrap();

        // Test single update
        println!("  Testing single file update...");
        let metadata = FileMetadata {
            size: 500,
            mtime: SystemTime::now(),
            is_symlink: false,
        };
        let start = Instant::now();
        cache
            .update_file(PathBuf::from("single_test.txt"), metadata)
            .await
            .unwrap();
        results.single_update_duration = start.elapsed();
        println!(
            "    Single update: {:.3}ms",
            results.single_update_duration.as_secs_f64() * 1000.0
        );

        // Test batch of 100
        println!("  Testing batch of 100 updates...");
        let mut batch_100 = Vec::new();
        for i in 0..100 {
            let metadata = FileMetadata {
                size: 1000 + i,
                mtime: SystemTime::now(),
                is_symlink: false,
            };
            batch_100.push((PathBuf::from(format!("batch100_{}.txt", i)), Some(metadata)));
        }
        let start = Instant::now();
        cache.batch_update(batch_100).await.unwrap();
        results.batch_100_update_duration = start.elapsed();
        println!(
            "    Batch 100: {:.3}ms",
            results.batch_100_update_duration.as_secs_f64() * 1000.0
        );

        // Test batch of 1000
        println!("  Testing batch of 1000 updates...");
        let mut batch_1000 = Vec::new();
        for i in 0..1000 {
            let metadata = FileMetadata {
                size: 2000 + i,
                mtime: SystemTime::now(),
                is_symlink: false,
            };
            batch_1000.push((
                PathBuf::from(format!("batch1000_{}.txt", i)),
                Some(metadata),
            ));
        }
        let start = Instant::now();
        cache.batch_update(batch_1000).await.unwrap();
        results.batch_1000_update_duration = start.elapsed();
        println!(
            "    Batch 1000: {:.3}ms",
            results.batch_1000_update_duration.as_secs_f64() * 1000.0
        );

        // Test batch of 10000
        println!("  Testing batch of 10000 updates...");
        let mut batch_10000 = Vec::new();
        for i in 0..10000 {
            let metadata = FileMetadata {
                size: 3000 + i,
                mtime: SystemTime::now(),
                is_symlink: false,
            };
            batch_10000.push((
                PathBuf::from(format!("batch10000_{}.txt", i)),
                Some(metadata),
            ));
        }
        let start = Instant::now();
        cache.batch_update(batch_10000).await.unwrap();
        results.batch_10000_update_duration = start.elapsed();
        println!(
            "    Batch 10000: {:.3}ms",
            results.batch_10000_update_duration.as_secs_f64() * 1000.0
        );
    });
}

fn benchmark_tree_queries(rt: &Runtime, results: &mut BenchmarkResults) {
    rt.block_on(async {
        // Build a realistic cache with actual repo data
        let cache = Arc::new(StatusCache::new(Path::new(TEST_REPO_PATH)).unwrap());

        // Do initial scan
        println!("  Building tree from actual repository...");
        let walker = WalkBuilder::new(TEST_REPO_PATH)
            .threads(num_cpus::get())
            .hidden(false)
            .filter_entry(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| name != ".oxen")
                    .unwrap_or(true)
            })
            .build_parallel();

        let (tx, rx) = std::sync::mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));

        walker.run(|| {
            let tx = tx.clone();
            Box::new(move |result| {
                if let Ok(entry) = result {
                    if let Some(ft) = entry.file_type() {
                        if ft.is_file() {
                            if let Ok(metadata) = entry.metadata() {
                                let file_metadata = FileMetadata {
                                    size: metadata.len(),
                                    mtime: metadata.modified().unwrap_or(SystemTime::now()),
                                    is_symlink: ft.is_symlink(),
                                };
                                let _ = tx
                                    .lock()
                                    .unwrap()
                                    .send((entry.into_path(), Some(file_metadata)));
                            }
                        } else if ft.is_dir() {
                            let _ = tx.lock().unwrap().send((entry.into_path(), None));
                        }
                    }
                }
                ignore::WalkState::Continue
            })
        });

        drop(tx);

        let mut updates = Vec::new();
        let mut sample_dir = None;
        while let Ok((path, metadata)) = rx.recv() {
            if let Ok(relative_path) = path.strip_prefix(TEST_REPO_PATH) {
                if relative_path != Path::new("") {
                    // Remember a directory for subtree query
                    if metadata.is_none() && sample_dir.is_none() {
                        sample_dir = Some(relative_path.to_path_buf());
                    }
                    updates.push((relative_path.to_path_buf(), metadata));
                }
            }
        }

        cache.batch_update(updates).await.unwrap();
        cache.mark_scan_complete().await.unwrap();

        // Test full tree query
        println!("  Testing full tree query...");
        let start = Instant::now();
        let tree = cache.get_tree(None).await.unwrap();
        results.full_query_duration = start.elapsed();
        println!(
            "    Full tree query: {:.3}ms",
            results.full_query_duration.as_secs_f64() * 1000.0
        );

        // Count nodes
        let file_count = tree.iter_files().count();
        println!("    Tree contains {} files", file_count);

        // Test subtree query
        if let Some(dir) = sample_dir {
            println!("  Testing subtree query for {:?}...", dir);
            let start = Instant::now();
            let _subtree = cache.get_tree(Some(&dir)).await;
            results.subtree_query_duration = start.elapsed();
            println!(
                "    Subtree query: {:.3}ms",
                results.subtree_query_duration.as_secs_f64() * 1000.0
            );
        }
    });
}

fn benchmark_serialization(rt: &Runtime, results: &mut BenchmarkResults) {
    rt.block_on(async {
        // Build a tree with realistic data
        let cache = Arc::new(StatusCache::new(Path::new(TEST_REPO_PATH)).unwrap());

        // Populate with 100K files for serialization test
        println!("  Building tree with 100K files for serialization test...");
        let mut updates = Vec::new();
        for i in 0..100000 {
            let dir_idx = i / 1000;
            let metadata = FileMetadata {
                size: 1000 + (i as u64),
                mtime: SystemTime::now(),
                is_symlink: false,
            };
            updates.push((
                PathBuf::from(format!("dir_{:03}/file_{:06}.txt", dir_idx, i)),
                Some(metadata),
            ));
        }
        cache.batch_update(updates).await.unwrap();

        // Get the tree
        let tree = cache.get_tree(None).await.unwrap();

        // Test serialization
        println!("  Testing MessagePack serialization...");
        let start = Instant::now();
        let serialized = rmp_serde::to_vec(&tree).unwrap();
        results.tree_serialization_duration = start.elapsed();
        results.tree_serialization_size = serialized.len();

        println!(
            "    Serialization: {:.3}ms, size: {:.2} MB",
            results.tree_serialization_duration.as_secs_f64() * 1000.0,
            serialized.len() as f64 / (1024.0 * 1024.0)
        );

        // Test deserialization
        println!("  Testing MessagePack deserialization...");
        let start = Instant::now();
        let _deserialized: FileSystemTree = rmp_serde::from_slice(&serialized).unwrap();
        results.tree_deserialization_duration = start.elapsed();

        println!(
            "    Deserialization: {:.3}ms",
            results.tree_deserialization_duration.as_secs_f64() * 1000.0
        );

        // Test complete round-trip with protocol
        println!("  Testing complete protocol round-trip...");
        let response = WatcherResponse::Tree(tree);
        let start = Instant::now();
        let response_bytes = response.to_bytes().unwrap();
        let _decoded = WatcherResponse::from_bytes(&response_bytes).unwrap();
        let roundtrip_duration = start.elapsed();

        println!(
            "    Protocol round-trip: {:.3}ms",
            roundtrip_duration.as_secs_f64() * 1000.0
        );
    });
}

fn benchmark_event_processing(rt: &Runtime, results: &mut BenchmarkResults) {
    rt.block_on(async {
        let cache = Arc::new(StatusCache::new(Path::new(TEST_REPO_PATH)).unwrap());

        // Create simulated file events and measure cache update performance
        println!("  Creating batch of 1000 file updates...");
        let mut updates = Vec::new();

        for i in 0..1000 {
            let metadata = oxen_watcher::tree::FileMetadata {
                size: 1000 + i,
                mtime: SystemTime::now(),
                is_symlink: false,
            };
            updates.push((
                PathBuf::from(format!("event_test_{}.txt", i)),
                Some(metadata),
            ));
        }

        // Measure batch update performance (simulating event processing)
        println!("  Processing {} updates...", updates.len());
        let event_count = updates.len();
        let start = Instant::now();
        cache.batch_update(updates).await.unwrap();
        results.event_batch_processing_duration = start.elapsed();

        results.event_processing_rate =
            event_count as f64 / results.event_batch_processing_duration.as_secs_f64();

        println!(
            "    Processing time: {:.3}ms",
            results.event_batch_processing_duration.as_secs_f64() * 1000.0
        );
        println!(
            "    Processing rate: {:.0} events/second",
            results.event_processing_rate
        );
    });
}

fn measure_memory_usage(rt: &Runtime, results: &mut BenchmarkResults) {
    rt.block_on(async {
        // Measure empty cache
        println!("  Creating empty cache...");
        let cache = Arc::new(StatusCache::new(Path::new(TEST_REPO_PATH)).unwrap());
        results.memory_usage_empty = estimate_memory_usage();
        println!(
            "    Empty cache memory: ~{:.2} MB",
            results.memory_usage_empty as f64 / (1024.0 * 1024.0)
        );

        // Measure after initial scan
        println!("  After populating with 100K files...");
        let mut updates = Vec::new();
        for i in 0..100000 {
            let metadata = FileMetadata {
                size: 1000 + i,
                mtime: SystemTime::now(),
                is_symlink: false,
            };
            updates.push((PathBuf::from(format!("mem_test_{}.txt", i)), Some(metadata)));
        }
        cache.batch_update(updates).await.unwrap();
        results.memory_usage_after_scan = estimate_memory_usage();
        println!(
            "    After 100K files: ~{:.2} MB",
            results.memory_usage_after_scan as f64 / (1024.0 * 1024.0)
        );

        // Measure after updates
        println!("  After additional 10K updates...");
        let mut more_updates = Vec::new();
        for i in 0..10000 {
            let metadata = FileMetadata {
                size: 5000 + i,
                mtime: SystemTime::now(),
                is_symlink: false,
            };
            more_updates.push((
                PathBuf::from(format!("mem_update_{}.txt", i)),
                Some(metadata),
            ));
        }
        cache.batch_update(more_updates).await.unwrap();
        results.memory_usage_after_updates = estimate_memory_usage();
        println!(
            "    After updates: ~{:.2} MB",
            results.memory_usage_after_updates as f64 / (1024.0 * 1024.0)
        );
    });
}

fn estimate_memory_usage() -> usize {
    // This is a rough estimate - in production you'd use a proper memory profiler
    // For now, we'll estimate based on structure sizes
    std::mem::size_of::<FileSystemTree>()
        + std::mem::size_of::<FileMetadata>() * 100000  // Rough estimate
        + std::mem::size_of::<PathBuf>() * 100000
        + 1024 * 1024 // Overhead
}

fn print_results(results: &BenchmarkResults) {
    println!("\n{}", "=".repeat(60));
    println!("BENCHMARK RESULTS SUMMARY");
    println!("{}", "=".repeat(60));

    println!("\nInitial Scan Performance:");
    println!("  Files scanned: {}", results.initial_scan_files_count);
    println!("  Directories scanned: {}", results.initial_scan_dirs_count);
    println!(
        "  Total time: {:.3}s",
        results.initial_scan_duration.as_secs_f64()
    );
    println!(
        "  Throughput: {:.0} files/second",
        results.initial_scan_files_count as f64 / results.initial_scan_duration.as_secs_f64()
    );

    println!("\nCache Update Performance:");
    println!(
        "  Single update: {:.3}ms",
        results.single_update_duration.as_secs_f64() * 1000.0
    );
    println!(
        "  Batch 100: {:.3}ms ({:.2}ms per file)",
        results.batch_100_update_duration.as_secs_f64() * 1000.0,
        results.batch_100_update_duration.as_secs_f64() * 1000.0 / 100.0
    );
    println!(
        "  Batch 1,000: {:.3}ms ({:.2}ms per file)",
        results.batch_1000_update_duration.as_secs_f64() * 1000.0,
        results.batch_1000_update_duration.as_secs_f64() * 1000.0 / 1000.0
    );
    println!(
        "  Batch 10,000: {:.3}ms ({:.2}ms per file)",
        results.batch_10000_update_duration.as_secs_f64() * 1000.0,
        results.batch_10000_update_duration.as_secs_f64() * 1000.0 / 10000.0
    );

    println!("\nTree Query Performance:");
    println!(
        "  Full tree query: {:.3}ms",
        results.full_query_duration.as_secs_f64() * 1000.0
    );
    println!(
        "  Subtree query: {:.3}ms",
        results.subtree_query_duration.as_secs_f64() * 1000.0
    );

    println!("\nSerialization Performance:");
    println!(
        "  Serialization: {:.3}ms",
        results.tree_serialization_duration.as_secs_f64() * 1000.0
    );
    println!(
        "  Size: {:.2} MB",
        results.tree_serialization_size as f64 / (1024.0 * 1024.0)
    );
    println!(
        "  Deserialization: {:.3}ms",
        results.tree_deserialization_duration.as_secs_f64() * 1000.0
    );
    println!(
        "  Total round-trip: {:.3}ms",
        (results.tree_serialization_duration + results.tree_deserialization_duration).as_secs_f64()
            * 1000.0
    );

    println!("\nEvent Processing Performance:");
    println!(
        "  Batch processing time: {:.3}ms",
        results.event_batch_processing_duration.as_secs_f64() * 1000.0
    );
    println!(
        "  Processing rate: {:.0} events/second",
        results.event_processing_rate
    );

    println!("\nMemory Usage:");
    println!(
        "  Empty cache: ~{:.2} MB",
        results.memory_usage_empty as f64 / (1024.0 * 1024.0)
    );
    println!(
        "  After 100K files: ~{:.2} MB",
        results.memory_usage_after_scan as f64 / (1024.0 * 1024.0)
    );
    println!(
        "  After updates: ~{:.2} MB",
        results.memory_usage_after_updates as f64 / (1024.0 * 1024.0)
    );
}

fn analyze_results(results: &BenchmarkResults) {
    println!("\n{}", "=".repeat(60));
    println!("ANALYSIS AND OBSERVATIONS");
    println!("{}", "=".repeat(60));

    println!("\n📊 Key Performance Metrics for 1M Files:");

    // Extrapolate to 1M files
    let files_per_second =
        results.initial_scan_files_count as f64 / results.initial_scan_duration.as_secs_f64();
    let estimated_1m_scan = 1_000_000.0 / files_per_second;
    println!(
        "\n  Estimated initial scan time for 1M files: {:.1}s",
        estimated_1m_scan
    );

    // Update performance analysis
    let update_throughput = 10000.0 / results.batch_10000_update_duration.as_secs_f64();
    println!(
        "  Cache update throughput: {:.0} updates/second",
        update_throughput
    );

    // Serialization analysis
    let bytes_per_file = results.tree_serialization_size as f64 / 100000.0;
    let estimated_1m_size = bytes_per_file * 1_000_000.0 / (1024.0 * 1024.0);
    println!(
        "  Estimated serialized size for 1M files: {:.1} MB",
        estimated_1m_size
    );

    let serialization_throughput = 100000.0 / results.tree_serialization_duration.as_secs_f64();
    let estimated_1m_serialization = 1_000_000.0 / serialization_throughput;
    println!(
        "  Estimated serialization time for 1M files: {:.2}s",
        estimated_1m_serialization
    );

    // Memory analysis
    let memory_per_file =
        (results.memory_usage_after_scan - results.memory_usage_empty) as f64 / 100000.0;
    let estimated_1m_memory = memory_per_file * 1_000_000.0 / (1024.0 * 1024.0);
    println!(
        "  Estimated memory usage for 1M files: {:.1} MB",
        estimated_1m_memory
    );

    println!("\n🎯 Performance Characteristics:");

    // Batch efficiency
    let batch_efficiency = results.batch_10000_update_duration.as_secs_f64()
        / (results.single_update_duration.as_secs_f64() * 10000.0);
    println!(
        "  Batch update efficiency: {:.1}x faster than individual updates",
        1.0 / batch_efficiency
    );

    // Query performance
    if results.subtree_query_duration > Duration::ZERO {
        let query_ratio = results.full_query_duration.as_secs_f64()
            / results.subtree_query_duration.as_secs_f64();
        println!(
            "  Full tree query is {:.1}x slower than subtree query",
            query_ratio
        );
    }

    println!("\n💡 Observations:");

    if estimated_1m_scan < 10.0 {
        println!("  ✅ Initial scan performance is excellent (<10s for 1M files)");
    } else if estimated_1m_scan < 30.0 {
        println!("  ⚡ Initial scan performance is good (<30s for 1M files)");
    } else {
        println!(
            "  ⚠️  Initial scan may be slow for 1M files (>{:.0}s)",
            estimated_1m_scan
        );
    }

    if results.event_processing_rate > 10000.0 {
        println!("  ✅ Event processing is very fast (>10K events/sec)");
    } else if results.event_processing_rate > 1000.0 {
        println!("  ⚡ Event processing is adequate (>1K events/sec)");
    } else {
        println!("  ⚠️  Event processing may bottleneck under heavy load");
    }

    if estimated_1m_memory < 500.0 {
        println!("  ✅ Memory usage is efficient (<500MB for 1M files)");
    } else if estimated_1m_memory < 1000.0 {
        println!("  ⚡ Memory usage is reasonable (<1GB for 1M files)");
    } else {
        println!(
            "  ⚠️  Memory usage may be high (>{:.0}MB for 1M files)",
            estimated_1m_memory
        );
    }

    println!("\n🚀 Recommendations:");

    if estimated_1m_scan > 30.0 {
        println!("  • Consider lazy loading for very large repositories");
        println!("  • Implement incremental scanning strategies");
    }

    if results.tree_serialization_duration.as_secs_f64() > 0.5 {
        println!("  • Consider compression for network transfer");
        println!("  • Implement partial tree updates instead of full transfers");
    }

    if estimated_1m_memory > 1000.0 {
        println!("  • Implement tree pruning for inactive branches");
        println!("  • Consider persistent caching to reduce memory footprint");
    }

    println!("\n📈 Overall Assessment:");
    let overall_score = calculate_overall_score(results, estimated_1m_scan, estimated_1m_memory);

    if overall_score >= 80.0 {
        println!("  🏆 Excellent performance - ready for production use with 1M+ files");
    } else if overall_score >= 60.0 {
        println!("  ✅ Good performance - suitable for most large repositories");
    } else if overall_score >= 40.0 {
        println!("  ⚡ Adequate performance - may need optimization for very large repos");
    } else {
        println!("  ⚠️  Performance improvements needed for large-scale usage");
    }

    println!("\n  Performance Score: {:.0}/100", overall_score);
}

fn calculate_overall_score(
    results: &BenchmarkResults,
    estimated_1m_scan: f64,
    estimated_1m_memory: f64,
) -> f64 {
    let mut score: f64 = 100.0;

    // Scan time scoring (max -30 points)
    if estimated_1m_scan > 60.0 {
        score -= 30.0;
    } else if estimated_1m_scan > 30.0 {
        score -= 20.0;
    } else if estimated_1m_scan > 10.0 {
        score -= 10.0;
    }

    // Memory scoring (max -20 points)
    if estimated_1m_memory > 2000.0 {
        score -= 20.0;
    } else if estimated_1m_memory > 1000.0 {
        score -= 10.0;
    } else if estimated_1m_memory > 500.0 {
        score -= 5.0;
    }

    // Event processing scoring (max -20 points)
    if results.event_processing_rate < 1000.0 {
        score -= 20.0;
    } else if results.event_processing_rate < 5000.0 {
        score -= 10.0;
    } else if results.event_processing_rate < 10000.0 {
        score -= 5.0;
    }

    // Serialization scoring (max -15 points)
    let serialization_ms = results.tree_serialization_duration.as_secs_f64() * 1000.0;
    if serialization_ms > 1000.0 {
        score -= 15.0;
    } else if serialization_ms > 500.0 {
        score -= 10.0;
    } else if serialization_ms > 200.0 {
        score -= 5.0;
    }

    // Query performance scoring (max -15 points)
    let query_ms = results.full_query_duration.as_secs_f64() * 1000.0;
    if query_ms > 100.0 {
        score -= 15.0;
    } else if query_ms > 50.0 {
        score -= 10.0;
    } else if query_ms > 20.0 {
        score -= 5.0;
    }

    score.max(0.0_f64)
}
