use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;
use liboxen::util;
use rand::distributions::Alphanumeric;
use rand::{Rng, RngCore};
use std::fs;
use std::path::{Path, PathBuf};

fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn write_file_for_add_benchmark(
    file_path: &Path,
    large_file_chance: f64,
    content: &str,
) -> Result<(), OxenError> {
    if rand::thread_rng().gen_range(0.0..1.0) < large_file_chance {
        // 10% of files are large
        let large_content_size = 20 * 1024 * 1024 + 1; // > 20MB
        let mut large_content = vec![0u8; large_content_size];
        rand::thread_rng().fill_bytes(&mut large_content);
        fs::write(file_path, &large_content)?;
    } else {
        fs::write(file_path, content)?;
    }
    Ok(())
}

async fn setup_repo_for_add_benchmark(
    base_dir: &Path,
    repo_size: usize,
    num_files_to_add_in_benchmark: usize,
    dir_size: usize,
) -> Result<(LocalRepository, Vec<PathBuf>, PathBuf), OxenError> {
    let repo_dir = base_dir.join(format!("repo_{}", num_files_to_add_in_benchmark));
    if repo_dir.exists() {
        util::fs::remove_dir_all(&repo_dir)?;
    }

    let repo = repositories::init(&repo_dir)?;

    let files_dir = repo_dir.join("files");
    util::fs::create_dir_all(&files_dir)?;

    let mut rng = rand::thread_rng();

    // Create a number of directories up to 4 levels deep
    let mut dirs: Vec<PathBuf> = (0..dir_size)
        .map(|_| {
            let mut path = files_dir.clone();
            let depth = rng.gen_range(1..=4);
            for _ in 0..depth {
                path = path.join(generate_random_string(10));
            }
            path
        })
        .collect();
    dirs.push(files_dir.clone());

    // Calculate large_file_percentage based on repo_size
    let large_file_percentage: f64;
    let min_repo_size_for_scaling = 1000.0;
    let max_repo_size_for_scaling = 100000.0;
    let max_large_file_ratio = 0.5; // 50% for smallest repo
    let min_large_file_ratio = 0.01; // 1% for largest repo

    if (repo_size as f64) <= min_repo_size_for_scaling {
        large_file_percentage = max_large_file_ratio;
    } else if (repo_size as f64) >= max_repo_size_for_scaling {
        large_file_percentage = min_large_file_ratio;
    } else {
        let log_repo_size = (repo_size as f64).log10();
        let log_min_repo_size = min_repo_size_for_scaling.log10();
        let log_max_repo_size = max_repo_size_for_scaling.log10();

        let normalized_log_repo_size =
            (log_repo_size - log_min_repo_size) / (log_max_repo_size - log_min_repo_size);

        large_file_percentage = max_large_file_ratio
            - (max_large_file_ratio - min_large_file_ratio) * normalized_log_repo_size;
    }

    for i in 0..repo_size {
        let dir_idx = rng.gen_range(0..dirs.len());
        let dir = &dirs[dir_idx];
        util::fs::create_dir_all(dir)?;
        let file_path = dir.join(format!("file_{}.txt", i));
        write_file_for_add_benchmark(&file_path, large_file_percentage, "this is a test file")?;
    }
    repositories::add(&repo, black_box(&files_dir)).await?;
    repositories::commit(&repo, "Init")?;

    for i in repo_size..(repo_size + num_files_to_add_in_benchmark) {
        let dir_idx = rng.gen_range(0..dirs.len());
        let dir = &dirs[dir_idx];
        util::fs::create_dir_all(dir)?;
        let file_path = dir.join(format!("file_{}.txt", i));
        write_file_for_add_benchmark(
            &file_path,
            large_file_percentage,
            "this is a new test file to be added",
        )?;
    }
    Ok((repo, dirs, files_dir))
}

fn add_benchmark(c: &mut Criterion) {
    let base_dir = PathBuf::from("data/test/benches/add");
    if base_dir.exists() {
        util::fs::remove_dir_all(&base_dir).unwrap();
    }
    util::fs::create_dir_all(&base_dir).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("add");
    group.sample_size(10);
    let params = [
        (1000, 20),
        (10000, 20),
        (100000, 20),
        (100000, 100),
        (100000, 1000),
        (1000000, 1000),
    ];
    for &(repo_size, dir_size) in params.iter() {
        let num_files_to_add = repo_size / 1000;
        let (repo, _, file_dir) = rt
            .block_on(setup_repo_for_add_benchmark(
                &base_dir,
                repo_size,
                num_files_to_add,
                dir_size,
            ))
            .unwrap();

        group.bench_with_input(
            BenchmarkId::new(
                format!("{}k_files_in_{}dirs", num_files_to_add, dir_size),
                format!("{:?}", (num_files_to_add, dir_size)),
            ),
            &(num_files_to_add, dir_size),
            |b, _| {
                // Run in async executor
                b.to_async(&rt).iter(|| async {
                    repositories::add(&repo, black_box(&file_dir))
                        .await
                        .unwrap();

                    let _ = util::fs::remove_dir_all(repo.path.join(".oxen/staging"));
                })
            },
        );
    }
    group.finish();

    // Cleanup
    util::fs::remove_dir_all(base_dir).unwrap();
}

// Register Benchmark functions
criterion_group!(benches, add_benchmark);
criterion_main!(benches);
