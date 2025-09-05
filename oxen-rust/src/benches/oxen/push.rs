use criterion::{black_box, BenchmarkId, Criterion};
use liboxen::constants::DEFAULT_REMOTE_NAME;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;
use liboxen::test::create_or_clear_remote_repo;
use liboxen::util;
use liboxen::{api, command};
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

fn write_file_for_push_benchmark(
    file_path: &Path,
    large_file_chance: f64,
) -> Result<(), OxenError> {
    if rand::thread_rng().gen_range(0.05..1.0) < large_file_chance {
        let large_content_size = 1024 * 1024 + 1;
        let mut large_content = vec![0u8; large_content_size];
        rand::thread_rng().fill_bytes(&mut large_content);
        fs::write(file_path, &large_content)?;
    } else {
        let small_content_size = 1024 - 1;
        let mut small_content = vec![0u8; small_content_size];
        rand::thread_rng().fill_bytes(&mut small_content);
        fs::write(file_path, &small_content)?;
    }
    Ok(())
}

async fn setup_repo_for_push_benchmark(
    base_dir: &Path,
    repo_size: usize,
    num_files_to_push_in_benchmark: usize,
    dir_size: usize,
    data_path: Option<String>,
) -> Result<LocalRepository, OxenError> {
    println!(
        "setup_repo_for_push_benchmark got repo_size {}, num_files_to_push {}, and dir_size {}",
        repo_size, num_files_to_push_in_benchmark, dir_size,
    );
    let repo_dir = base_dir.join(format!(
        "repo_{}_{}",
        num_files_to_push_in_benchmark, dir_size
    ));
    if repo_dir.exists() {
        util::fs::remove_dir_all(&repo_dir)?;
    }

    let mut repo = repositories::init(&repo_dir)?;
    let remote_repo = create_or_clear_remote_repo(&repo).await?;
    command::config::set_remote(&mut repo, DEFAULT_REMOTE_NAME, &remote_repo.remote.url)?;

    let mut rng = rand::thread_rng();
    let files_dir = if let Some(data_path) = data_path {
        PathBuf::from(data_path)
    } else {
        let files_dir = repo_dir.join("files");
        util::fs::create_dir_all(&files_dir)?;
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

        let large_file_percentage: f64;
        let min_repo_size_for_scaling = 1000.0;
        let max_repo_size_for_scaling = 100000.0;
        let max_large_file_ratio = 0.5;
        let min_large_file_ratio = 0.01;

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

        // Note: If we want to have the push benchmark actually push to remotes with the proper repo_size,
        //       We would need to do this every iteration, which would be slow

        /*
        for i in 0..repo_size {
            let dir_idx = rng.gen_range(0..dirs.len());
            let dir = &dirs[dir_idx];
            util::fs::create_dir_all(dir)?;
            let file_path = dir.join(format!("file_{}.txt", i));
            write_file_for_push_benchmark(&file_path, large_file_percentage)?;
        }

        repositories::add(&repo, black_box(&files_dir)).await?;
        repositories::commit(&repo, "Init")?;
        repositories::push(&repo).await?;
        */

        for i in repo_size..(repo_size + num_files_to_push_in_benchmark) {
            let dir_idx = rng.gen_range(0..dirs.len());
            let dir = &dirs[dir_idx];
            util::fs::create_dir_all(dir)?;
            let file_path = dir.join(format!("file_{}.txt", i));
            write_file_for_push_benchmark(&file_path, large_file_percentage)?;
        }

        files_dir
    };

    repositories::add(&repo, black_box(&files_dir)).await?;
    repositories::commit(&repo, "Prepare test files for push benchmark")?;

    Ok(repo)
}

pub fn push_benchmark(c: &mut Criterion, data: Option<String>, iters: Option<usize>) {
    let base_dir = PathBuf::from("data/test/benches/push");
    if base_dir.exists() {
        util::fs::remove_dir_all(&base_dir).unwrap();
    }
    util::fs::create_dir_all(&base_dir).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("push");
    group.sample_size(iters.unwrap_or(10));
    let params = [
        (1000, 20),
        (10000, 20),
        (100000, 20),
        (100000, 100),
        (100000, 1000),
        (1000000, 1000),
    ];
    for &(repo_size, dir_size) in params.iter() {
        let num_files_to_push = repo_size / 1000;
        let mut repo = rt
            .block_on(setup_repo_for_push_benchmark(
                &base_dir,
                repo_size,
                num_files_to_push,
                dir_size,
                data.clone(),
            ))
            .unwrap();

        group.bench_with_input(
            BenchmarkId::new(
                format!("{}k_files_in_{}dirs", num_files_to_push, dir_size),
                format!("{:?}", (num_files_to_push, dir_size)),
            ),
            &(num_files_to_push, dir_size),
            |b, _| {
                // TODO: Refactor to do the push to remotes that already have 'repo_size' files in them
                //       Should be possible, but may be slow unless there's a good way to 'reverse' a push
                let remote_repo = rt.block_on(create_or_clear_remote_repo(&repo)).unwrap();
                let _ = command::config::set_remote(
                    &mut repo,
                    DEFAULT_REMOTE_NAME,
                    &remote_repo.remote.url,
                );

                b.to_async(&rt).iter(|| async {
                    repositories::push(&repo).await.unwrap();
                });

                let _ = rt
                    .block_on(api::client::repositories::delete(&remote_repo))
                    .unwrap();
            },
        );
    }
    group.finish();

    util::fs::remove_dir_all(base_dir).unwrap();
}
