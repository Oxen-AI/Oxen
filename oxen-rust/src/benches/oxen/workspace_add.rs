use criterion::{black_box, BenchmarkId, Criterion};
use liboxen::api;
use liboxen::command;
use liboxen::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME};
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, RemoteRepository};
use liboxen::repositories;
use liboxen::test::create_or_clear_remote_repo;
use liboxen::util;
use rand::distributions::Alphanumeric;
use rand::{Rng, RngCore};
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn write_file_for_workspace_add_benchmark(
    file_path: &Path,
    large_file_chance: f64,
) -> Result<(), OxenError> {
    if rand::thread_rng().gen_range(0.0..1.0) < large_file_chance {
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

async fn setup_repo_for_workspace_add_benchmark(
    base_dir: &Path,
    repo_size: usize,
    num_files_to_add_in_benchmark: usize,
    dir_size: usize,
    data_path: Option<String>,
) -> Result<(LocalRepository, RemoteRepository, Vec<PathBuf>), OxenError> {
    println!("setup_repo_for_workspace_add_benchmark got repo_size {}, num_files_to_add {}, and dir_size {}",
        repo_size,
        num_files_to_add_in_benchmark,
        dir_size,
    );
    // Generate Uuid to ensure the data is pushed to a new remote
    let remote_id = Uuid::new_v4().to_string();
    let repo_dir = base_dir.join(format!(
        "repo_{}_{}_{}",
        num_files_to_add_in_benchmark, dir_size, remote_id
    ));

    if repo_dir.exists() {
        util::fs::remove_dir_all(&repo_dir)?;
    }

    let mut repo = repositories::init(&repo_dir)?;

    let remote_repo = create_or_clear_remote_repo(&repo).await?;
    command::config::set_remote(&mut repo, DEFAULT_REMOTE_NAME, &remote_repo.remote.url)?;

    // Create the workspace
    let mut rng = rand::thread_rng();

    // TODO: Support creating workspace on empty repo
    let base_dir = repo_dir.join("base");
    util::fs::create_dir_all(&base_dir)?;
    let mut base_dirs: Vec<PathBuf> = (0..dir_size)
        .map(|_| {
            let mut path = base_dir.clone();
            let depth = rng.gen_range(1..=4);
            for _ in 0..depth {
                path = path.join(generate_random_string(10));
            }
            path
        })
        .collect();
    base_dirs.push(base_dir.clone());

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

    for i in 0..repo_size {
        let dir_idx = rng.gen_range(0..base_dirs.len());
        let dir = &base_dirs[dir_idx];
        util::fs::create_dir_all(dir)?;
        let file_path = dir.join(format!("file_{}.txt", i));
        write_file_for_workspace_add_benchmark(&file_path, large_file_percentage)?;
    }

    repositories::add(&repo, black_box(&base_dir)).await?;
    repositories::commit(&repo, "Init")?;
    repositories::push(&repo).await?;

    let files_dir = repo_dir.join("files");
    util::fs::create_dir_all(&files_dir)?;
    let mut files_dirs: Vec<PathBuf> = (0..dir_size)
        .map(|_| {
            let mut path = base_dir.clone();
            let depth = rng.gen_range(1..=4);
            for _ in 0..depth {
                path = path.join(generate_random_string(10));
            }
            path
        })
        .collect();
    files_dirs.push(files_dir.clone());

    let files: Vec<PathBuf> = if let Some(data_path) = data_path {
        vec![PathBuf::from(data_path)]
    } else {
        let mut files = vec![];
        for i in repo_size..(repo_size + num_files_to_add_in_benchmark) {
            let dir_idx = rng.gen_range(0..files_dirs.len());
            let dir = &files_dirs[dir_idx];
            util::fs::create_dir_all(dir)?;

            let file_path = dir.join(format!("file_{}.txt", i));
            write_file_for_workspace_add_benchmark(&file_path, large_file_percentage)?;

            files.push(file_path);
        }

        files
    };

    Ok((repo, remote_repo, files))
}

pub fn workspace_add_benchmark(c: &mut Criterion, data: Option<String>, iters: Option<usize>) {
    let base_dir = PathBuf::from("data/test/benches/workspace_add");
    if base_dir.exists() {
        util::fs::remove_dir_all(&base_dir).unwrap();
    }
    util::fs::create_dir_all(&base_dir).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("workspace_add");
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
        let num_files_to_add = repo_size / 1000;
        let (_, remote_repo, files) = rt
            .block_on(setup_repo_for_workspace_add_benchmark(
                &base_dir,
                repo_size,
                num_files_to_add,
                dir_size,
                data.clone(),
            ))
            .unwrap();

        group.bench_with_input(
            BenchmarkId::new(
                format!("{}k_files_in_{}dirs", num_files_to_add, dir_size),
                format!("{:?}", (num_files_to_add, dir_size)),
            ),
            &(num_files_to_add, dir_size),
            |b, _| {
                let branch_name = DEFAULT_BRANCH_NAME;

                // Generate a random workspace id
                let workspace_id = Uuid::new_v4().to_string();

                // Use the branch name as the workspace name
                let name = format!("{}: {workspace_id}", branch_name);

                let workspace = rt
                    .block_on(api::client::workspaces::create_with_new_branch(
                        &remote_repo,
                        &branch_name,
                        &workspace_id,
                        Path::new("/"),
                        Some(name.clone()),
                    ))
                    .unwrap();

                b.to_async(&rt).iter(|| async {
                    api::client::workspaces::files::add(
                        &remote_repo,
                        &workspace.id,
                        "",
                        files.clone(),
                        &None,
                    )
                    .await
                    .unwrap();
                });

                let _ = rt
                    .block_on(api::client::workspaces::delete(&remote_repo, &workspace.id))
                    .unwrap();
            },
        );
    }
    group.finish();

    util::fs::remove_dir_all(base_dir).unwrap();
}
