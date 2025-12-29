use criterion::{black_box, BenchmarkId, Criterion};
use liboxen::api;
use liboxen::command;
use liboxen::core;
use liboxen::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME};
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, RemoteRepository, Workspace, };
use liboxen::repositories;
use liboxen::test::create_or_clear_remote_repo;
use liboxen::util;
use rand::distributions::Alphanumeric;
use rand::{Rng, RngCore};
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use tokio::runtime::Runtime;
use liboxen::view::FileWithHash;
use liboxen::constants;


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
) -> Result<(LocalRepository, Workspace, String), OxenError> {

    // Create the repo
    let dot_oxen = base_dir.join(constants::OXEN_HIDDEN_DIR);
    util::fs::remove_dir_all(&dot_oxen);

    let mut repo = repositories::init(&base_dir)?;
    let first_path = base_dir.join("first");
    write_file_for_workspace_add_benchmark(&first_path, 0.0)?;
    
    repositories::add(&repo, &first_path).await?;
    let commit = repositories::commit(&repo, "Add a file")?;

    // Create the workspace
    let workspace_id = Uuid::new_v4().to_string();
    let workspace = repositories::workspaces::create(&repo, &commit, &workspace_id, true)?;

    // Create the dir
    let dir = "dir".to_string();

    Ok((repo, workspace, dir))
}

pub fn workspace_add_benchmark(c: &mut Criterion, data_path: Option<String>) {

    // Create the benchmark dir
    let bench_id = Uuid::new_v4().to_string();
    let base_dir = PathBuf::from("data/test/benches/workspace_add").join(&bench_id);
    if base_dir.exists() {
        util::fs::remove_dir_all(&base_dir).unwrap();
    }
    util::fs::create_dir_all(&base_dir).unwrap();
    println!("bench id: {bench_id}");

    let mut rng = rand::thread_rng();
    let large_file_percentage: f64 = 0.0;

    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("workspace_add");
    group.sample_size(50);

    let params = [100000];
    use walkdir::WalkDir;
    for &num_files in params.iter() {
        // Write the files for this benchmark
        util::fs::remove_dir_all(base_dir.join("files"));

        let files_dir = base_dir.join("files");
        util::fs::create_dir_all(&files_dir).unwrap();

        let files: Vec<FileWithHash> = if let Some(ref data_path) = data_path { 
        
            let mut files = vec![];
            for entry in WalkDir::new(&data_path).into_iter().filter_map(|e| e.ok()) {
                    // Walkdir outputs full paths
                    let entry_path = entry.path().to_path_buf();
                    let hash = util::hasher::hash_file_contents(&entry_path).unwrap();

                    files.push(FileWithHash {path: entry_path, hash});
                    
                }

            files
            
        // Otherwise, generate new test files
        } else {
            let mut files_dirs: Vec<PathBuf> = 
                (0..(num_files / 100))
                    .map(|_| {
                        let depth = rng.gen_range(1..=4);
                        (0..depth).fold(files_dir.clone(), |path, _| {
                            path.join(generate_random_string(10))
                        })
                    })
                    .collect();
            
            files_dirs.push(files_dir);

            (0..num_files)
                .map(|i| {
                    let dir = &files_dirs[rng.gen_range(0..files_dirs.len())];
                    util::fs::create_dir_all(dir)?;

                    let file_path = dir.join(format!("file_{i}.txt"));
                    write_file_for_workspace_add_benchmark(&file_path, large_file_percentage).unwrap();
                    
                    let hash = util::hasher::hash_file_contents(&file_path)?;

                    Ok::<FileWithHash, OxenError>(FileWithHash {
                        hash,
                        path: file_path,
                    })
                })
                .collect::<Result<Vec<_>, _>>().unwrap()
        };

        group.bench_with_input(
            BenchmarkId::from_parameter(num_files),
            &num_files,
            |b, &n| {
                b.iter_batched(
                    || {
                        rt.block_on(async {

                            setup_repo_for_workspace_add_benchmark(&base_dir).await.unwrap()
                        })
                    },
                    |(repo, workspace, dir)| {
                        core::v_latest::workspaces::files::add_version_files(&repo, &workspace, black_box(&files), &dir)
                            .unwrap();

                        let dot_oxen = base_dir.join(constants::OXEN_HIDDEN_DIR);
                        util::fs::remove_dir_all(&dot_oxen);
                            
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
    util::fs::remove_dir_all(&base_dir);
}
