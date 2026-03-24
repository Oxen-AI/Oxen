use crate::chunker::{get_chunker, Algorithm, FrameworkError, FrameworkResult};
use chunker::oxendedup::OxenChunker;
use clap::{Parser, Subcommand};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub mod chunker;
pub mod xhash;

struct TestMetrics {
    pack_time: Duration,
    unpack_time: Duration,
    _pack_cpu_usage: f32,
    _pack_memory_usage_bytes: u64,
    _unpack_cpu_usage: f32,
    _unpack_memory_usage_bytes: u64,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Pack {
        #[arg(short, long, value_enum)]
        algorithm: Algorithm,

        #[arg(short, long, value_enum)]
        chunk_size: usize,

        #[arg(short, long)]
        input_file: PathBuf,

        #[arg(short, long)]
        output_dir: PathBuf,
    },

    Unpack {
        #[arg(short, long, value_enum)]
        algorithm: Algorithm,

        #[arg(short, long, value_enum)]
        chunk_size: usize,

        #[arg(short, long)]
        input_dir: PathBuf,

        #[arg(short, long)]
        output_file: PathBuf,
    },

    Test {
        #[arg(short, long, value_enum)]
        algorithm: Algorithm,

        #[arg(short, long, value_enum)]
        chunk_size: usize,

        #[arg(short, long)]
        input_file: PathBuf,

        #[arg(short, long)]
        use_temp: bool,
    },

    TestOxen {
        #[arg(short, long, value_enum)]
        algorithm: Algorithm,

        #[arg(short, long, value_enum)]
        chunk_size: usize,

        #[arg(short, long)]
        input_file: PathBuf,

        #[arg(short, long)]
        use_temp: bool,

        #[arg(short, long)]
        n_commits: u8,
    },
}

fn main() -> FrameworkResult<()> {
    let args = Args::parse();

    match args.command {
        Commands::Pack {
            algorithm,
            chunk_size,
            input_file,
            output_dir,
        } => {
            let chunker = get_chunker(&algorithm, chunk_size)?;
            chunker.pack(&input_file, &output_dir)?;

            Ok(())
        }
        Commands::Unpack {
            algorithm,
            chunk_size,
            input_dir,
            output_file,
        } => {
            let chunker = get_chunker(&algorithm, chunk_size)?;
            let _ = chunker.unpack(&input_dir, &output_file);
            Ok(())
        }

        Commands::TestOxen {
            algorithm,
            chunk_size,
            input_file,
            use_temp,
            n_commits,
        } => {
            let base_dir = if use_temp {
                let temp_dir = std::env::temp_dir();
                temp_dir
            } else {
                std::env::current_dir()?
            };
            let oxen_dedup = OxenChunker::new(
                64 * 1024,
                algorithm.as_str().to_string(),
                PathBuf::from(".oxen/versions/files"),
            )?;
            let mut _metrics = TestMetrics {
                pack_time: Duration::new(0, 0),
                unpack_time: Duration::new(0, 0),
                _pack_cpu_usage: 0.0,
                _pack_memory_usage_bytes: 0,
                _unpack_cpu_usage: 0.0,
                _unpack_memory_usage_bytes: 0,
            };
            let timestamp_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| FrameworkError::TimeError {
                    message: "System time went backwards".to_string(),
                    source: e,
                })?
                .as_nanos();

            let test_dir_name = format!("chunker_test_{}", timestamp_nanos);
            let test_dir = base_dir.join(test_dir_name);
            oxen_dedup.pack(algorithm, chunk_size, &input_file, &test_dir, n_commits)?;
            Ok(())
        }

        Commands::Test {
            algorithm,
            chunk_size,
            input_file,
            use_temp,
        } => {
            let mut metrics = TestMetrics {
                pack_time: Duration::new(0, 0),
                unpack_time: Duration::new(0, 0),
                _pack_cpu_usage: 0.0,
                _pack_memory_usage_bytes: 0,
                _unpack_cpu_usage: 0.0,
                _unpack_memory_usage_bytes: 0,
            };
            let chunker = get_chunker(&algorithm, chunk_size)?;

            let base_dir = if use_temp {
                let temp_dir = std::env::temp_dir();
                temp_dir
            } else {
                std::env::current_dir()?
            };

            let timestamp_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| FrameworkError::TimeError {
                    message: "System time went backwards".to_string(),
                    source: e,
                })?
                .as_nanos();

            let test_dir_name = format!("chunker_test_{}", timestamp_nanos);
            let test_dir = base_dir.join(test_dir_name);

            println!("Creating test directory: {:?}", test_dir);

            fs::create_dir_all(&test_dir)?;

            println!("Packing {:?} into {:?}", input_file, test_dir);
            let pack_start_time = Instant::now();
            chunker.pack(&input_file, &test_dir)?;
            metrics.pack_time = pack_start_time.elapsed();

            let unpacked_output_file = test_dir.join("unpacked_output");
            println!(
                "Unpacking from {:?} to {:?}",
                test_dir, unpacked_output_file
            );

            let unpack_start_time = Instant::now();
            let _ = chunker.unpack(&test_dir, &unpacked_output_file)?;
            metrics.unpack_time = unpack_start_time.elapsed();

            // show metrics
            println!("packing time: {:?}", metrics.pack_time);
            println!("unpack time : {:?}", metrics.unpack_time);

            println!("verifying unpacked file.");
            // Verify the unpacked file matches the original by hashing both files
            // and comparing the hashes

            let original_file_hash = xhash::hash_file_128bit(&input_file).map_err(|e| {
                FrameworkError::InternalError {
                    message: format!("Failed to hash original file: {}", e),
                }
            })?;
            let unpacked_file_hash =
                xhash::hash_file_128bit(&unpacked_output_file).map_err(|e| {
                    FrameworkError::InternalError {
                        message: format!("Failed to hash unpacked file: {}", e),
                    }
                })?;
            println!("Original file hash: {}", original_file_hash);
            if original_file_hash == unpacked_file_hash {
                println!("Verification successful: Unpacked file matches original.");
            } else {
                println!("Verification FAILED: Unpacked file does NOT match original.");
                return Err(FrameworkError::VerificationFailed);
            }
            println!(
                "Test completed successfully. Packed and unpacked files are in: {:?}",
                test_dir
            );

            Ok(())
        }
    }
}
