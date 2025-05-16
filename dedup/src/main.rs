use clap::{Parser, Subcommand};
use std::fs;
use std::path::{PathBuf};
use std::time::{SystemTime, UNIX_EPOCH, Instant, Duration};
use crate::chunker::{Algorithm,get_chunker, FrameworkResult, FrameworkError};
use sysinfo::{ProcessesToUpdate, System, get_current_pid};

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

        #[arg(short, long)]
        input_file: PathBuf,

        #[arg(short, long)]
        output_dir: PathBuf,
    },

    Unpack {

        #[arg(short, long, value_enum)]
        algorithm: Algorithm,

        #[arg(short, long)]
        input_dir: PathBuf,

        #[arg(short, long)]
        output_file: PathBuf,
    },


    Test {

        #[arg(short, long, value_enum)]
        algorithm: Algorithm,

        #[arg(short, long)]
        input_file: PathBuf,

        #[arg(short, long)]
        use_temp: bool,
    },
}

fn main() -> FrameworkResult<()> {
    let args = Args::parse();

    match args.command {
        Commands::Pack { algorithm, input_file, output_dir } => {
            let chunker = get_chunker(&algorithm)?;
            chunker.pack(&input_file, &output_dir)?;

            Ok(())
        }
        Commands::Unpack { algorithm, input_dir, output_file } => {
            let chunker = get_chunker(&algorithm)?;
            let _ = chunker.unpack(&input_dir, &output_file);
            Ok(())
        }

        Commands::Test { algorithm, input_file , use_temp} =>{

            let mut metrics = TestMetrics {
                pack_time: Duration::new(0, 0),
                unpack_time: Duration::new(0, 0),
                _pack_cpu_usage: 0.0,
                _pack_memory_usage_bytes: 0,
                _unpack_cpu_usage: 0.0,
                _unpack_memory_usage_bytes: 0,
            };
            let chunker = get_chunker(&algorithm)?;


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

            let pid = get_current_pid().expect("Failed to get current PID");
            let mut sys = System::new();
            sys.refresh_processes(ProcessesToUpdate::All, true );
            let _proc_before_pack = sys.process(pid);

            println!("Packing {:?} into {:?}", input_file, test_dir);
            let pack_start_time = Instant::now();
            chunker.pack(&input_file, &test_dir)?;
            metrics.pack_time = pack_start_time.elapsed();

            // sys.refresh_processes(ProcessesToUpdate::All, true);
            let proc_after_pack = sys.process(pid).ok_or_else(|| FrameworkError::InternalError { message: "Current process not found after pack".to_string() })?;

            let pack_cpu_usage = proc_after_pack.cpu_usage(); // CPU usage percentage since last refresh
            let pack_memory_usage_bytes = proc_after_pack.memory(); 
        
            println!("CPU usage during packing: {}%", pack_cpu_usage);
            println!("Memory usage during packing: {} bytes", pack_memory_usage_bytes);

            let unpacked_output_file = test_dir.join("unpacked_output");
            println!("Unpacking from {:?} to {:?}", test_dir, unpacked_output_file);

            let unpack_start_time = Instant::now();
            let _ = chunker.unpack(&test_dir, &unpacked_output_file)?;
            metrics.unpack_time = unpack_start_time.elapsed();


            // show metrics
            println!("Unpack step in test finished in {:?}", metrics.unpack_time);    

            println!("Verifying unpacked file content...");
            let original_content = fs::read(&input_file)?;
            let unpacked_content = fs::read(&unpacked_output_file)?;

            if original_content == unpacked_content {
                println!("Verification successful: Unpacked file matches original.");
            } else {
                println!("Verification FAILED: Unpacked file does NOT match original.");
                return Err(FrameworkError::VerificationFailed); 
            }

            println!("Test completed successfully. Packed and unpacked files are in: {:?}", test_dir);

            Ok(())
        }
    }
}
