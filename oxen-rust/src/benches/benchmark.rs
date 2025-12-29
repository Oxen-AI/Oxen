use criterion::Criterion;
use std::env;

mod oxen;
use crate::oxen::add;
use crate::oxen::download;
use crate::oxen::fetch;
use crate::oxen::push;
use crate::oxen::workspace_add;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut c = Criterion::default().configure_from_args();

    let benchmark_name = args.get(1);
    let data_path = env::var("BENCHMARK_DATA").ok();
    let iters_str = env::var("BENCHMARK_ITERS").ok();

    if let Some(name) = benchmark_name {
        let iters = iters_str
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(10);

        match name.as_str() {
            "add" => add::add_benchmark(&mut c, data_path, Some(iters)),
            "push" => push::push_benchmark(&mut c, data_path, Some(iters)),
            "workspace_add" => {
                workspace_add::workspace_add_benchmark(&mut c)
            }
            "fetch" => fetch::fetch_benchmark(&mut c, data_path, Some(iters)),
            "download" => download::download_benchmark(&mut c, data_path, Some(iters)),
            _ => {
                eprintln!("Benchmark not found: {name}");
                std::process::exit(1);
            }
        }
    } else {
        eprintln!("Error parsing args for benchmark");
        std::process::exit(1);
    }

    c.final_summary();
}
