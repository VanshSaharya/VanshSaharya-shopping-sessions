//! Shopping Sessions pipeline (rules.md). Fast packed-key path, with a string-based
//! fallback used only if some ID is longer than 15 bytes.

mod fast;
mod slow;

use std::path::PathBuf;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let ncpu: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().unwrap();
    let force_slow = std::env::var("FORCE_SLOW").is_ok();
    if force_slow || fast::run(data.clone(), out.clone(), ncpu).is_err() {
        eprintln!("using string-based path");
        let _ = std::fs::remove_dir_all(out.join("user_daily"));
        let _ = std::fs::remove_dir_all(out.join("campaign_daily"));
        slow::run(data, out, ncpu);
    }
}
