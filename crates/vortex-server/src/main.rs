//! VortexDB — next-generation in-memory database server.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const BANNER: &str = r"
 __     __       _            ____  ____
 \ \   / /___  _| |_ _____  _|  _ \| __ )
  \ \ / / _ \| '_| __/ _ \ \/ / | | |  _ \
   \ V / (_) | |  | ||  __/>  <| |_| | |_) |
    \_/ \___/|_|   \__\___/_/\_\____/|____/
";

fn main() {
    let config = match vortex_config::VortexConfig::load() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("fatal: {e}");
            std::process::exit(1);
        }
    };

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.log_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    eprintln!("{BANNER}");
    tracing::info!(
        "VortexDB v{} starting — bind={}, threads={}, max_memory={}",
        env!("CARGO_PKG_VERSION"),
        config.bind,
        config.threads,
        config.max_memory,
    );

    let running = Arc::new(AtomicBool::new(true));

    // Register signal handlers for graceful shutdown.
    {
        let running = Arc::clone(&running);
        ctrlc::set_handler(move || {
            tracing::info!("shutdown signal received");
            running.store(false, Ordering::SeqCst);
        })
        .expect("failed to set signal handler");
    }

    tracing::info!("server ready — awaiting connections");

    // Phase 1 replaces this spin with the io-reactor event loop.
    while running.load(Ordering::SeqCst) {
        std::thread::park_timeout(std::time::Duration::from_millis(100));
    }

    tracing::info!("VortexDB shutting down — goodbye");
}
