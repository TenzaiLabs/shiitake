//! Standalone shiitake-worker binary. Connects to the local dispatch WS,
//! receives one Execute, runs it, reports the Result, exits.
//!
//! Config comes from CLI flags or their `SHIITAKE_*` env fallbacks. Re-running
//! per-command is intentional — each container is fresh after K8s restart,
//! which gives hostile-command isolation. The wire/disk contract comes from
//! the `worker-protocol` crate.

mod cgroup;
mod client;
mod exec;

use anyhow::Result;
use clap::Parser;
use client::run_one_command;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "shiitake-worker")]
struct Config {
    #[arg(long, env = "SHIITAKE_WORKER_ID", default_value = "worker-unknown")]
    worker_id: String,
    /// Dispatch port on the server. The host is always loopback — the worker
    /// and server share the pod's network namespace.
    #[arg(long, env = "SHIITAKE_DISPATCH_PORT", default_value_t = 8090)]
    dispatch_port: u16,
    #[arg(long, env = "SHIITAKE_CAPTURE_ROOT", default_value = "/capture")]
    capture_root: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cfg = Config::parse();
    run_one_command(&cfg.worker_id, cfg.dispatch_port, &cfg.capture_root).await
}
