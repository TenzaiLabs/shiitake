//! Standalone shiitake-worker binary. Connects to the local dispatch WS and
//! serves commands for the lifetime of the process: receive an Execute, run it,
//! report the Result, then reset the sandbox to a clean slate before accepting
//! the next one.
//!
//! Staying resident (rather than exiting per command and being restarted by the
//! orchestrator) avoids the kubelet CrashLoopBackOff that rapid per-command
//! container exits incur. The per-command isolation a fresh container gave is
//! reproduced in-process by `reset` — see that module. `SHIITAKE_RESTART_AFTER`
//! optionally re-introduces a periodic full-container teardown (exit after N
//! commands) on top of that. Either way the process exits 0, so a recycle never
//! reads as a crash. The wire/disk contract comes from the
//! `shiitake-worker-api` crate.

mod cgroup;
mod client;
mod exec;
mod reset;

use clap::Parser;
use client::{ClientConfig, run};

#[derive(Parser)]
#[command(
    name = "shiitake-worker",
    about = "Resident sandbox worker: serves commands over the dispatch WS, \
             resetting between each."
)]
struct Config {
    #[command(flatten)]
    client: ClientConfig,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let cfg = Config::parse();
    // Always exit 0: whether the worker reaches its restart-after quota or a
    // session errors out, a non-zero exit would make routine recycles look like
    // crashes (CrashLoopBackOff). The orchestrator restarts it via the
    // container restartPolicy; the command's own exit status rides the Result
    // frame, never the worker process exit code.
    if let Err(e) = run(&cfg.client).await {
        tracing::warn!("worker exited with error: {e:#}");
    }
}
