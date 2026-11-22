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

mod account;
mod cgroup;
mod client;
mod exec;
mod pty;
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

/// Record the Bash that sessions run in, the first on this process's PATH.
/// Cancel needs Bash 5.0 or newer: an older one keeps running the rest of a
/// cancelled command line and reports status 0 or 1 for it.
async fn log_session_shell() {
    let version = tokio::process::Command::new("bash")
        .args(["-c", "printf %s \"$BASH_VERSION\""])
        .output()
        .await;
    match version {
        Ok(out) if out.status.success() => {
            let bash = String::from_utf8_lossy(&out.stdout);
            let major = bash.split('.').next().and_then(|m| m.parse::<u32>().ok());
            if major.is_some_and(|major| major >= 5) {
                tracing::info!(%bash, "session shell");
            } else {
                tracing::warn!(
                    %bash,
                    "session shell is older than Bash 5.0; a cancelled command line keeps running"
                );
            }
        }
        Ok(out) => tracing::warn!(status = %out.status, "could not read the session shell version"),
        Err(e) => tracing::warn!("could not run bash to read the session shell version: {e}"),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    // Session commands run beside this process and can be root. Without the
    // dumpable flag, /proc/<pid>/environ and memory need CAP_SYS_PTRACE, which
    // keeps SHIITAKE_DISPATCH_TOKEN out of their reach. Children regain the
    // default at exec.
    #[cfg(target_os = "linux")]
    nix::sys::prctl::set_dumpable(false).expect("clear the dumpable flag");
    let cfg = Config::parse();
    log_session_shell().await;
    // Always exit 0: whether the worker reaches its restart-after quota, a
    // session errors out, or it catches a termination signal (SIGTERM on pod
    // teardown), a non-zero exit would make routine recycles look like crashes
    // (CrashLoopBackOff) and a planned teardown surface as an "Error" in the
    // supervisor. The orchestrator restarts it via the container restartPolicy;
    // the command's own exit status rides the Result frame, never the worker
    // process exit code.
    if let Err(e) = run(&cfg.client).await {
        tracing::warn!("worker exited with error: {e:#}");
    }
}
