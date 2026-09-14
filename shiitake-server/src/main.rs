//! Standalone shiitake-server binary. Reads config from CLI flags or their
//! `SHIITAKE_*` / `POD_*` env fallbacks, exposes the default API + dispatch
//! routers, and serves until SIGINT.
//!
//! The two listeners are independent — each with its own bind address and bearer
//! token — which is what lets the server and workers deploy as one pod or two.
//!
//! Embedding crates compose the same library pieces (`AppState`,
//! `build_api_router`, `build_dispatch_router`, `WorkerPool`) with their own
//! routes and middleware.

use anyhow::{Context, Result};
use clap::Parser;
use shiitake_server::{
    http::{AppState, build_api_router, build_dispatch_router},
    pool::{WorkerPool, k8s_status::ClusterProbe},
    serve::{ListenAddrs, run},
};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[derive(Parser)]
#[command(name = "shiitake-server")]
struct Config {
    #[arg(long, env = "SHIITAKE_HOST", default_value = "0.0.0.0")]
    host: String,
    #[arg(long, env = "SHIITAKE_PORT", default_value_t = 8080)]
    port: u16,
    /// Worker dispatch listen address. Loopback is all single-pod needs; set
    /// `0.0.0.0` for workers in another pod. Bearer-authenticated either way.
    #[arg(long, env = "SHIITAKE_DISPATCH_HOST", default_value = "127.0.0.1")]
    dispatch_host: String,
    #[arg(long, env = "SHIITAKE_DISPATCH_PORT", default_value_t = 8090)]
    dispatch_port: u16,
    /// Bearer token workers present on the dispatch upgrade. Required, and
    /// distinct from `auth_token` — a worker never needs the API's token.
    #[arg(
        long,
        env = "SHIITAKE_DISPATCH_TOKEN",
        hide_env_values = true,
        value_parser = clap::builder::NonEmptyStringValueParser::new()
    )]
    dispatch_token: String,
    #[arg(long, env = "SHIITAKE_DEFAULT_WORKDIR", default_value = "/")]
    default_workdir: PathBuf,
    #[arg(
        long,
        env = "SHIITAKE_AUTH_TOKEN",
        hide_env_values = true,
        value_parser = clap::builder::NonEmptyStringValueParser::new()
    )]
    auth_token: String,
    #[arg(long, env = "SHIITAKE_MAX_BODY_BYTES", default_value_t = 256 * 1024 * 1024)]
    max_body_bytes: usize,
    #[arg(long, env = "SHIITAKE_CAPTURE_ROOT", default_value = "/capture")]
    capture_root: PathBuf,
    /// Registered workers the pool needs before `/ready` reports ready.
    #[arg(long, env = "SHIITAKE_MIN_READY_WORKERS", default_value_t = 1)]
    min_ready_workers: usize,
    /// How often the `/pty` handler pings a pinned worker to keep its session
    /// lease alive while a client is attached. Must stay below the worker's
    /// `SHIITAKE_LEASE_TIMEOUT`.
    #[arg(long, env = "SHIITAKE_PTY_KEEPALIVE_SECS", default_value_t = 10)]
    pty_keepalive_secs: u64,
    #[arg(long, env = "POD_NAME", default_value = "shiitake")]
    pod_name: String,
    #[arg(long, env = "POD_NAMESPACE", default_value = "")]
    pod_namespace: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Fail fast on any panic — including ones on OpenTelemetry's background
    // export task. The default hook only unwinds the panicking thread, which
    // would leave the server running degraded (e.g. telemetry silently dead);
    // aborting instead exits non-zero so the supervisor restarts the pod.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_hook(info);
        std::process::abort();
    }));

    let telemetry = shiitake_server::telemetry::init("shiitake-server")?;
    let cfg = Config::parse();

    let probe = Some(Arc::new(
        ClusterProbe::new().await.context("kube client init")?,
    ));

    tokio::fs::create_dir_all(&cfg.capture_root)
        .await
        .with_context(|| format!("create capture root {}", cfg.capture_root.display()))?;

    let pool = Arc::new(WorkerPool::new(
        probe,
        cfg.pod_name,
        cfg.pod_namespace,
        cfg.capture_root,
    ));
    // Anything on the volume predates this process, so no handle can name it.
    pool.reconcile_capture().await;

    let state = AppState {
        pool: pool.clone(),
        auth_token: Arc::new(cfg.auth_token),
        default_workdir: cfg.default_workdir,
        max_body_bytes: cfg.max_body_bytes,
        min_ready_workers: cfg.min_ready_workers,
        pty_keepalive: Duration::from_secs(cfg.pty_keepalive_secs),
    };
    let api = build_api_router(state);
    let dispatch = build_dispatch_router(pool.clone(), &cfg.dispatch_token);

    let addrs = ListenAddrs {
        api: format!("{}:{}", cfg.host, cfg.port)
            .parse()
            .context("SHIITAKE_HOST:PORT")?,
        dispatch: format!("{}:{}", cfg.dispatch_host, cfg.dispatch_port)
            .parse()
            .context("SHIITAKE_DISPATCH_HOST:PORT")?,
    };

    // Background loops run alongside the servers in the same task, not detached
    // spawns: if either exits or panics, the select! surfaces it instead of
    // silently leaving the pool unswept or unmonitored.
    let sweeper = pool.clone();
    let keepalive = pool;
    let result = tokio::select! {
        r = run(addrs, api, dispatch) => r,
        _ = sweeper.run_sweeper(
            Duration::from_secs(60),
            Duration::from_secs(60 * 60),
            Duration::from_secs(60 * 60),
        ) => Err(anyhow::anyhow!("sweeper exited unexpectedly")),
        _ = keepalive.run_keepalive(Duration::from_secs(10), Duration::from_secs(30)) =>
            Err(anyhow::anyhow!("keepalive exited unexpectedly")),
    };
    telemetry.shutdown();
    result
}
