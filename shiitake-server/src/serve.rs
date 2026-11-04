//! Bind public API + worker dispatch listeners and run them until a termination
//! signal (SIGTERM on a Kubernetes pod teardown, or SIGINT for a local Ctrl-C).
//!
//! Downstream embedders pass in their already-composed Router (which usually
//! starts from `http::build_api_router` and adds their own routes) and the
//! dispatch Router from `http::build_dispatch_router`.

use anyhow::{Context, Result};
use axum::Router;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

pub struct ListenAddrs {
    pub api: SocketAddr,
    pub dispatch: SocketAddr,
}

pub async fn run(addrs: ListenAddrs, api: Router, dispatch: Router) -> Result<()> {
    let api_listener = TcpListener::bind(addrs.api).await.context("bind api")?;
    let dispatch_listener = TcpListener::bind(addrs.dispatch)
        .await
        .context("bind dispatch")?;
    info!(api_addr = %addrs.api, dispatch_addr = %addrs.dispatch, "shiitake-server listening");

    let api_handle =
        tokio::spawn(async move { axum::serve(api_listener, api.into_make_service()).await });
    let dispatch_handle =
        tokio::spawn(
            async move { axum::serve(dispatch_listener, dispatch.into_make_service()).await },
        );

    tokio::select! {
        res = api_handle => res??,
        res = dispatch_handle => res??,
        _ = shutdown_signal() => {
            info!("termination signal received; shutting down");
        }
    }
    Ok(())
}

/// Resolves on SIGTERM (a Kubernetes pod teardown) or SIGINT (local Ctrl-C), so
/// `main` can flush telemetry and exit 0 instead of being hard-killed. Trapping
/// only SIGINT (the prior behaviour) meant every k8s teardown was a hard kill.
#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    async fn wait(kind: SignalKind, name: &str) {
        match signal(kind) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::warn!("cannot install {name} handler: {e}");
                std::future::pending::<()>().await;
            }
        }
    }
    tokio::select! {
        _ = wait(SignalKind::terminate(), "SIGTERM") => {}
        _ = wait(SignalKind::interrupt(), "SIGINT") => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
