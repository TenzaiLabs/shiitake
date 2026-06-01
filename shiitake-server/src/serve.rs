//! Bind public API + worker dispatch listeners and run them until SIGINT.
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
        _ = tokio::signal::ctrl_c() => {
            info!("SIGINT received; shutting down");
        }
    }
    Ok(())
}
