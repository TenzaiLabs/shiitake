//! Worker connection lifecycle: connect to the server's dispatch WS,
//! advertise availability, run one command, report the result, exit 0.
//!
//! Re-running per-command is intentional: each container is fresh after
//! K8s restart, which the user wants for hostile-command isolation.

use crate::exec;
use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use shiitake_worker_api::{Frame, ResultFrame};
use std::{path::Path, time::Duration};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tracing::{info, warn};

/// Single-shot run: connect, send Hello, await Execute, run, report.
/// Listens for Cancel frames while exec is in flight.
pub async fn run_one_command(
    worker_id: &str,
    dispatch_port: u16,
    capture_root: &Path,
) -> Result<()> {
    // The dispatcher is always on loopback — the worker shares the pod's
    // network namespace with the server — so only the port is configurable.
    let dispatch_url = format!("ws://127.0.0.1:{dispatch_port}/dispatch");

    info!(%worker_id, dispatch = %dispatch_url, "connecting");
    let req = dispatch_url
        .as_str()
        .into_client_request()
        .context("invalid WS url")?;

    let (ws, _resp) = tokio::time::timeout(
        Duration::from_secs(30),
        tokio_tungstenite::connect_async(req),
    )
    .await
    .context("timeout connecting to dispatcher")?
    .context("ws connect")?;

    let (mut sink, mut stream) = ws.split();

    let hello = serde_json::to_string(&Frame::Hello {
        worker_id: worker_id.to_string(),
    })?;
    sink.send(Message::Text(hello.into()))
        .await
        .context("send Hello")?;

    let execute = loop {
        match stream.next().await {
            Some(Ok(Message::Text(t))) => match serde_json::from_str::<Frame>(&t)? {
                Frame::Execute(e) => break e,
                Frame::Cancel { .. } => {
                    warn!("received Cancel before Execute; ignoring");
                }
                Frame::Hello { .. } | Frame::Result(_) => {
                    warn!("unexpected frame from server; ignoring");
                }
            },
            Some(Ok(Message::Ping(p))) => {
                sink.send(Message::Pong(p)).await.ok();
            }
            Some(Ok(Message::Close(_))) | None => bail!("dispatcher closed before Execute"),
            Some(Err(e)) => bail!("ws error: {e}"),
            _ => {}
        }
    };

    info!(request_id = %execute.request_id, "executing");
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let target_request_id = execute.request_id.clone();

    let cancel_listener = tokio::spawn(async move {
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(Message::Text(t)) => match serde_json::from_str::<Frame>(&t) {
                    Ok(Frame::Cancel { request_id }) if request_id == target_request_id => {
                        info!(%request_id, "cancel received");
                        let _ = cancel_tx.send(true);
                        return;
                    }
                    Ok(Frame::Cancel { request_id }) => {
                        warn!(%request_id, "cancel for unknown request_id; ignoring");
                    }
                    Ok(_) => {}
                    Err(e) => warn!("frame parse error during exec: {e}"),
                },
                Ok(Message::Close(_)) | Err(_) => return,
                _ => {}
            }
        }
    });

    let result = match exec::run(&execute, cancel_rx, capture_root).await {
        Ok(o) => o.result,
        Err(e) => {
            warn!("exec failure: {e:#}");
            ResultFrame::errored(execute.request_id.clone(), format!("worker error: {e:#}"))
        }
    };

    // Stop listening for further Cancel frames before we send Result.
    cancel_listener.abort();

    let result_json = serde_json::to_string(&Frame::Result(result))?;
    sink.send(Message::Text(result_json.into()))
        .await
        .context("send Result")?;
    sink.close().await.ok();
    info!("done; exit");
    Ok(())
}
