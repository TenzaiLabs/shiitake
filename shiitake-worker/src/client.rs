//! Worker connection lifecycle: connect to the server's dispatch WS, advertise
//! availability, then serve commands in a loop — run one, report the Result,
//! reset the sandbox, await the next. The worker stays resident; `reset`
//! reproduces the clean slate a fresh container used to give (see `reset`).
//!
//! The dispatcher is addressed by a full URL (`SHIITAKE_DISPATCH_URL`), so the
//! server may sit in the same pod or behind a Service in another. That path is
//! no longer protected by being loopback-bound, so every connection carries a
//! bearer token the server validates on the upgrade.
//!
//! If the connection drops the worker reconnects rather than exiting, so a
//! server restart or network blip doesn't churn the container.
//!
//! `restart_after` adds an optional full-teardown layer: after that many
//! commands the worker exits (process ends → fresh container), which bounds
//! anything the in-process reset can't scrub. `0` disables it (pure resident);
//! `1` exits after every command (a fresh container per command).

use crate::{exec, reset};
use anyhow::{Context, Result};
use clap::Args;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use shiitake_worker_api::{ExecuteFrame, Frame, ResultFrame, WorkerId, WorkerLocation};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{net::TcpStream, sync::watch, time::sleep};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::header::{AUTHORIZATION, HeaderValue},
    },
};
use tracing::{debug, info, warn};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Sink = SplitSink<Ws, Message>;
type Stream = SplitStream<Ws>;

/// Delay before reconnecting after a session ends, so a persistently-failing
/// server doesn't spin the worker. The orchestrator's restart policy is the
/// ultimate backstop if the worker process itself dies.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// One served command's outcome on the live session.
enum CmdOutcome {
    /// Command finished (normally, timed out, or cancelled); its Result is ready.
    Done(ResultFrame),
    /// The connection dropped mid-command. The session must end; the server
    /// reconciles the in-flight handle when it sees the socket close.
    ConnectionLost,
}

/// Why a dispatch session ended.
enum SessionEnd {
    /// The worker served its configured command quota; the process should exit
    /// so the orchestrator hands it a brand-new container — a full-teardown
    /// reset layered on top of the per-command in-process reset.
    RestartQuotaReached,
    /// A between-command reset failed, so the sandbox can no longer be trusted
    /// as clean. The process exits to be replaced by a fresh container (itself a
    /// clean slate); a worker is only safe to reuse once it has reset cleanly.
    ResetFailed,
    /// The connection closed (clean close or mid-command loss); reconnect.
    Closed,
}

/// CLI/env configuration for the resident worker. `main` flattens this into the
/// binary's top-level args, so every knob the worker loop reads lives here next
/// to the code that consumes it.
#[derive(Args)]
pub struct ClientConfig {
    #[arg(long, env = "SHIITAKE_WORKER_ID", default_value = "worker-unknown")]
    pub worker_id: String,
    /// Full WebSocket URL of the server's dispatch endpoint. Defaults to the
    /// single-pod case; point it at a Service to split the two apart.
    #[arg(
        long,
        env = "SHIITAKE_DISPATCH_URL",
        default_value = "ws://127.0.0.1:8090/dispatch"
    )]
    pub dispatch_url: String,
    /// Bearer token presented on the dispatch upgrade. Must match the server's.
    /// Required: the dispatch path may cross pods.
    #[arg(
        long,
        env = "SHIITAKE_DISPATCH_TOKEN",
        hide_env_values = true,
        value_parser = clap::builder::NonEmptyStringValueParser::new()
    )]
    pub dispatch_token: String,
    /// This worker pod's name, from the downward API, so the server's OOM probe
    /// queries the right pod. Empty outside k8s.
    #[arg(long, env = "POD_NAME", default_value = "")]
    pub pod_name: String,
    /// This worker pod's namespace, from the downward API. See `pod_name`.
    #[arg(long, env = "POD_NAMESPACE", default_value = "")]
    pub pod_namespace: String,
    /// Container name within that pod, for the same probe. Defaults to the
    /// worker id, which is how single-pod names its worker containers.
    #[arg(long, env = "SHIITAKE_CONTAINER_NAME", default_value = "")]
    pub container_name: String,
    #[arg(long, env = "SHIITAKE_CAPTURE_ROOT", default_value = "/capture")]
    pub capture_root: PathBuf,
    /// Writable scratch paths emptied between commands (comma-separated, e.g.
    /// `SHIITAKE_RESET_PATHS=/tmp,/var/tmp,/dev/shm`). Shiitake is path-agnostic:
    /// the embedding layer lists only per-command scratch here and omits any
    /// directory whose contents must persist across commands. Empty by default.
    #[arg(long, env = "SHIITAKE_RESET_PATHS", default_value = "")]
    pub reset_paths: String,
    /// Exit (for a fresh container) after this many commands. `0` = never (stay
    /// resident, relying only on the in-process reset between commands); `1` =
    /// after every command (a fresh container per command); `N` = every N. A
    /// periodic full-container teardown bounds anything the in-process reset
    /// can't scrub. Pair with a container `restartPolicy: Always`.
    #[arg(long, env = "SHIITAKE_RESTART_AFTER", default_value_t = 0)]
    pub restart_after: u64,
}

impl ClientConfig {
    /// Parse the comma-separated `reset_paths` into a path list. An unset/empty
    /// `SHIITAKE_RESET_PATHS` means "clear nothing" (no scratch paths), not
    /// "clear ''", so blank segments are dropped.
    fn scratch_paths(&self) -> Vec<PathBuf> {
        self.reset_paths
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .collect()
    }

    /// Where this worker's container lives, for the server's OOM probe. `None`
    /// when the pod identity isn't wired in (a local run) — the server then
    /// looks in its own pod for a container named after the worker id.
    fn location(&self) -> Option<WorkerLocation> {
        if self.pod_name.is_empty() || self.pod_namespace.is_empty() {
            return None;
        }
        let container = if self.container_name.is_empty() {
            self.worker_id.clone()
        } else {
            self.container_name.clone()
        };
        Some(WorkerLocation {
            pod: self.pod_name.clone(),
            namespace: self.pod_namespace.clone(),
            container,
        })
    }
}

/// Serve commands for the lifetime of the worker process. Reconnects across
/// connection loss; returns `Ok(())` (so the process exits 0) once the
/// `restart_after` quota is reached. `restart_after == 0` never returns.
pub async fn run(cfg: &ClientConfig) -> Result<()> {
    let reset_paths = cfg.scratch_paths();
    let location = cfg.location();
    let mut served: u64 = 0;
    loop {
        match serve_session(cfg, location.clone(), &reset_paths, &mut served).await {
            Ok(SessionEnd::RestartQuotaReached) => {
                info!(
                    served,
                    restart_after = cfg.restart_after,
                    "restart-after quota reached; exiting for a fresh container"
                );
                return Ok(());
            }
            Ok(SessionEnd::ResetFailed) => {
                warn!("sandbox reset failed; exiting for a fresh container");
                return Ok(());
            }
            Ok(SessionEnd::Closed) => info!("dispatch session ended; reconnecting"),
            Err(e) => warn!("dispatch session error: {e:#}; reconnecting"),
        }
        sleep(RECONNECT_DELAY).await;
    }
}

/// Connect, Hello, then serve commands on this one connection until it closes.
async fn serve_session(
    cfg: &ClientConfig,
    location: Option<WorkerLocation>,
    reset_paths: &[PathBuf],
    served: &mut u64,
) -> Result<SessionEnd> {
    let ClientConfig {
        worker_id,
        dispatch_url,
        dispatch_token,
        capture_root,
        restart_after,
        ..
    } = cfg;
    let capture_root: &Path = capture_root;
    info!(%worker_id, dispatch = %dispatch_url, "connecting");
    let mut req = dispatch_url
        .as_str()
        .into_client_request()
        .context("invalid SHIITAKE_DISPATCH_URL")?;
    // On the upgrade itself, so the server rejects the handshake before any
    // frame is exchanged.
    let mut bearer = HeaderValue::try_from(format!("Bearer {dispatch_token}"))
        .context("SHIITAKE_DISPATCH_TOKEN is not a valid header value")?;
    bearer.set_sensitive(true);
    req.headers_mut().insert(AUTHORIZATION, bearer);

    let (ws, _resp) = tokio::time::timeout(
        Duration::from_secs(30),
        tokio_tungstenite::connect_async(req),
    )
    .await
    .context("timeout connecting to dispatcher")?
    .context("ws connect")?;

    let (mut sink, mut stream) = ws.split();

    let hello = serde_json::to_string(&Frame::Hello {
        worker_id: WorkerId::new(worker_id.as_str()),
        location,
    })?;
    sink.send(Message::Text(hello.into()))
        .await
        .context("send Hello")?;

    loop {
        // Phase 1: wait for the next command (or session end).
        let Some(execute) = next_execute(&mut sink, &mut stream).await? else {
            return Ok(SessionEnd::Closed); // dispatcher closed cleanly between commands
        };

        // Phase 2: run it, watching for a Cancel on the same socket.
        info!(request_id = %execute.request_id, "executing");
        let outcome = run_command(execute, &mut sink, &mut stream, capture_root).await;
        let result = match outcome {
            CmdOutcome::Done(r) => r,
            CmdOutcome::ConnectionLost => return Ok(SessionEnd::Closed),
        };

        *served += 1;
        let exiting = *restart_after != 0 && *served >= *restart_after;

        // Phase 3: reset to give the NEXT command a clean slate — skipped when
        // we're about to exit, because the fresh container the orchestrator
        // hands us is itself the reset. Resetting before reporting the Result
        // also keeps the worker "in-flight" (not ping-pinged) and means the
        // server only re-advertises it once the sandbox is already clean. If the
        // reset fails the sandbox can't be trusted, so we report this command's
        // Result and then recycle rather than serve another command on it.
        let mut reset_failed = false;
        if !exiting {
            let paths = reset_paths.to_vec();
            match tokio::task::spawn_blocking(move || reset::reset(&paths)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!("sandbox reset failed: {e:#}; recycling worker");
                    reset_failed = true;
                }
                Err(e) => {
                    // The reset task panicked; treat it as a failed reset.
                    warn!("reset task panicked: {e}; recycling worker");
                    reset_failed = true;
                }
            }
        }

        // Phase 4: report. The just-finished command's Result is valid
        // regardless of the reset outcome, so always send it. A send failure
        // means the socket is gone — end the session and let the reconnect loop
        // take over.
        let result_json = serde_json::to_string(&Frame::Result(result))?;
        if let Err(e) = sink.send(Message::Text(result_json.into())).await {
            warn!("send Result failed: {e}; ending session");
            return Ok(SessionEnd::Closed);
        }

        // Exit for a fresh container when the quota is reached or a reset failed.
        // Close first so the server stops dispatching to us; a command racing
        // onto us in the re-advertise/close window is reconciled by the server's
        // worker-drop path, exactly like any other worker exit.
        if exiting {
            if let Err(e) = sink.close().await {
                debug!("closing the dispatch socket before restart failed: {e}");
            }
            return Ok(SessionEnd::RestartQuotaReached);
        }
        if reset_failed {
            if let Err(e) = sink.close().await {
                debug!("closing the dispatch socket after a failed reset: {e}");
            }
            return Ok(SessionEnd::ResetFailed);
        }
    }
}

/// Read frames until an Execute arrives. Answers Pings, ignores stray Cancels
/// (nothing is in flight between commands). `Ok(None)` on a clean close.
async fn next_execute(sink: &mut Sink, stream: &mut Stream) -> Result<Option<ExecuteFrame>> {
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(t))) => match serde_json::from_str::<Frame>(&t)? {
                Frame::Execute(e) => return Ok(Some(e)),
                Frame::Cancel { .. } => warn!("Cancel with no command in flight; ignoring"),
                Frame::Hello { .. } | Frame::Result(_) => warn!("unexpected frame; ignoring"),
            },
            Some(Ok(Message::Ping(p))) => {
                if let Err(e) = sink.send(Message::Pong(p)).await {
                    warn!("failed to answer a keepalive ping; the server may evict us: {e}");
                }
            }
            Some(Ok(Message::Close(_))) | None => return Ok(None),
            Some(Err(e)) => return Err(e).context("ws read"),
            _ => {}
        }
    }
}

/// Run one command while concurrently reading the socket so a Cancel for this
/// request SIGKILLs it. Keeps the stream owned here (no spawned reader) so the
/// session loop can reuse it for the next command.
async fn run_command(
    execute: ExecuteFrame,
    sink: &mut Sink,
    stream: &mut Stream,
    capture_root: &Path,
) -> CmdOutcome {
    let request_id = execute.request_id.clone();
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let mut exec_fut = std::pin::pin!(exec::run(&execute, cancel_rx, capture_root));
    let mut connection_lost = false;

    let result = loop {
        if connection_lost {
            // Cancel already signalled; just let exec wind down so we don't
            // leak the child, then end the session.
            if let Err(e) = (&mut exec_fut).await {
                warn!("exec failed while winding down after connection loss: {e:#}");
            }
            return CmdOutcome::ConnectionLost;
        }
        tokio::select! {
            biased;
            res = &mut exec_fut => {
                break match res {
                    Ok(o) => o.result,
                    Err(e) => {
                        warn!("exec failure: {e:#}");
                        ResultFrame::errored(request_id.clone(), format!("worker error: {e:#}"))
                    }
                };
            }
            msg = stream.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    if let Ok(Frame::Cancel { request_id: rid }) = serde_json::from_str::<Frame>(&t)
                        && rid == request_id
                    {
                        info!(%request_id, "cancel received");
                        if cancel_tx.send(true).is_err() {
                            debug!("command already finished; cancel had no receiver");
                        }
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    if let Err(e) = sink.send(Message::Pong(p)).await {
                        warn!("failed to answer a keepalive ping mid-exec: {e}");
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                    warn!("connection lost during exec; cancelling command");
                    if cancel_tx.send(true).is_err() {
                        debug!("command already finished; cancel had no receiver");
                    }
                    connection_lost = true;
                }
                _ => {}
            }
        }
    };
    CmdOutcome::Done(result)
}
