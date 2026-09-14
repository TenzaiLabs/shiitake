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

use crate::{exec, pty, reset};
use anyhow::{Context, Result};
use clap::Args;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use shiitake_worker_api::{
    ExecuteFrame, Frame, PtyOpenFrame, ResultFrame, WorkerId, WorkerLocation,
};
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
    /// The session died mid-command (socket closed, or the lease lapsed). The
    /// command has been SIGKILLed; the worker must recycle, not reconnect.
    SessionLost,
}

/// A unit of work pulled off the dispatch socket between resets.
enum Work {
    /// A one-shot command (`bash -c`), output captured to files.
    Execute(ExecuteFrame),
    /// An interactive PTY session — persistent, bidirectional, pinned.
    Pty(PtyOpenFrame),
}

/// One PTY session's outcome on the live session.
enum PtyOutcome {
    /// The shell exited, the server sent PtyClose, or the pty could not be
    /// opened. Carries the `PtyExit` frame to report — sent *after* the reset
    /// (Phase 4), like a command's Result, so the server re-advertises this
    /// worker only once its sandbox (including any account it named) is clean.
    Done(Box<Frame>),
    /// The dispatch socket died mid-session; the shell has been SIGHUP'd and the
    /// worker must recycle, not reconnect.
    SessionLost,
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
    /// The connection closed between commands, or the lease lapsed while idle.
    /// Nothing ran, so the sandbox is still clean: reconnect.
    Closed,
    /// The session died with a command in flight — the socket closed, or the
    /// lease lapsed with the server silent. The command was SIGKILLed, so the
    /// sandbox holds a half-run command's leftovers and no reset has been done.
    /// The process exits for a fresh container rather than serving the next
    /// command on a dirty sandbox, which also fences the worker: it cannot come
    /// back and pick up work while the old command's debris is still around.
    LostMidCommand,
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
    /// Give up on a session after this many seconds with nothing heard from the
    /// server. The server pings every 10s, so silence this long means the
    /// connection is dead in a way TCP hasn't reported — a partition, or a
    /// server pod that vanished without the FIN reaching us. Idle, the worker
    /// reconnects; mid-command it kills the command and recycles, so a command
    /// nobody is waiting for can't outlive the server that ordered it. `0`
    /// disables the lease and waits forever.
    #[arg(long, env = "SHIITAKE_LEASE_TIMEOUT", default_value_t = 45)]
    pub lease_timeout_secs: u64,
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
            Ok(SessionEnd::LostMidCommand) => {
                warn!("session lost mid-command; exiting for a fresh container");
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
        lease_timeout_secs,
        ..
    } = cfg;
    // `0` disables the lease: wait forever, the pre-lease behaviour.
    let lease = match lease_timeout_secs {
        0 => Duration::MAX,
        secs => Duration::from_secs(*secs),
    };
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
        // Idle: a lapsed lease just means this connection is dead. Nothing has
        // run, so the sandbox is clean — drop it and reconnect (which re-resolves
        // the dispatch URL, picking up a replacement server pod).
        let Some(work) = next_execute(&mut sink, &mut stream, lease).await? else {
            return Ok(SessionEnd::Closed);
        };

        // Phase 2: run it, watching the same socket for control frames. A
        // command yields a Result to report in Phase 4; a PTY session sends its
        // own PtyExit as it ends, so there is nothing left to report.
        // A command yields a Result; a PTY session yields a PtyExit. Both are
        // reported in Phase 4, after the reset, so the server only re-advertises
        // this worker once its sandbox is clean.
        let mut pty_exit: Option<Frame> = None;
        let result: Option<ResultFrame> = match work {
            Work::Execute(execute) => {
                info!(request_id = %execute.request_id, "executing");
                match run_command(execute, &mut sink, &mut stream, capture_root, lease).await {
                    CmdOutcome::Done(r) => Some(r),
                    CmdOutcome::SessionLost => return Ok(SessionEnd::LostMidCommand),
                }
            }
            Work::Pty(open) => {
                info!(session_id = %open.session_id, "pty session");
                match run_pty(open, &mut sink, &mut stream, lease).await {
                    PtyOutcome::Done(exit) => {
                        pty_exit = Some(*exit);
                        None
                    }
                    PtyOutcome::SessionLost => return Ok(SessionEnd::LostMidCommand),
                }
            }
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
            let mut reset_task = tokio::task::spawn_blocking(move || reset::reset(&paths));
            // Keep answering keepalive pings while the reset runs. The worker is
            // still in-flight to the pool, and the pool pings in-flight workers,
            // so a reset long enough to outlast the eviction window would
            // otherwise read as a wedged worker and kill a command that in fact
            // finished. A reset is never abandoned part-done, though: if the
            // socket dies we stop reading and wait it out, because a half-reset
            // sandbox is exactly what must not be reconnected on.
            let mut lost_during_reset = false;
            let outcome = loop {
                if lost_during_reset {
                    break (&mut reset_task).await;
                }
                tokio::select! {
                    biased;
                    done = &mut reset_task => break done,
                    msg = stream.next() => match msg {
                        Some(Ok(Message::Ping(p))) => {
                            if let Err(e) = sink.send(Message::Pong(p)).await {
                                warn!("failed to answer a keepalive ping during reset: {e}");
                            }
                        }
                        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                            warn!("connection lost during reset; finishing the reset anyway");
                            lost_during_reset = true;
                        }
                        _ => {}
                    },
                }
            };
            match outcome {
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
        if let Some(result) = result {
            let result_json = serde_json::to_string(&Frame::Result(result))?;
            if let Err(e) = sink.send(Message::Text(result_json.into())).await {
                warn!("send Result failed: {e}; ending session");
                return Ok(SessionEnd::Closed);
            }
        }
        // A PTY session's terminal frame, sent after the reset for the same
        // reason: the server frees the pinned worker only once it is clean.
        if let Some(exit) = pty_exit {
            let exit_json = serde_json::to_string(&exit)?;
            if let Err(e) = sink.send(Message::Text(exit_json.into())).await {
                warn!("send PtyExit failed: {e}; ending session");
                return Ok(SessionEnd::Closed);
            }
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
async fn next_execute(
    sink: &mut Sink,
    stream: &mut Stream,
    lease: Duration,
) -> Result<Option<Work>> {
    loop {
        let Ok(msg) = tokio::time::timeout(lease, stream.next()).await else {
            warn!(
                lease_s = lease.as_secs(),
                "nothing heard from the server within the lease; reconnecting"
            );
            return Ok(None);
        };
        match msg {
            Some(Ok(Message::Text(t))) => match serde_json::from_str::<Frame>(&t)? {
                Frame::Execute(e) => return Ok(Some(Work::Execute(e))),
                Frame::PtyOpen(o) => return Ok(Some(Work::Pty(o))),
                Frame::Cancel { .. } | Frame::PtyResize { .. } | Frame::PtyClose { .. } => {
                    warn!("control frame with nothing in flight; ignoring")
                }
                Frame::Hello { .. } | Frame::Result(_) | Frame::PtyExit { .. } => {
                    warn!("unexpected frame; ignoring")
                }
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
    lease: Duration,
) -> CmdOutcome {
    let request_id = execute.request_id.clone();
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let mut exec_fut = std::pin::pin!(exec::run(&execute, cancel_rx, capture_root));
    let mut session_lost = false;
    // The server pings in-flight workers, so a whole lease of silence means
    // nobody is waiting for this command any more. Reset on every inbound
    // message, so a merely slow command never trips it.
    let mut deadline = std::pin::pin!(tokio::time::sleep(lease));

    let result = loop {
        if session_lost {
            // Cancel already signalled; just let exec wind down so we don't
            // leak the child, then end the session.
            if let Err(e) = (&mut exec_fut).await {
                warn!("exec failed while winding down after session loss: {e:#}");
            }
            return CmdOutcome::SessionLost;
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
            () = &mut deadline => {
                warn!(
                    %request_id,
                    lease_s = lease.as_secs(),
                    "lease lapsed with the server silent; cancelling the command"
                );
                if cancel_tx.send(true).is_err() {
                    debug!("command already finished; cancel had no receiver");
                }
                session_lost = true;
            }
            msg = stream.next() => {
                // Any inbound frame proves the server is still there, so the
                // lease starts over.
                deadline.as_mut().reset(tokio::time::Instant::now() + lease);
                match msg {
                    Some(Ok(Message::Text(t))) => {
                        if let Ok(Frame::Cancel { request_id: rid }) =
                            serde_json::from_str::<Frame>(&t)
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
                        session_lost = true;
                    }
                    _ => {}
                }
            }
        }
    };
    CmdOutcome::Done(result)
}

/// Drive one interactive PTY session: pump the pty master to/from the dispatch
/// socket (output out as WS binary, keystrokes in from WS binary), apply
/// resizes, and end when the shell exits or the server sends PtyClose. Reads the
/// socket concurrently with the pty, like `run_command`, so the session stays
/// responsive and a lease of total silence still tears it down.
async fn run_pty(
    open: PtyOpenFrame,
    sink: &mut Sink,
    stream: &mut Stream,
    lease: Duration,
) -> PtyOutcome {
    let session_id = open.session_id.clone();

    let mut pty = match pty::PtySession::spawn(&open) {
        Ok(p) => p,
        Err(e) => {
            warn!(%session_id, "pty open failed: {e:#}");
            // Reported in Phase 4 after the reset — which also cleans any account
            // `spawn` named before it failed.
            return PtyOutcome::Done(Box::new(Frame::PtyExit {
                session_id,
                exit_code: None,
                exit_signal: None,
                error: Some(format!("{e:#}")),
            }));
        }
    };

    let mut buf = vec![0u8; 32 * 1024];
    let mut deadline = std::pin::pin!(tokio::time::sleep(lease));
    // `shell_gone` = the pty hit EOF (the shell exited); `lost` = the dispatch
    // socket died. Both are cleaned up after the loop with a single &mut borrow,
    // which is why the loop touches `pty` only through its `&self` methods.
    let mut shell_gone = false;
    let mut lost = false;

    loop {
        tokio::select! {
            biased;
            read = pty.read_output(&mut buf) => match read {
                Ok(0) => { shell_gone = true; break; }
                Err(e) => {
                    warn!(%session_id, "pty read error: {e}");
                    shell_gone = true;
                    break;
                }
                Ok(n) => {
                    if sink.send(Message::Binary(buf[..n].to_vec().into())).await.is_err() {
                        lost = true;
                        break;
                    }
                }
            },
            () = &mut deadline => {
                warn!(%session_id, lease_s = lease.as_secs(),
                      "lease lapsed with the server silent; closing pty");
                lost = true;
                break;
            }
            msg = stream.next() => {
                deadline.as_mut().reset(tokio::time::Instant::now() + lease);
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        if let Err(e) = pty.write_input(&data).await {
                            warn!(%session_id, "pty stdin write failed: {e}");
                        }
                    }
                    Some(Ok(Message::Text(t))) => match serde_json::from_str::<Frame>(&t) {
                        Ok(Frame::PtyResize { cols, rows, .. }) => {
                            let _ = pty.resize(cols, rows);
                        }
                        Ok(Frame::PtyClose { .. }) => break,
                        _ => {} // stray Cancel/other frames: nothing in flight to touch
                    },
                    Some(Ok(Message::Ping(p))) => {
                        if sink.send(Message::Pong(p)).await.is_err() {
                            lost = true;
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                        lost = true;
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    // Clean up + collect exit metadata (single &mut borrow, post-loop).
    let (exit_code, exit_signal, error) = if shell_gone {
        match pty.wait().await {
            Ok(status) => {
                #[cfg(unix)]
                {
                    use std::os::unix::process::ExitStatusExt;
                    (status.code(), status.signal(), None)
                }
                #[cfg(not(unix))]
                {
                    (status.code(), None, None)
                }
            }
            Err(e) => (None, None, Some(format!("could not reap shell: {e}"))),
        }
    } else {
        // PtyClose or a lost socket: hang up the shell so it can't linger.
        pty.shutdown().await;
        (None, None, None)
    };

    if lost {
        return PtyOutcome::SessionLost;
    }
    // Reported in Phase 4, after the reset, so the worker is clean before the
    // server frees it for the next session.
    PtyOutcome::Done(Box::new(Frame::PtyExit {
        session_id,
        exit_code,
        exit_signal,
        error,
    }))
}
