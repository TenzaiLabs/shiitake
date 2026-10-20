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
    ExecId, ExecuteFrame, Frame, OutputChunk, OutputStream, PtyOpenFrame, ResourceUsage,
    ResultFrame, SessionCommandMode, SessionOpenFrame, WorkerId, WorkerLocation, capture,
};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    sync::{mpsc, watch},
    task::JoinSet,
    time::sleep,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::header::{AUTHORIZATION, HeaderValue},
    },
};
use tracing::{debug, info, warn};
use uuid::Uuid;

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
    Session(SessionOpenFrame),
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
    /// Pool this worker advertises to the server.
    #[arg(long, env = "SHIITAKE_PROFILE", default_value = "default")]
    pub profile: String,
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
        profile,
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
        protocol_version: shiitake_worker_api::PROTOCOL_VERSION,
        worker_id: WorkerId::new(worker_id.as_str()),
        profile: profile.clone(),
        location,
    })?;
    sink.send(Message::Text(hello.into()))
        .await
        .context("send Hello")?;

    // An older server ignores Hello's new field and may still dispatch a
    // command. Refuse that command before running it so its output cannot be
    // silently lost; only a matching HelloAck makes this connection usable.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match stream.next().await {
                Some(Ok(Message::Text(t))) => match serde_json::from_str::<Frame>(&t)? {
                    Frame::HelloAck { protocol_version }
                        if protocol_version == shiitake_worker_api::PROTOCOL_VERSION =>
                    {
                        return Ok(());
                    }
                    Frame::Execute(execute) => {
                        let error = ResultFrame::errored(
                            execute.request_id,
                            "incompatible dispatcher protocol; command was not run",
                        );
                        let frame = serde_json::to_string(&Frame::Result(error))?;
                        sink.send(Message::Text(frame.into())).await?;
                        anyhow::bail!("dispatcher did not acknowledge protocol version");
                    }
                    other => anyhow::bail!("unexpected dispatcher handshake frame: {other:?}"),
                },
                Some(Ok(Message::Ping(p))) => sink.send(Message::Pong(p)).await?,
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                    anyhow::bail!("dispatcher closed before protocol acknowledgement")
                }
                _ => {}
            }
        }
    })
    .await
    .context("dispatcher protocol acknowledgement timed out")??;

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
            Work::Session(open) => {
                info!(session_id = %open.session_id, "session opened");
                match run_session(open, &mut sink, &mut stream, capture_root, lease).await {
                    PtyOutcome::Done(closed) => {
                        pty_exit = Some(*closed);
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
            let exit = match exit {
                Frame::SessionClosed {
                    session_id, error, ..
                } => Frame::SessionClosed {
                    session_id,
                    error: if reset_failed {
                        Some("sandbox reset failed".into())
                    } else {
                        error
                    },
                    reusable: !reset_failed && !exiting,
                },
                other => other,
            };
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
                Frame::SessionOpen(o) => return Ok(Some(Work::Session(o))),
                Frame::Cancel { .. } | Frame::PtyResize { .. } | Frame::PtyClose { .. } => {
                    warn!("control frame with nothing in flight; ignoring")
                }
                Frame::Hello { .. }
                | Frame::HelloAck { .. }
                | Frame::Result(_)
                | Frame::PtyExit { .. }
                | Frame::SessionReady { .. }
                | Frame::SessionClosed { .. }
                | Frame::SessionRun { .. }
                | Frame::SessionClose { .. }
                | Frame::SessionResize { .. } => {
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

    let mut result = loop {
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
    // The legacy Execute frame still uses a worker-local capture while the
    // process runs. Send its completed streams over dispatch before Result so
    // the server can spool them without a shared volume.
    for (stream_kind, wire_kind) in [
        (capture::Stream::Stdout, OutputStream::Stdout),
        (capture::Stream::Stderr, OutputStream::Stderr),
    ] {
        let path = capture::stream_path(capture_root, &request_id, stream_kind);
        let mut file = match tokio::fs::File::open(&path).await {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                result.error = Some(format!("could not read command output: {e}"));
                break;
            }
        };
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            let n = match file.read(&mut buf).await {
                Ok(n) => n,
                Err(e) => {
                    result.error = Some(format!("could not read command output: {e}"));
                    break;
                }
            };
            if n == 0 {
                break;
            }
            let chunk = OutputChunk {
                stream: wire_kind,
                id: request_id.as_str(),
                bytes: &buf[..n],
            };
            if !matches!(
                tokio::time::timeout(lease, sink.send(Message::Binary(chunk.encode().into())))
                    .await,
                Ok(Ok(()))
            ) {
                let _ = capture::purge(capture_root, &request_id).await;
                return CmdOutcome::SessionLost;
            }
        }
    }
    if let Err(e) = capture::purge(capture_root, &request_id).await {
        warn!(%request_id, "could not purge legacy command capture: {e}");
    }
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

enum SessionEvent {
    Output(ExecId, OutputStream, Vec<u8>),
    Result(ResultFrame),
    Interrupt,
    Abort {
        request_id: ExecId,
        timed_out: bool,
        cancelled: bool,
    },
}

/// The shell writes this private, per-command marker to its PTY only after the
/// sourced script and state snapshot have finished. A status file alone is not
/// completion evidence: the command can edit files in its capture directory.
struct ShellCompletion {
    marker: Vec<u8>,
    pending: Vec<u8>,
    complete: watch::Sender<bool>,
    seen: bool,
}

impl ShellCompletion {
    fn feed(&mut self, bytes: &[u8]) -> Vec<u8> {
        if self.seen {
            return bytes.to_vec();
        }
        self.pending.extend_from_slice(bytes);
        if let Some(pos) = self
            .pending
            .windows(self.marker.len())
            .position(|window| window == self.marker)
        {
            let mut output = self.pending[..pos].to_vec();
            output.extend_from_slice(&self.pending[pos + self.marker.len()..]);
            self.pending.clear();
            self.seen = true;
            let _ = self.complete.send(true);
            return output;
        }
        let keep = (1..self.marker.len())
            .rev()
            .find(|&n| self.pending.ends_with(&self.marker[..n]))
            .unwrap_or(0);
        let ready = self.pending.len() - keep;
        self.pending.drain(..ready).collect()
    }
}

/// One worker hosts one session, but runs several jobs concurrently. The PTY
/// shell stays alive across jobs and is only hung up when the session closes.
/// The dispatch socket is the sole writer so output and results stay ordered.
async fn run_session(
    open: SessionOpenFrame,
    sink: &mut Sink,
    stream: &mut Stream,
    capture_root: &Path,
    lease: Duration,
) -> PtyOutcome {
    let session_id = open.session_id.clone();
    let shell = PtyOpenFrame {
        session_id: ExecId::new(session_id.as_str()),
        // Stateful commands source scripts into this process; the session
        // shell must be Bash even if legacy PTY's default is overridden.
        command: vec!["bash".into(), "-i".into()],
        working_dir: open.working_dir.clone(),
        env: open.env.clone(),
        cols: open.cols,
        rows: open.rows,
        drop_to: open.drop_to.clone(),
    };
    let mut pty = match pty::PtySession::spawn(&shell) {
        Ok(pty) => pty,
        Err(e) => {
            let error = format!("could not start session shell: {e:#}");
            let _ = send_worker_frame(
                sink,
                &Frame::SessionReady {
                    session_id: session_id.clone(),
                    error: Some(error.clone()),
                },
            )
            .await;
            return PtyOutcome::Done(Box::new(Frame::SessionClosed {
                session_id,
                error: Some(error),
                reusable: true,
            }));
        }
    };
    if send_worker_frame(
        sink,
        &Frame::SessionReady {
            session_id: session_id.clone(),
            error: None,
        },
    )
    .await
    .is_err()
    {
        pty.shutdown().await;
        return PtyOutcome::SessionLost;
    }

    let (event_tx, mut event_rx) = mpsc::channel::<SessionEvent>(64);
    let mut running: HashMap<ExecId, watch::Sender<bool>> = HashMap::new();
    let mut queued: VecDeque<(ExecuteFrame, SessionCommandMode)> = VecDeque::new();
    let mut stateful_running: Option<ExecId> = None;
    let mut shell_completion: Option<ShellCompletion> = None;
    let mut current_workdir = open.working_dir.clone();
    let mut current_env = open.env.clone();
    current_env.insert("TERM".into(), "xterm-256color".into());
    if let Some(drop_to) = open.drop_to.as_ref()
        && let Ok(Some(home)) = crate::account::ensure(drop_to)
    {
        current_env.insert("HOME".into(), home.to_string_lossy().into_owned());
    }
    let mut tasks = JoinSet::new();
    let mut buf = vec![0u8; 32 * 1024];
    let mut deadline = std::pin::pin!(tokio::time::sleep(lease));
    let mut lost = false;
    let mut close_error = None;
    loop {
        while stateful_running.is_none() && running.len() < 32 {
            let Some((mut execute, mode)) = queued.pop_front() else {
                break;
            };
            let rid = execute.request_id.clone();
            execute.working_dir = current_workdir.clone();
            execute.env = current_env.clone();
            execute.drop_to = open.drop_to.clone();
            let timeout = match crate::exec::command_timeout(execute.timeout_secs) {
                Ok(timeout) => timeout,
                Err(e) => {
                    let _ = send_worker_frame(
                        sink,
                        &Frame::Result(ResultFrame::errored(rid, format!("invalid timeout: {e}"))),
                    )
                    .await;
                    continue;
                }
            };
            let (cancel_tx, cancel_rx) = watch::channel(false);
            if mode == SessionCommandMode::Stateful {
                match prepare_stateful_job(&execute, capture_root).await {
                    Ok((line, marker)) => {
                        if let Err(e) = pty.write_input(line.as_bytes()).await {
                            close_error = Some(format!("could not submit shell command: {e}"));
                            break;
                        }
                        let (complete, completed) = watch::channel(false);
                        shell_completion = Some(ShellCompletion {
                            marker,
                            pending: Vec::new(),
                            complete,
                            seen: false,
                        });
                        stateful_running = Some(rid.clone());
                        running.insert(rid, cancel_tx);
                        let tx = event_tx.clone();
                        let root = capture_root.to_path_buf();
                        tasks.spawn(async move {
                            watch_stateful_job(execute, timeout, cancel_rx, completed, root, tx)
                                .await;
                        });
                    }
                    Err(e) => {
                        if let Err(purge_error) = capture::purge(capture_root, &rid).await {
                            warn!(%rid, "could not purge failed stateful setup: {purge_error}");
                        }
                        let _ = send_worker_frame(
                            sink,
                            &Frame::Result(ResultFrame::errored(
                                rid,
                                format!("stateful command setup: {e:#}"),
                            )),
                        )
                        .await;
                    }
                }
                break;
            }
            running.insert(rid, cancel_tx);
            let tx = event_tx.clone();
            let root = capture_root.to_path_buf();
            tasks.spawn(async move {
                run_session_job(execute, cancel_rx, root, tx).await;
            });
        }
        if close_error.is_some() {
            break;
        }
        tokio::select! {
            output = pty.read_output(&mut buf) => match output {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let output = if let Some(completion) = shell_completion.as_mut() {
                        completion.feed(&buf[..n])
                    } else {
                        buf[..n].to_vec()
                    };
                    if output.is_empty() {
                        continue;
                    }
                    let chunk = OutputChunk {
                        stream: OutputStream::Terminal,
                        id: session_id.as_str(),
                        bytes: &output,
                    };
                    if sink.send(Message::Binary(chunk.encode().into())).await.is_err() {
                        lost = true;
                        break;
                    }
                }
            },
            Some(event) = event_rx.recv() => match event {
                SessionEvent::Output(id, which, bytes) => {
                    let chunk = OutputChunk { stream: which, id: id.as_str(), bytes: &bytes };
                    if sink.send(Message::Binary(chunk.encode().into())).await.is_err() {
                        lost = true;
                        break;
                    }
                }
                SessionEvent::Result(mut result) => {
                    if stateful_running.as_ref() == Some(&result.request_id) {
                        match read_shell_state(capture_root, &result.request_id).await {
                            Ok((cwd, env)) => {
                                current_workdir = cwd;
                                current_env = env;
                            }
                            Err(e) => {
                                let error = format!("could not read completed shell state: {e:#}");
                                result.error = Some(error.clone());
                                close_error = Some(error);
                                let _ = send_worker_frame(sink, &Frame::Result(result)).await;
                                break;
                            }
                        }
                        stateful_running = None;
                        shell_completion = None;
                        let _ = capture::purge(capture_root, &result.request_id).await;
                    }
                    running.remove(&result.request_id);
                    if send_worker_frame(sink, &Frame::Result(result)).await.is_err() {
                        lost = true;
                        break;
                    }
                }
                SessionEvent::Interrupt => {
                    let _ = pty.write_input(b"\x03").await;
                }
                SessionEvent::Abort { request_id, timed_out, cancelled } => {
                    let _ = send_worker_frame(sink, &Frame::Result(ResultFrame {
                        request_id,
                        exit_code: None,
                        exit_signal: None,
                        timed_out,
                        cancelled,
                        usage: ResourceUsage::default(),
                        error: Some("stateful command did not stop".into()),
                    })).await;
                    close_error = Some("stateful command did not stop".into());
                    break;
                }
            },
            () = &mut deadline => {
                warn!(%session_id, "server lease lapsed during session");
                lost = true;
                break;
            }
            msg = stream.next() => {
                deadline.as_mut().reset(tokio::time::Instant::now() + lease);
                match msg {
                    Some(Ok(Message::Text(t))) => match serde_json::from_str::<Frame>(&t) {
                        Ok(Frame::SessionRun { session_id: id, execute, mode }) if id == session_id => {
                            let rid = execute.request_id.clone();
                            if running.contains_key(&rid)
                                || queued.iter().any(|(e, _)| e.request_id == rid)
                                || running.len() + queued.len() >= 32 {
                                let _ = send_worker_frame(sink, &Frame::Result(
                                    ResultFrame::errored(rid, "duplicate job or session concurrency limit reached")
                                )).await;
                                continue;
                            }
                            queued.push_back((execute, mode));
                        }
                        Ok(Frame::Cancel { request_id }) => {
                            if let Some(cancel) = running.get(&request_id) {
                                let _ = cancel.send(true);
                            } else if let Some(pos) = queued.iter().position(|(e, _)| e.request_id == request_id) {
                                queued.remove(pos);
                                let mut result = ResultFrame::errored(request_id, "cancelled before start");
                                result.cancelled = true;
                                let _ = send_worker_frame(sink, &Frame::Result(result)).await;
                            }
                        }
                        Ok(Frame::SessionResize { session_id: id, cols, rows }) if id == session_id => {
                            let _ = pty.resize(cols, rows);
                        }
                        Ok(Frame::SessionClose { session_id: id }) if id == session_id => break,
                        _ => {}
                    },
                    Some(Ok(Message::Binary(data))) => {
                        if let Ok(chunk) = OutputChunk::decode(&data)
                            && chunk.stream == OutputStream::Stdin
                            && chunk.id == session_id.as_str()
                        {
                            let _ = pty.write_input(chunk.bytes).await;
                        }
                    }
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

    drop(event_rx);
    for cancel in running.values() {
        let _ = cancel.send(true);
    }
    pty.shutdown().await;
    // A command may need a moment to reap its process group. Reset runs after
    // this method returns and sweeps every remaining process in the namespace.
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    tasks.abort_all();
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    for rid in running.keys() {
        if let Err(e) = capture::purge(capture_root, rid).await {
            warn!(%rid, "could not purge session capture after close: {e}");
        }
    }
    if lost {
        PtyOutcome::SessionLost
    } else {
        PtyOutcome::Done(Box::new(Frame::SessionClosed {
            session_id,
            error: close_error,
            reusable: true,
        }))
    }
}

async fn send_worker_frame(sink: &mut Sink, frame: &Frame) -> Result<()> {
    sink.send(Message::Text(serde_json::to_string(frame)?.into()))
        .await
        .context("send session frame")
}

async fn run_session_job(
    execute: ExecuteFrame,
    cancel: watch::Receiver<bool>,
    root: PathBuf,
    tx: mpsc::Sender<SessionEvent>,
) {
    let rid = execute.request_id.clone();
    let (done_tx, done_rx) = watch::channel(false);
    let out = tokio::spawn(tail_capture(
        root.clone(),
        rid.clone(),
        capture::Stream::Stdout,
        OutputStream::Stdout,
        tx.clone(),
        done_rx.clone(),
    ));
    let err = tokio::spawn(tail_capture(
        root.clone(),
        rid.clone(),
        capture::Stream::Stderr,
        OutputStream::Stderr,
        tx.clone(),
        done_rx,
    ));
    let mut result = match exec::run(&execute, cancel, &root).await {
        Ok(outcome) => outcome.result,
        Err(e) => ResultFrame::errored(rid.clone(), format!("worker error: {e:#}")),
    };
    // Parallel jobs share a container cgroup, so its CPU and memory readings
    // cannot be attributed to one command. Avoid reporting misleading values.
    result.usage = ResourceUsage::default();
    let _ = done_tx.send(true);
    let _ = out.await;
    let _ = err.await;
    let _ = tx.send(SessionEvent::Result(result)).await;
    if let Err(e) = capture::purge(&root, &rid).await {
        warn!(%rid, "could not remove local command capture: {e}");
    }
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}

fn give_to_session_user(path: &Path, execute: &ExecuteFrame) -> Result<()> {
    let Some(drop_to) = &execute.drop_to else {
        return Ok(());
    };
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    if unsafe { libc::chown(path.as_ptr(), drop_to.uid, drop_to.gid) } != 0 {
        return Err(std::io::Error::last_os_error()).context("chown stateful command file");
    }
    Ok(())
}

/// Source a script in the live Bash process. The braces run in the parent
/// shell, so `cd` and `export` persist. Output is redirected to local files,
/// tailed into the dispatch socket, and spooled by the server. The final
/// status file is written only after the cwd and exported environment snapshot.
async fn prepare_stateful_job(execute: &ExecuteFrame, root: &Path) -> Result<(String, Vec<u8>)> {
    let dir = capture::handle_dir(root, &execute.request_id);
    tokio::fs::create_dir_all(&dir).await?;
    give_to_session_user(&dir, execute)?;
    let script = dir.join("command.sh");
    tokio::fs::write(&script, &execute.command).await?;
    give_to_session_user(&script, execute)?;
    let stdout = capture::stream_path(root, &execute.request_id, capture::Stream::Stdout);
    let stderr = capture::stream_path(root, &execute.request_id, capture::Stream::Stderr);
    for path in [&stdout, &stderr] {
        tokio::fs::File::create(path).await?;
        give_to_session_user(path, execute)?;
    }
    let cwd = dir.join("cwd");
    let env = dir.join("env");
    let status = dir.join("status");
    let nonce = Uuid::new_v4();
    let marker = format!("\x1b]shiitake-complete:{nonce}\x07").into_bytes();
    let line = format!(
        "__shiitake_finish() {{ \
         printf '%s' \"$PWD\" > {}; \
         while IFS= read -r __shiitake_name; do \
         printf '%s=%s\\0' \"$__shiitake_name\" \"${{!__shiitake_name}}\"; \
         done < <(compgen -e) > {}; \
         printf '%s' \"$1\" > {}; \
         }}; \
         __shiitake_interrupted=0; \
         trap '__shiitake_interrupted=1; return 130' INT; \
         source {} > {} 2> {}; __shiitake_rc=$?; \
         trap - INT; \
         if [[ \"$__shiitake_interrupted\" == 1 ]]; then __shiitake_rc=130; fi; \
         __shiitake_finish \"$__shiitake_rc\"; \
         printf '\\033]shiitake-complete:{}\\007'\n",
        shell_quote(&cwd),
        shell_quote(&env),
        shell_quote(&status),
        shell_quote(&script),
        shell_quote(&stdout),
        shell_quote(&stderr),
        nonce,
    );
    Ok((line, marker))
}

async fn read_shell_state(root: &Path, rid: &ExecId) -> Result<(String, BTreeMap<String, String>)> {
    let dir = capture::handle_dir(root, rid);
    let cwd = tokio::fs::read_to_string(dir.join("cwd")).await?;
    let bytes = tokio::fs::read(dir.join("env")).await?;
    let mut env = BTreeMap::new();
    for entry in bytes.split(|b| *b == 0).filter(|part| !part.is_empty()) {
        let text = String::from_utf8(entry.to_vec())?;
        if let Some((key, value)) = text.split_once('=') {
            env.insert(key.to_string(), value.to_string());
        }
    }
    Ok((cwd, env))
}

async fn watch_stateful_job(
    execute: ExecuteFrame,
    timeout: Duration,
    cancel: watch::Receiver<bool>,
    completed: watch::Receiver<bool>,
    root: PathBuf,
    tx: mpsc::Sender<SessionEvent>,
) {
    let rid = execute.request_id.clone();
    let (done_tx, done_rx) = watch::channel(false);
    let out = tokio::spawn(tail_capture(
        root.clone(),
        rid.clone(),
        capture::Stream::Stdout,
        OutputStream::Stdout,
        tx.clone(),
        done_rx.clone(),
    ));
    let err = tokio::spawn(tail_capture(
        root.clone(),
        rid.clone(),
        capture::Stream::Stderr,
        OutputStream::Stderr,
        tx.clone(),
        done_rx,
    ));
    let status_path = capture::handle_dir(&root, &rid).join("status");
    let started = tokio::time::Instant::now();
    let mut interrupted_at = None;
    let mut timed_out = false;
    let mut cancelled = false;
    let exit_code = loop {
        if *completed.borrow()
            && let Ok(code) = tokio::fs::read_to_string(&status_path).await
            && let Ok(parsed) = code.trim().parse::<i32>()
        {
            break Some(parsed);
        }
        if interrupted_at.is_none() && (*cancel.borrow() || started.elapsed() >= timeout) {
            cancelled = *cancel.borrow();
            timed_out = !cancelled;
            interrupted_at = Some(tokio::time::Instant::now());
            if tx.send(SessionEvent::Interrupt).await.is_err() {
                abandon_stateful_job(done_tx, out, err, &root, &rid).await;
                return;
            }
        }
        if interrupted_at.is_some_and(|at| at.elapsed() > Duration::from_secs(2)) {
            abandon_stateful_job(done_tx, out, err, &root, &rid).await;
            let _ = tx
                .send(SessionEvent::Abort {
                    request_id: rid,
                    timed_out,
                    cancelled,
                })
                .await;
            return;
        }
        sleep(Duration::from_millis(10)).await;
    };
    let _ = done_tx.send(true);
    let _ = out.await;
    let _ = err.await;
    let _ = tx
        .send(SessionEvent::Result(ResultFrame {
            request_id: rid,
            exit_code,
            exit_signal: None,
            timed_out,
            cancelled,
            usage: ResourceUsage::default(),
            error: None,
        }))
        .await;
}

async fn abandon_stateful_job(
    done: watch::Sender<bool>,
    out: tokio::task::JoinHandle<()>,
    err: tokio::task::JoinHandle<()>,
    root: &Path,
    rid: &ExecId,
) {
    let _ = done.send(true);
    out.abort();
    err.abort();
    let _ = out.await;
    let _ = err.await;
    if let Err(e) = capture::purge(root, rid).await {
        warn!(%rid, "could not purge aborted stateful capture: {e}");
    }
}

async fn tail_capture(
    root: PathBuf,
    rid: ExecId,
    stream: capture::Stream,
    which: OutputStream,
    tx: mpsc::Sender<SessionEvent>,
    done: watch::Receiver<bool>,
) {
    let path = capture::stream_path(&root, &rid, stream);
    let mut file = loop {
        match tokio::fs::File::open(&path).await {
            Ok(f) => break f,
            Err(_) if *done.borrow() || done.has_changed().is_err() => return,
            Err(_) => sleep(Duration::from_millis(10)).await,
        }
    };
    let mut buf = vec![0u8; 32 * 1024];
    loop {
        match file.read(&mut buf).await {
            Ok(0) if *done.borrow() || done.has_changed().is_err() => return,
            Ok(0) => sleep(Duration::from_millis(10)).await,
            Ok(n) => {
                if tx
                    .send(SessionEvent::Output(rid.clone(), which, buf[..n].to_vec()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Err(e) => {
                warn!(%rid, "could not read local output: {e}");
                return;
            }
        }
    }
}
