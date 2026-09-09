//! Worker pool + handle registry.
//!
//! Each in-flight handle holds one worker. Dispatch returns as soon as the
//! Execute frame is sent (or fails with `DispatchError::NoIdleWorker` if
//! the pool is exhausted) — clients poll the handle for completion via
//! the HTTP API. The worker's sink is held in the inflight slot so the
//! server can later send a `Cancel` frame.
//!
//! The idle set is a FIFO queue: dispatch takes from the front and a worker
//! returning from a command goes to the back, so selection is
//! least-recently-used. A serial stream of commands therefore rotates through
//! the whole pool instead of dwelling on one worker — which spreads per-worker
//! resource limits and `SHIITAKE_RESTART_AFTER` recycling evenly, and exercises
//! every worker's sandbox reset rather than letting untried workers sit idle
//! until load finally reaches them.

pub mod k8s_status;

use crate::{metrics::metrics, pool::k8s_status::ClusterProbe};
use anyhow::Result;
use axum::extract::ws::{Message, WebSocket};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use shiitake_worker_api::{
    ExecId, ExecuteFrame, Frame, ResourceUsage, ResultFrame, WorkerId, WorkerLocation, capture,
};
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use thiserror::Error;
use tokio::sync::{Mutex, Notify, mpsc};
use tracing::{Span, info, warn};

pub type WorkerSink = SplitSink<WebSocket, Message>;
pub type WorkerStream = SplitStream<WebSocket>;

/// A worker's write half, shared so the keepalive pinger and dispatch/cancel
/// can send without holding the pool lock across a (possibly slow) send.
pub type SharedSink = Arc<Mutex<WorkerSink>>;

// `HandleStatus` and `ExitCause` are part of the public HTTP API; their
// canonical definitions live in `shiitake-server-api`. The pool's internal
// handle types embed them, so re-export for the rest of the server.
pub use shiitake_server_api::{ExitCause, HandleStatus};

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("no idle worker available")]
    NoIdleWorker,
    #[error("worker send failed: {0}")]
    Send(String),
}

struct PendingEntry {
    sink: SharedSink,
    worker_id: WorkerId,
    started_at: Instant,
}

struct PoolState {
    idle: VecDeque<WorkerEntry>,
    inflight: HashMap<ExecId, PendingEntry>,
    handles: HashMap<ExecId, HandleRow>,
    liveness: HashMap<WorkerId, WorkerLiveness>,
    /// Workers currently held by an interactive PTY session. While a worker is
    /// here, its read loop forwards output/PtyExit to the session instead of
    /// routing Result frames.
    pty_sessions: HashMap<WorkerId, mpsc::Sender<PtyEvent>>,
}

/// Bounded backlog of pty output events per session. A slow client fills this,
/// which blocks the worker's read loop, which stops draining the dispatch WS,
/// which back-pressures the worker into pausing its pty reads — end-to-end flow
/// control, instead of buffering a runaway program's output without bound. Each
/// event holds up to one worker read (32 KiB), so this caps in-flight bytes.
const PTY_OUTPUT_BACKLOG: usize = 64;

/// Last time a worker sent any frame, plus a signal the keepalive pinger
/// fires to tear down an unresponsive worker's read loop.
struct WorkerLiveness {
    last_seen: Instant,
    shutdown: Arc<Notify>,
    /// Where this worker's container runs, as reported on `Hello`. Read on
    /// disconnect to aim the OOM probe; see [`WorkerPool::worker_location`].
    location: Option<WorkerLocation>,
}

struct WorkerEntry {
    worker_id: WorkerId,
    sink: SharedSink,
}

#[derive(Clone)]
struct HandleRow {
    state: Arc<HandleStateInner>,
}

struct HandleStateInner {
    handle_id: ExecId,
    worker_id: WorkerId,
    started_at: SystemTime,
    inner: Mutex<HandleRuntime>,
    /// Notifies waiters every time the runtime transitions. Used by the
    /// /terminal/execute endpoint to block without polling.
    completion: Notify,
}

#[derive(Debug, Clone)]
struct HandleRuntime {
    last_polled_at: SystemTime,
    status: HandleStatus,
    finished_at: Option<SystemTime>,
    exit_code: Option<i32>,
    exit_signal: Option<i32>,
    exit_cause: Option<ExitCause>,
    timed_out: bool,
    cancelled: bool,
    /// The exec span, opened at dispatch and dropped at completion so its
    /// exported duration matches the command's wall-clock lifetime. `None`
    /// once the handle has reached a terminal state.
    span: Option<Span>,
}

/// A snapshot of a handle. Cheap to construct, safe to serialize.
#[derive(Debug, Clone)]
pub struct HandleSnapshot {
    pub handle_id: ExecId,
    pub worker_id: WorkerId,
    pub started_at: SystemTime,
    pub status: HandleStatus,
    pub finished_at: Option<SystemTime>,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
    pub exit_cause: Option<ExitCause>,
    pub timed_out: bool,
    pub cancelled: bool,
}

/// A worker-side output event during a pinned PTY session, forwarded by the
/// per-worker read loop to the `/pty` handler that owns the session.
pub enum PtyEvent {
    /// Raw pty bytes (merged stdout/stderr) to relay to the client.
    Output(Vec<u8>),
    /// The shell exited (or the pty could not open, `error` set). Terminal.
    Exit {
        exit_code: Option<i32>,
        exit_signal: Option<i32>,
        error: Option<String>,
    },
}

/// A worker held out of the pool for one interactive PTY session. The `/pty`
/// handler sends control/stdin through `send` and receives worker output on
/// `recv`; `release_pinned` returns the worker to the pool.
pub struct PinnedSession {
    pub worker_id: WorkerId,
    sink: SharedSink,
    output: mpsc::Receiver<PtyEvent>,
}

impl PinnedSession {
    /// A clonable handle to the worker's sink, for sending frames/stdin without
    /// borrowing the session — so a splice loop can send and `recv` at once.
    pub fn writer(&self) -> SharedSink {
        self.sink.clone()
    }

    /// Await the next output event from the worker; `None` once its read loop
    /// stops forwarding (the worker disconnected).
    pub async fn recv(&mut self) -> Option<PtyEvent> {
        self.output.recv().await
    }
}

#[derive(Clone)]
pub struct WorkerPool {
    state: Arc<Mutex<PoolState>>,
    probe: Option<Arc<ClusterProbe>>,
    pod_name: String,
    namespace: String,
    capture_root: PathBuf,
}

impl WorkerPool {
    pub fn new(
        probe: Option<Arc<ClusterProbe>>,
        pod_name: String,
        namespace: String,
        capture_root: PathBuf,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(PoolState {
                idle: VecDeque::new(),
                inflight: HashMap::new(),
                handles: HashMap::new(),
                liveness: HashMap::new(),
                pty_sessions: HashMap::new(),
            })),
            probe,
            pod_name,
            namespace,
            capture_root,
        }
    }

    pub fn capture_root(&self) -> &std::path::Path {
        &self.capture_root
    }

    /// Drop capture files left by a previous server process.
    ///
    /// Handles live only in memory, so a restart orphans every capture
    /// directory on the volume: no handle can name them, `run_sweeper` walks
    /// the registry and so never reaches them, and they accumulate for the life
    /// of the volume. Call once at startup, before serving. Failures are logged
    /// and skipped — a capture root we can't fully clean is no reason to refuse
    /// to boot.
    pub async fn reconcile_capture(&self) {
        match capture::purge_orphans(&self.capture_root).await {
            Ok((0, failed)) if failed.is_empty() => {}
            Ok((removed, failed)) => {
                info!(removed, "purged orphaned capture directories at startup");
                for (path, e) in failed {
                    warn!(path = %path.display(), "could not purge capture entry: {e}");
                }
            }
            Err(e) => warn!(
                root = %self.capture_root.display(),
                "could not read the capture root to reconcile it: {e}"
            ),
        }
    }

    /// Snapshot for the /health and /ready endpoints: (idle, in-flight).
    pub async fn snapshot(&self) -> (usize, usize) {
        let s = self.state.lock().await;
        (s.idle.len(), s.inflight.len())
    }

    /// Workers currently held by an interactive PTY session.
    pub async fn interactive_count(&self) -> usize {
        self.state.lock().await.pty_sessions.len()
    }

    /// Pop an idle worker and pin it to a new PTY session. `None` when no worker
    /// is idle (the caller should surface a busy signal). Until `release_pinned`,
    /// the worker's read loop forwards its output/PtyExit to the returned session.
    pub async fn acquire_pinned(&self) -> Option<PinnedSession> {
        let mut s = self.state.lock().await;
        let worker = s.idle.pop_front()?;
        let (tx, rx) = mpsc::channel(PTY_OUTPUT_BACKLOG);
        s.pty_sessions.insert(worker.worker_id.clone(), tx);
        Some(PinnedSession {
            worker_id: worker.worker_id,
            sink: worker.sink,
            output: rx,
        })
    }

    /// End a PTY session: stop forwarding output and return the worker to the
    /// idle pool if it is still connected. The worker resets its own sandbox
    /// after the session, exactly as it does after a command.
    pub async fn release_pinned(&self, session: PinnedSession) {
        let mut s = self.state.lock().await;
        s.pty_sessions.remove(&session.worker_id);
        if s.liveness.contains_key(&session.worker_id) {
            s.idle.push_back(WorkerEntry {
                worker_id: session.worker_id,
                sink: session.sink,
            });
        }
    }

    /// Send the Execute frame to the least-recently-used idle worker,
    /// register the handle as Running, and return immediately. The worker's
    /// sink is parked in the inflight slot so a later Cancel can reach it.
    /// The handle's terminal state is filled in when the worker sends a
    /// Result frame (or its connection drops).
    pub async fn dispatch(&self, execute: ExecuteFrame) -> Result<HandleSnapshot, DispatchError> {
        let handle_id = execute.request_id.clone();
        let started_at = SystemTime::now();

        let worker = {
            let mut s = self.state.lock().await;
            match s.idle.pop_front() {
                Some(w) => w,
                None => {
                    metrics().record_pool_rejected();
                    return Err(DispatchError::NoIdleWorker);
                }
            }
        };

        let serialized = serde_json::to_string(&Frame::Execute(execute))
            .map_err(|e| DispatchError::Send(e.to_string()))?;
        if let Err(e) = worker
            .sink
            .lock()
            .await
            .send(Message::Text(serialized.into()))
            .await
        {
            // Couldn't even send Execute; surface the failure and don't
            // create a handle. The worker is gone — don't re-register.
            return Err(DispatchError::Send(format!(
                "worker {}: {e}",
                worker.worker_id
            )));
        }

        let span = tracing::info_span!(
            "shiitake.exec",
            handle = %handle_id,
            worker = %worker.worker_id,
            exit_cause = tracing::field::Empty,
            exit_code = tracing::field::Empty,
            duration_seconds = tracing::field::Empty,
        );
        let runtime = HandleRuntime {
            last_polled_at: started_at,
            status: HandleStatus::Running,
            finished_at: None,
            exit_code: None,
            exit_signal: None,
            exit_cause: None,
            timed_out: false,
            cancelled: false,
            span: Some(span),
        };
        let row = HandleRow {
            state: Arc::new(HandleStateInner {
                handle_id: handle_id.clone(),
                worker_id: worker.worker_id.clone(),
                started_at,
                inner: Mutex::new(runtime),
                completion: Notify::new(),
            }),
        };
        {
            let mut s = self.state.lock().await;
            s.inflight.insert(
                handle_id.clone(),
                PendingEntry {
                    sink: worker.sink,
                    worker_id: worker.worker_id.clone(),
                    started_at: Instant::now(),
                },
            );
            s.handles.insert(handle_id.clone(), row.clone());
        }

        Ok(HandleSnapshot {
            handle_id: row.state.handle_id.clone(),
            worker_id: row.state.worker_id.clone(),
            started_at: row.state.started_at,
            status: HandleStatus::Running,
            finished_at: None,
            exit_code: None,
            exit_signal: None,
            exit_cause: None,
            timed_out: false,
            cancelled: false,
        })
    }

    /// Look up a handle and bump its last_polled_at timestamp. Used by
    /// all GET endpoints so the idle TTL sweeper can tell forgotten
    /// handles from actively-watched ones.
    pub async fn touch_and_snapshot(&self, handle_id: &ExecId) -> Option<HandleSnapshot> {
        let row = {
            let s = self.state.lock().await;
            s.handles.get(handle_id).cloned()
        }?;
        let mut runtime = row.state.inner.lock().await;
        runtime.last_polled_at = SystemTime::now();
        Some(snapshot_from(&row.state, &runtime))
    }

    /// Cancel a handle: signal the worker to SIGKILL the command, then mark the
    /// handle `Cancelled` immediately without waiting for a Result. The resident
    /// worker SIGKILLs the command, resets its sandbox, and sends a
    /// `Result(cancelled)`; we keep the inflight slot so that Result re-idles
    /// the worker (see `complete_request`) rather than stranding it. Idempotent
    /// on terminal handles. Returns None if the handle is unknown.
    pub async fn cancel(&self, handle_id: &ExecId) -> Option<HandleSnapshot> {
        let row = {
            let s = self.state.lock().await;
            s.handles.get(handle_id).cloned()
        }?;
        // Clone the worker's sink (shared) to send Cancel, but leave the
        // inflight slot in place: the worker is still running and will report a
        // Result once it has killed the command and reset.
        let sink = {
            let s = self.state.lock().await;
            s.inflight.get(handle_id).map(|p| p.sink.clone())
        };
        if let Some(sink) = sink {
            let frame = Frame::Cancel {
                request_id: handle_id.clone(),
            };
            if let Ok(serialized) = serde_json::to_string(&frame)
                && let Err(e) = sink
                    .lock()
                    .await
                    .send(Message::Text(serialized.into()))
                    .await
            {
                warn!(%handle_id, error = %e, "cancel send failed");
            }
        }
        let finished_at = SystemTime::now();
        {
            let mut runtime = row.state.inner.lock().await;
            if runtime.status == HandleStatus::Running {
                runtime.finished_at = Some(finished_at);
                runtime.cancelled = true;
                runtime.status = HandleStatus::Error;
                runtime.exit_cause = Some(ExitCause::Cancelled);
                self.finalize_telemetry(
                    &mut runtime,
                    &row.state,
                    finished_at,
                    ExitCause::Cancelled,
                    &ResourceUsage::default(),
                )
                .await;
            }
        }
        row.state.completion.notify_waiters();
        let runtime = row.state.inner.lock().await;
        Some(snapshot_from(&row.state, &runtime))
    }

    /// Remove a handle's capture data and drop it from the registry.
    /// Idempotent.
    pub async fn purge_handle(&self, handle_id: &ExecId) {
        let removed = {
            let mut s = self.state.lock().await;
            s.handles.remove(handle_id).is_some()
        };
        if removed && let Err(e) = capture::purge(&self.capture_root, handle_id).await {
            warn!(%handle_id, error = %e, "capture purge failed");
        }
    }

    /// List all currently-known handle snapshots. Used by the idle TTL
    /// sweeper and (optionally) a future /handles endpoint.
    pub async fn list_snapshots(&self) -> Vec<HandleSnapshot> {
        let rows: Vec<HandleRow> = {
            let s = self.state.lock().await;
            s.handles.values().cloned().collect()
        };
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let runtime = row.state.inner.lock().await;
            out.push(snapshot_from(&row.state, &runtime));
        }
        out
    }

    /// Register a freshly-Hello'd worker as available for dispatch and
    /// drive its read loop. Returns when the WS closes.
    ///
    /// `location` is kept for the connection's lifetime so a disconnect can be
    /// classified against the right pod, even when it isn't the server's.
    pub async fn register_and_run(
        &self,
        worker_id: WorkerId,
        location: Option<WorkerLocation>,
        sink: WorkerSink,
        mut stream: WorkerStream,
    ) -> Result<()> {
        let shutdown = Arc::new(Notify::new());
        {
            let mut s = self.state.lock().await;
            s.idle.push_back(WorkerEntry {
                worker_id: worker_id.clone(),
                sink: Arc::new(Mutex::new(sink)),
            });
            s.liveness.insert(
                worker_id.clone(),
                WorkerLiveness {
                    last_seen: Instant::now(),
                    shutdown: shutdown.clone(),
                    location,
                },
            );
            info!(%worker_id, idle = s.idle.len(), "worker registered");
        }
        self.report_occupancy().await;

        loop {
            tokio::select! {
                msg = stream.next() => {
                    let Some(msg) = msg else { break };
                    self.touch_worker(&worker_id).await;
                    // A worker in a PTY session forwards its output/PtyExit to
                    // that session instead of the Result/handle path.
                    let pty_tx = { self.state.lock().await.pty_sessions.get(&worker_id).cloned() };
                    match msg {
                        Ok(Message::Binary(b)) => {
                            if let Some(tx) = &pty_tx {
                                // Awaits when the session's backlog is full — this
                                // is the back-pressure that stops draining the WS
                                // and pauses the worker's pty reads.
                                let _ = tx.send(PtyEvent::Output(b.to_vec())).await;
                            }
                        }
                        Ok(Message::Text(t)) => match serde_json::from_str::<Frame>(&t) {
                            Ok(Frame::PtyExit {
                                exit_code,
                                exit_signal,
                                error,
                                ..
                            }) => {
                                if let Some(tx) = &pty_tx {
                                    let _ = tx
                                        .send(PtyEvent::Exit {
                                            exit_code,
                                            exit_signal,
                                            error,
                                        })
                                        .await;
                                }
                            }
                            Ok(Frame::Result(r)) => self.complete_request(r).await,
                            Ok(other) => warn!(?other, "unexpected frame from worker"),
                            Err(e) => warn!("frame parse error: {e}"),
                        },
                        Ok(Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }
                _ = shutdown.notified() => {
                    warn!(%worker_id, "evicted by keepalive; closing read loop");
                    break;
                }
            }
        }

        self.handle_worker_drop(&worker_id).await;
        Ok(())
    }

    /// Refresh a worker's last-seen timestamp. Called on every inbound frame
    /// (Result, Pong, …) so the keepalive pinger can tell a live worker from
    /// a wedged one.
    async fn touch_worker(&self, worker_id: &WorkerId) {
        let mut s = self.state.lock().await;
        if let Some(l) = s.liveness.get_mut(worker_id) {
            l.last_seen = Instant::now();
        }
    }

    async fn complete_request(&self, result: ResultFrame) {
        // Re-advertise the worker. A resident worker has, by the time it sends
        // Result, already run the command and reset its sandbox to a clean
        // slate, so it's ready for the next one: recover its sink from the
        // inflight slot and return it to the back of the idle queue. (Done
        // regardless of the handle's state below, so a worker whose handle was
        // already finalized by cancel is still reused rather than stranded.)
        let row = {
            let mut s = self.state.lock().await;
            if let Some(p) = s.inflight.remove(&result.request_id) {
                s.idle.push_back(WorkerEntry {
                    worker_id: p.worker_id,
                    sink: p.sink,
                });
            }
            s.handles.get(&result.request_id).cloned()
        };
        let Some(row) = row else { return };
        if let Some(error) = &result.error {
            warn!(request_id = %result.request_id, "worker could not run the command: {error}");
        }
        let (status, cause) = classify(&result);
        let finished_at = SystemTime::now();
        {
            let mut runtime = row.state.inner.lock().await;
            // Ignore a late Result for a handle already marked terminal (e.g.
            // cancelled), which set its own outcome and won't be overwritten.
            if runtime.status != HandleStatus::Running {
                return;
            }
            runtime.finished_at = Some(finished_at);
            runtime.exit_code = result.exit_code;
            runtime.exit_signal = result.exit_signal;
            runtime.timed_out = result.timed_out;
            runtime.cancelled = result.cancelled;
            runtime.status = status;
            runtime.exit_cause = Some(cause);
            self.finalize_telemetry(&mut runtime, &row.state, finished_at, cause, &result.usage)
                .await;
        }
        row.state.completion.notify_waiters();
        self.report_occupancy().await;
    }

    /// Close the exec span and emit per-command metrics. Called under the
    /// runtime lock once the handle has its terminal fields set.
    async fn finalize_telemetry(
        &self,
        runtime: &mut HandleRuntime,
        state: &HandleStateInner,
        finished_at: SystemTime,
        cause: ExitCause,
        usage: &ResourceUsage,
    ) {
        let duration = finished_at
            .duration_since(state.started_at)
            .unwrap_or_default()
            .as_secs_f64();
        let stdout_bytes = capture::stream_len(
            &self.capture_root,
            &state.handle_id,
            capture::Stream::Stdout,
        )
        .await;
        let stderr_bytes = capture::stream_len(
            &self.capture_root,
            &state.handle_id,
            capture::Stream::Stderr,
        )
        .await;
        let cause_label: &'static str = cause.into();
        if let Some(span) = runtime.span.take() {
            span.record("exit_cause", cause_label);
            if let Some(code) = runtime.exit_code {
                span.record("exit_code", code);
            }
            span.record("duration_seconds", duration);
        }
        metrics().record_exec(cause_label, duration, stdout_bytes, stderr_bytes, usage);
    }

    /// Report current pool occupancy to the metrics pipeline.
    async fn report_occupancy(&self) {
        let (idle, inflight) = self.snapshot().await;
        let count = |n: usize| u64::try_from(n).expect("pool worker count fits u64");
        metrics().set_pool_workers(count(idle), count(inflight));
    }

    /// The `(pod, namespace, container)` the OOM probe should query. A reported
    /// location is authoritative — the only way to find a worker in its own pod.
    /// Without one, assume a container of the server's pod named after the id.
    fn worker_location<'a>(
        &'a self,
        worker_id: &'a WorkerId,
        location: &'a Option<WorkerLocation>,
    ) -> (&'a str, &'a str, &'a str) {
        match location {
            Some(l) => (&l.pod, &l.namespace, &l.container),
            None => (&self.pod_name, &self.namespace, worker_id.as_str()),
        }
    }

    async fn handle_worker_drop(&self, worker_id: &WorkerId) {
        // Find any inflight requests on this worker. The worker may have
        // disconnected mid-command; we use the K8s probe to disambiguate
        // OOM-killed-container from generic disconnect.
        let pending: Vec<(ExecId, PendingEntry)> = {
            let mut s = self.state.lock().await;
            let to_remove: Vec<ExecId> = s
                .inflight
                .iter()
                .filter(|(_, p)| p.worker_id == *worker_id)
                .map(|(rid, _)| rid.clone())
                .collect();
            to_remove
                .into_iter()
                .filter_map(|rid| s.inflight.remove(&rid).map(|p| (rid, p)))
                .collect()
        };
        // Also drop any stale idle entry. A resident worker only reaches this
        // path on a real disconnect (crash/eviction); on reconnect it Hellos
        // and registers afresh. Take the location on the way out — the OOM probe
        // needs it and the liveness entry is about to go.
        let location = {
            let mut s = self.state.lock().await;
            s.idle.retain(|w| w.worker_id != *worker_id);
            // A worker that drops mid-PTY-session leaves its client still
            // attached. Notify the session so the handler closes that client
            // cleanly (with a reason) instead of blocking forever on output that
            // will never arrive. `try_send` (we hold the lock): if the backlog is
            // full the event is dropped, but removing the sender still unblocks
            // the handler — it drains the backlog, then sees the channel closed.
            if let Some(tx) = s.pty_sessions.remove(worker_id) {
                let _ = tx.try_send(PtyEvent::Exit {
                    exit_code: None,
                    exit_signal: None,
                    error: Some("worker disconnected".to_string()),
                });
            }
            s.liveness.remove(worker_id).and_then(|l| l.location)
        };
        self.report_occupancy().await;
        if pending.is_empty() {
            return;
        }

        let oom_killed = match &self.probe {
            Some(probe) => {
                let (pod, namespace, container) = self.worker_location(worker_id, &location);
                probe.was_oom_killed(pod, namespace, container).await
            }
            None => false,
        };
        let (status, cause) = if oom_killed {
            (HandleStatus::Oomkilled, ExitCause::OomContainer)
        } else {
            (HandleStatus::Error, ExitCause::WorkerDied)
        };
        for (rid, p) in pending {
            let row = {
                let s = self.state.lock().await;
                s.handles.get(&rid).cloned()
            };
            if let Some(row) = row {
                let finished_at = SystemTime::now();
                {
                    let mut runtime = row.state.inner.lock().await;
                    runtime.finished_at = Some(finished_at);
                    runtime.status = status;
                    runtime.exit_cause = Some(cause);
                    if cause == ExitCause::OomContainer {
                        runtime.exit_signal = Some(libc::SIGKILL);
                    }
                    self.finalize_telemetry(
                        &mut runtime,
                        &row.state,
                        finished_at,
                        cause,
                        &ResourceUsage::default(),
                    )
                    .await;
                }
                row.state.completion.notify_waiters();
            }
            let waited = p.started_at.elapsed();
            warn!(
                %worker_id, ?cause, waited_ms = %waited.as_millis(),
                "handle fulfilled by worker_drop reconciler"
            );
        }
    }

    /// Block until the handle transitions out of `Running` (or it
    /// disappears). Used by the synchronous /terminal/execute path and
    /// the Python client's `wait()` helper.
    pub async fn wait_for_terminal(&self, handle_id: &ExecId) {
        let row = {
            let s = self.state.lock().await;
            s.handles.get(handle_id).cloned()
        };
        let Some(row) = row else { return };
        loop {
            let notified = row.state.completion.notified();
            tokio::pin!(notified);
            // Subscribe *before* the status check to avoid the race where
            // complete_request fires between our check and our subscribe.
            let runtime = row.state.inner.lock().await;
            if runtime.status != HandleStatus::Running {
                return;
            }
            drop(runtime);
            notified.await;
        }
    }

    /// Run the idle-TTL sweeper forever: every `interval`, kill running
    /// handles whose last_polled_at is older than `running_ttl`, and
    /// purge terminal handles older than `terminal_ttl`.
    pub async fn run_sweeper(
        self: Arc<Self>,
        interval: Duration,
        running_ttl: Duration,
        terminal_ttl: Duration,
    ) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Some(free) = capture::free_bytes(&self.capture_root) {
                metrics().record_capture_free_bytes(free);
            }
            let snapshots = self.list_snapshots().await;
            let now = SystemTime::now();
            for snap in snapshots {
                let last = self.last_polled(&snap.handle_id).await.unwrap_or(now);
                let idle = now.duration_since(last).unwrap_or_default();
                match snap.status {
                    HandleStatus::Running if idle > running_ttl => {
                        warn!(handle_id = %snap.handle_id, idle_s = idle.as_secs(),
                              "idle TTL reached; cancelling");
                        self.cancel(&snap.handle_id).await;
                    }
                    HandleStatus::Completed
                    | HandleStatus::Timeout
                    | HandleStatus::Oomkilled
                    | HandleStatus::Error
                        if idle > terminal_ttl =>
                    {
                        info!(handle_id = %snap.handle_id, idle_s = idle.as_secs(),
                              "terminal TTL reached; purging");
                        self.purge_handle(&snap.handle_id).await;
                    }
                    _ => {}
                }
            }
        }
    }

    /// Ping idle workers every `interval` and evict any silent for longer
    /// than `timeout`. Idle workers answer pings with pongs, so a silent one
    /// is wedged or gone — evicting it tears down its read loop. (Workers
    /// that close cleanly are already caught immediately by that read loop;
    /// this covers the ones that hang without sending a FIN.)
    pub async fn run_keepalive(self: Arc<Self>, interval: Duration, timeout: Duration) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let now = Instant::now();
            // Idle *and* in-flight. A busy worker sends nothing between its
            // Execute and its Result, so without pinging it a command that
            // outlives its server would never be noticed from either end: the
            // pool would hold the handle open, and the worker's own lease
            // (SHIITAKE_LEASE_TIMEOUT) would have no traffic to measure.
            let workers: Vec<(WorkerId, SharedSink, Instant, Arc<Notify>)> = {
                let s = self.state.lock().await;
                let idle = s.idle.iter().map(|w| (&w.worker_id, &w.sink));
                let inflight = s.inflight.values().map(|p| (&p.worker_id, &p.sink));
                idle.chain(inflight)
                    .filter_map(|(worker_id, sink)| {
                        s.liveness.get(worker_id).map(|l| {
                            (
                                worker_id.clone(),
                                sink.clone(),
                                l.last_seen,
                                l.shutdown.clone(),
                            )
                        })
                    })
                    .collect()
            };
            for (worker_id, sink, last_seen, shutdown) in workers {
                let silent = now.duration_since(last_seen);
                if silent > timeout {
                    warn!(%worker_id, silent_s = silent.as_secs(), "worker unresponsive; evicting");
                    shutdown.notify_one();
                    continue;
                }
                let send = async {
                    sink.lock()
                        .await
                        .send(Message::Ping(Vec::new().into()))
                        .await
                };
                match tokio::time::timeout(Duration::from_secs(2), send).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        warn!(%worker_id, error = %e, "ping send failed; evicting");
                        shutdown.notify_one();
                    }
                    Err(_) => {
                        warn!(%worker_id, "ping send timed out; evicting");
                        shutdown.notify_one();
                    }
                }
            }
        }
    }

    async fn last_polled(&self, handle_id: &ExecId) -> Option<SystemTime> {
        let row = {
            let s = self.state.lock().await;
            s.handles.get(handle_id).cloned()
        }?;
        let runtime = row.state.inner.lock().await;
        Some(runtime.last_polled_at)
    }
}

fn snapshot_from(state: &HandleStateInner, runtime: &HandleRuntime) -> HandleSnapshot {
    HandleSnapshot {
        handle_id: state.handle_id.clone(),
        worker_id: state.worker_id.clone(),
        started_at: state.started_at,
        status: runtime.status,
        finished_at: runtime.finished_at,
        exit_code: runtime.exit_code,
        exit_signal: runtime.exit_signal,
        exit_cause: runtime.exit_cause,
        timed_out: runtime.timed_out,
        cancelled: runtime.cancelled,
    }
}

fn classify(r: &ResultFrame) -> (HandleStatus, ExitCause) {
    if r.cancelled {
        return (HandleStatus::Error, ExitCause::Cancelled);
    }
    if r.timed_out {
        return (HandleStatus::Timeout, ExitCause::Timeout);
    }
    if r.exit_signal.is_some() {
        return (HandleStatus::Error, ExitCause::Signal);
    }
    (HandleStatus::Completed, ExitCause::Normal)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool() -> WorkerPool {
        WorkerPool::new(
            None,
            "shiitake-server-abc".into(),
            "shiitake".into(),
            PathBuf::from("/capture"),
        )
    }

    // Split topology: probing the server's pod would find nothing and report
    // every OOM as a plain `worker_died`, so the reported location wins.
    #[test]
    fn reported_location_aims_the_oom_probe_at_the_worker_pod() {
        let pool = pool();
        let id = WorkerId::new("shiitake-workers-xyz");
        let location = Some(WorkerLocation {
            pod: "shiitake-workers-xyz".into(),
            namespace: "shiitake".into(),
            container: "worker".into(),
        });
        assert_eq!(
            pool.worker_location(&id, &location),
            ("shiitake-workers-xyz", "shiitake", "worker")
        );
    }

    // Single-pod (and any worker predating the field): the server's own pod,
    // container named after the worker id.
    #[test]
    fn missing_location_falls_back_to_the_servers_own_pod() {
        let pool = pool();
        let id = WorkerId::new("worker-3");
        assert_eq!(
            pool.worker_location(&id, &None),
            ("shiitake-server-abc", "shiitake", "worker-3")
        );
    }
}
