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
    ExecId, ExecuteFrame, Frame, OutputChunk, OutputStream, ResourceUsage, ResultFrame,
    SessionCommandMode, SessionId, SessionOpenFrame, WorkerId, WorkerLocation, capture,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    ffi::CString,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::ffi::OsStrExt,
    },
    path::PathBuf,
    sync::{Arc, OnceLock},
    time::{Duration, Instant, SystemTime},
};
use thiserror::Error;
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, Notify, mpsc, oneshot},
};
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
    profile: String,
    session_id: Option<SessionId>,
    mode: SessionCommandMode,
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
    sessions: HashMap<SessionId, SessionRecord>,
    worker_sessions: HashMap<WorkerId, SessionId>,
}

struct SessionRecord {
    worker_id: WorkerId,
    profile: String,
    sink: SharedSink,
    /// Orders SessionRun before SessionClose when submit and release race.
    send_lock: Arc<Mutex<()>>,
    state: shiitake_server_api::SessionState,
    created_at: SystemTime,
    last_activity_at: SystemTime,
    finished_at: Option<SystemTime>,
    opened: Option<oneshot::Sender<Result<(), String>>>,
    closed: Vec<oneshot::Sender<()>>,
    terminal: Option<mpsc::Sender<TerminalEvent>>,
    working_dir: String,
    env: std::collections::BTreeMap<String, String>,
    drop_to: Option<shiitake_worker_api::DropTo>,
    stateful_count: usize,
}

#[derive(Clone)]
pub struct SessionSnapshot {
    pub session_id: SessionId,
    pub worker_id: WorkerId,
    pub profile: String,
    pub state: shiitake_server_api::SessionState,
    pub created_at: SystemTime,
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("unknown profile")]
    UnknownProfile,
    #[error("no idle worker in profile")]
    Busy,
    #[error("session not found")]
    NotFound,
    #[error("session is not active")]
    NotActive,
    #[error("terminal is attached or a stateful command is active")]
    TerminalBusy,
    #[error("worker failed: {0}")]
    Worker(String),
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
    profile: String,
}

struct WorkerEntry {
    worker_id: WorkerId,
    profile: String,
    sink: SharedSink,
}

#[derive(Clone)]
struct HandleRow {
    state: Arc<HandleStateInner>,
}

struct HandleStateInner {
    handle_id: ExecId,
    session_id: Option<SessionId>,
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
    spool_failed: bool,
    /// The exec span, opened at dispatch and dropped at completion so its
    /// exported duration matches the command's wall-clock lifetime. `None`
    /// once the handle has reached a terminal state.
    span: Option<Span>,
}

/// A snapshot of a handle. Cheap to construct, safe to serialize.
#[derive(Debug, Clone)]
pub struct HandleSnapshot {
    pub handle_id: ExecId,
    pub session_id: Option<SessionId>,
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
    profile: String,
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

pub struct TerminalAttachment {
    pub session_id: SessionId,
    pool: Arc<WorkerPool>,
    sink: SharedSink,
    output: mpsc::Receiver<TerminalEvent>,
    reservation: mpsc::Sender<TerminalEvent>,
    detached: bool,
}

pub enum TerminalEvent {
    Output(Vec<u8>),
    Closed(Option<String>),
}

impl TerminalAttachment {
    pub fn writer(&self) -> SharedSink {
        self.sink.clone()
    }
    pub async fn recv(&mut self) -> Option<TerminalEvent> {
        self.output.recv().await
    }
    pub async fn detach(&mut self) {
        self.pool
            .detach_terminal_if(&self.session_id, &self.reservation)
            .await;
        self.detached = true;
    }
    pub async fn send(&self, bytes: &[u8]) -> Result<(), String> {
        let chunk = OutputChunk {
            stream: OutputStream::Stdin,
            id: self.session_id.as_str(),
            bytes,
        };
        self.sink
            .lock()
            .await
            .send(Message::Binary(chunk.encode().into()))
            .await
            .map_err(|e| e.to_string())
    }
    pub async fn resize(&self, cols: u16, rows: u16) -> Result<(), String> {
        let frame = Frame::SessionResize {
            session_id: self.session_id.clone(),
            cols,
            rows,
        };
        send_frame(&self.sink, &frame).await
    }
}

// The WebSocket upgrade callback may be dropped before it starts. Releasing on
// drop also covers an aborted handler; the channel check fences a later attach.
impl Drop for TerminalAttachment {
    fn drop(&mut self) {
        if self.detached {
            return;
        }
        let pool = self.pool.clone();
        let id = self.session_id.clone();
        let reservation = self.reservation.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                pool.detach_terminal_if(&id, &reservation).await;
            });
        }
    }
}

async fn send_frame(sink: &SharedSink, frame: &Frame) -> Result<(), String> {
    let json = serde_json::to_string(frame).map_err(|e| e.to_string())?;
    sink.lock()
        .await
        .send(Message::Text(json.into()))
        .await
        .map_err(|e| e.to_string())
}

fn open_capture_stream(
    root: &std::fs::File,
    id: &ExecId,
    stream: capture::Stream,
) -> std::io::Result<std::fs::File> {
    if !valid_capture_id(id.as_str()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "capture handle has invalid path characters",
        ));
    }
    let component = CString::new(id.as_str())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    // SAFETY: root owns a live directory fd and component is NUL-terminated.
    let dir_fd = unsafe {
        libc::openat(
            root.as_raw_fd(),
            component.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if dir_fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: openat returned a fresh owned descriptor above.
    let dir = unsafe { std::fs::File::from_raw_fd(dir_fd) };
    let name = CString::new(stream.file_name())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    // SAFETY: dir owns a live directory fd and name is NUL-terminated.
    let file_fd = unsafe {
        libc::openat(
            dir.as_raw_fd(),
            name.as_ptr(),
            libc::O_WRONLY | libc::O_APPEND | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if file_fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: openat returned a fresh owned descriptor above.
    Ok(unsafe { std::fs::File::from_raw_fd(file_fd) })
}

fn valid_capture_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn open_capture_root(root: &std::path::Path) -> std::io::Result<std::fs::File> {
    let path = CString::new(root.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    // SAFETY: path is NUL-terminated and open returns a fresh descriptor.
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

#[derive(Clone)]
pub struct WorkerPool {
    state: Arc<Mutex<PoolState>>,
    probe: Option<Arc<ClusterProbe>>,
    pod_name: String,
    namespace: String,
    capture_root: PathBuf,
    capture_dir: Arc<OnceLock<Arc<std::fs::File>>>,
}

impl WorkerPool {
    pub fn new(
        probe: Option<Arc<ClusterProbe>>,
        pod_name: String,
        namespace: String,
        capture_root: PathBuf,
    ) -> Self {
        // Pin the configured spool directory before request processing. Output
        // writes use relative openat calls against this descriptor, so a
        // worker-supplied frame can never turn into an arbitrary path open.
        let capture_dir = Arc::new(OnceLock::new());
        match open_capture_root(&capture_root) {
            Ok(dir) => {
                let _ = capture_dir.set(Arc::new(dir));
            }
            Err(e) => {
                warn!(root = %capture_root.display(), "could not open capture root: {e}");
            }
        }
        Self {
            state: Arc::new(Mutex::new(PoolState {
                idle: VecDeque::new(),
                inflight: HashMap::new(),
                handles: HashMap::new(),
                liveness: HashMap::new(),
                pty_sessions: HashMap::new(),
                sessions: HashMap::new(),
                worker_sessions: HashMap::new(),
            })),
            probe,
            pod_name,
            namespace,
            capture_root,
            capture_dir,
        }
    }

    pub fn capture_root(&self) -> &std::path::Path {
        &self.capture_root
    }

    fn capture_dir(&self) -> Option<Arc<std::fs::File>> {
        if let Some(dir) = self.capture_dir.get() {
            return Some(dir.clone());
        }
        let dir = Arc::new(open_capture_root(&self.capture_root).ok()?);
        let _ = self.capture_dir.set(dir.clone());
        self.capture_dir.get().cloned()
    }

    pub fn capture_len(&self, id: &ExecId, stream: capture::Stream) -> u64 {
        self.capture_dir()
            .and_then(|dir| open_capture_stream(&dir, id, stream).ok())
            .and_then(|file| file.metadata().ok())
            .map_or(0, |metadata| metadata.len())
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
        let mut busy: HashSet<WorkerId> =
            s.inflight.values().map(|p| p.worker_id.clone()).collect();
        busy.extend(s.worker_sessions.keys().cloned());
        busy.extend(s.pty_sessions.keys().cloned());
        (s.idle.len(), busy.len())
    }

    /// Workers currently held by an interactive PTY session.
    pub async fn interactive_count(&self) -> usize {
        self.state.lock().await.pty_sessions.len()
    }

    pub async fn session_count(&self) -> usize {
        self.state.lock().await.worker_sessions.len()
    }

    pub async fn profiles(&self) -> Vec<shiitake_server_api::ProfileResponse> {
        let s = self.state.lock().await;
        let mut counts: HashMap<String, (usize, usize)> = HashMap::new();
        for l in s.liveness.values() {
            counts.entry(l.profile.clone()).or_default();
        }
        for w in &s.idle {
            counts.entry(w.profile.clone()).or_default().0 += 1;
        }
        for session in s.sessions.values() {
            if matches!(
                session.state,
                shiitake_server_api::SessionState::Opening
                    | shiitake_server_api::SessionState::Active
                    | shiitake_server_api::SessionState::Closing
            ) {
                counts.entry(session.profile.clone()).or_default().1 += 1;
            }
        }
        let mut out: Vec<_> = counts
            .into_iter()
            .map(
                |(name, (idle, allocated))| shiitake_server_api::ProfileResponse {
                    name,
                    idle,
                    allocated,
                },
            )
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    pub async fn session_snapshot(&self, id: &SessionId) -> Option<SessionSnapshot> {
        let mut s = self.state.lock().await;
        s.sessions.get_mut(id).map(|r| {
            if r.state == shiitake_server_api::SessionState::Active {
                r.last_activity_at = SystemTime::now();
            }
            SessionSnapshot {
                session_id: id.clone(),
                worker_id: r.worker_id.clone(),
                profile: r.profile.clone(),
                state: r.state,
                created_at: r.created_at,
            }
        })
    }

    pub async fn create_session(
        &self,
        profile: String,
        open: SessionOpenFrame,
    ) -> Result<SessionSnapshot, SessionError> {
        let id = open.session_id.clone();
        let (ready_tx, ready_rx) = oneshot::channel();
        let worker = {
            let mut s = self.state.lock().await;
            if !s.liveness.values().any(|w| w.profile == profile) {
                return Err(SessionError::UnknownProfile);
            }
            let pos = s
                .idle
                .iter()
                .position(|w| w.profile == profile)
                .ok_or(SessionError::Busy)?;
            let worker = s.idle.remove(pos).expect("position came from idle");
            s.worker_sessions
                .insert(worker.worker_id.clone(), id.clone());
            s.sessions.insert(
                id.clone(),
                SessionRecord {
                    worker_id: worker.worker_id.clone(),
                    profile: profile.clone(),
                    sink: worker.sink.clone(),
                    send_lock: Arc::new(Mutex::new(())),
                    state: shiitake_server_api::SessionState::Opening,
                    created_at: SystemTime::now(),
                    last_activity_at: SystemTime::now(),
                    opened: Some(ready_tx),
                    closed: Vec::new(),
                    finished_at: None,
                    terminal: None,
                    working_dir: open.working_dir.clone(),
                    env: open.env.clone(),
                    drop_to: open.drop_to.clone(),
                    stateful_count: 0,
                },
            );
            worker
        };
        if let Err(e) = tokio::time::timeout(
            Duration::from_secs(10),
            send_frame(&worker.sink, &Frame::SessionOpen(open)),
        )
        .await
        .map_err(|_| "session open send timed out".to_string())
        .and_then(|result| result)
        {
            self.handle_worker_drop(&worker.worker_id).await;
            return Err(SessionError::Worker(e));
        }
        match tokio::time::timeout(Duration::from_secs(10), ready_rx).await {
            Ok(Ok(Ok(()))) => self
                .session_snapshot(&id)
                .await
                .ok_or(SessionError::NotFound),
            Ok(Ok(Err(e))) => Err(SessionError::Worker(e)),
            Ok(Err(_)) => Err(SessionError::Worker(
                "worker disconnected during open".into(),
            )),
            Err(_) => {
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    send_frame(&worker.sink, &Frame::SessionClose { session_id: id }),
                )
                .await;
                self.evict_worker(&worker.worker_id).await;
                Err(SessionError::Worker("session open timed out".into()))
            }
        }
    }

    async fn evict_worker(&self, id: &WorkerId) {
        let shutdown = {
            let s = self.state.lock().await;
            s.liveness.get(id).map(|l| l.shutdown.clone())
        };
        if let Some(shutdown) = shutdown {
            shutdown.notify_one();
        }
    }

    pub async fn close_session(&self, id: &SessionId) -> Result<SessionSnapshot, SessionError> {
        self.close_session_inner(id, None).await
    }

    async fn close_session_inner(
        &self,
        id: &SessionId,
        idle_gate: Option<(SystemTime, Duration)>,
    ) -> Result<SessionSnapshot, SessionError> {
        let send_lock = {
            let s = self.state.lock().await;
            s.sessions
                .get(id)
                .ok_or(SessionError::NotFound)?
                .send_lock
                .clone()
        };
        let send_guard = send_lock.lock().await;
        let (sink, worker_id, rx, send_close) = {
            let mut s = self.state.lock().await;
            if let Some((now, ttl)) = idle_gate {
                let rec = s.sessions.get(id).ok_or(SessionError::NotFound)?;
                let still_idle = rec.state == shiitake_server_api::SessionState::Active
                    && rec.terminal.is_none()
                    && !s
                        .inflight
                        .values()
                        .any(|p| p.session_id.as_ref() == Some(id))
                    && now.duration_since(rec.last_activity_at).unwrap_or_default() > ttl;
                if !still_idle {
                    return Ok(SessionSnapshot {
                        session_id: id.clone(),
                        worker_id: rec.worker_id.clone(),
                        profile: rec.profile.clone(),
                        state: rec.state,
                        created_at: rec.created_at,
                    });
                }
            }
            let rec = s.sessions.get_mut(id).ok_or(SessionError::NotFound)?;
            if matches!(
                rec.state,
                shiitake_server_api::SessionState::Closed
                    | shiitake_server_api::SessionState::Failed
            ) {
                return Ok(SessionSnapshot {
                    session_id: id.clone(),
                    worker_id: rec.worker_id.clone(),
                    profile: rec.profile.clone(),
                    state: rec.state,
                    created_at: rec.created_at,
                });
            }
            let (tx, rx) = oneshot::channel();
            let send_close = rec.state != shiitake_server_api::SessionState::Closing;
            rec.state = shiitake_server_api::SessionState::Closing;
            rec.closed.push(tx);
            (rec.sink.clone(), rec.worker_id.clone(), rx, send_close)
        };
        if send_close
            && let Err(e) = tokio::time::timeout(
                Duration::from_secs(10),
                send_frame(
                    &sink,
                    &Frame::SessionClose {
                        session_id: id.clone(),
                    },
                ),
            )
            .await
            .map_err(|_| "session close send timed out".to_string())
            .and_then(|result| result)
        {
            self.evict_worker(&worker_id).await;
            return Err(SessionError::Worker(e));
        }
        drop(send_guard);
        if tokio::time::timeout(Duration::from_secs(15), rx)
            .await
            .is_err()
        {
            self.evict_worker(&worker_id).await;
            return Err(SessionError::Worker(
                "session close timed out; worker evicted".into(),
            ));
        }
        self.session_snapshot(id)
            .await
            .ok_or(SessionError::NotFound)
    }

    pub async fn submit_session_command(
        &self,
        id: &SessionId,
        request_id: ExecId,
        command: String,
        timeout: f64,
        mode: SessionCommandMode,
    ) -> Result<HandleSnapshot, SessionError> {
        if !valid_capture_id(request_id.as_str()) {
            return Err(SessionError::Worker("invalid command handle id".into()));
        }
        let send_lock = {
            let s = self.state.lock().await;
            s.sessions
                .get(id)
                .ok_or(SessionError::NotFound)?
                .send_lock
                .clone()
        };
        let _send_guard = send_lock.lock().await;
        let (worker_id, profile, sink, working_dir, env, drop_to) = {
            let s = self.state.lock().await;
            let rec = s.sessions.get(id).ok_or(SessionError::NotFound)?;
            if rec.state != shiitake_server_api::SessionState::Active {
                return Err(SessionError::NotActive);
            }
            if mode == SessionCommandMode::Stateful && rec.terminal.is_some() {
                return Err(SessionError::TerminalBusy);
            }
            (
                rec.worker_id.clone(),
                rec.profile.clone(),
                rec.sink.clone(),
                rec.working_dir.clone(),
                rec.env.clone(),
                rec.drop_to.clone(),
            )
        };
        let dir = capture::handle_dir(&self.capture_root, &request_id);
        let setup = async {
            tokio::fs::create_dir_all(&dir).await?;
            for stream in [capture::Stream::Stdout, capture::Stream::Stderr] {
                tokio::fs::File::create(capture::stream_path(
                    &self.capture_root,
                    &request_id,
                    stream,
                ))
                .await?;
            }
            Ok::<(), std::io::Error>(())
        }
        .await;
        if let Err(e) = setup {
            let _ = capture::purge(&self.capture_root, &request_id).await;
            return Err(SessionError::Worker(e.to_string()));
        }
        let started_at = SystemTime::now();
        let span = tracing::info_span!("shiitake.exec", handle = %request_id,
            worker = %worker_id, session = %id, profile = %profile,
            exit_cause = tracing::field::Empty, exit_code = tracing::field::Empty,
            duration_seconds = tracing::field::Empty);
        let row = HandleRow {
            state: Arc::new(HandleStateInner {
                handle_id: request_id.clone(),
                session_id: Some(id.clone()),
                worker_id: worker_id.clone(),
                started_at,
                inner: Mutex::new(HandleRuntime {
                    last_polled_at: started_at,
                    status: HandleStatus::Running,
                    finished_at: None,
                    exit_code: None,
                    exit_signal: None,
                    exit_cause: None,
                    timed_out: false,
                    cancelled: false,
                    spool_failed: false,
                    span: Some(span),
                }),
                completion: Notify::new(),
            }),
        };
        let registration = {
            let mut s = self.state.lock().await;
            let error = match s.sessions.get(id) {
                None => Some(SessionError::NotFound),
                Some(rec) if rec.state != shiitake_server_api::SessionState::Active => {
                    Some(SessionError::NotActive)
                }
                Some(rec) if mode == SessionCommandMode::Stateful && rec.terminal.is_some() => {
                    Some(SessionError::TerminalBusy)
                }
                Some(_) => None,
            };
            if let Some(error) = error {
                Err(error)
            } else {
                s.inflight.insert(
                    request_id.clone(),
                    PendingEntry {
                        sink: sink.clone(),
                        worker_id: worker_id.clone(),
                        profile,
                        session_id: Some(id.clone()),
                        mode,
                        started_at: Instant::now(),
                    },
                );
                s.handles.insert(request_id.clone(), row.clone());
                if mode == SessionCommandMode::Stateful
                    && let Some(rec) = s.sessions.get_mut(id)
                {
                    rec.stateful_count += 1;
                }
                if let Some(rec) = s.sessions.get_mut(id) {
                    rec.last_activity_at = SystemTime::now();
                }
                Ok(())
            }
        };
        if let Err(error) = registration {
            let _ = capture::purge(&self.capture_root, &request_id).await;
            return Err(error);
        }
        let execute = ExecuteFrame {
            request_id: request_id.clone(),
            command,
            working_dir,
            env,
            timeout_secs: timeout,
            drop_to,
        };
        if let Err(e) = tokio::time::timeout(
            Duration::from_secs(10),
            send_frame(
                &sink,
                &Frame::SessionRun {
                    session_id: id.clone(),
                    execute,
                    mode,
                },
            ),
        )
        .await
        .map_err(|_| "session command send timed out".to_string())
        .and_then(|result| result)
        {
            self.handle_worker_drop(&worker_id).await;
            return Err(SessionError::Worker(e));
        }
        let runtime = row.state.inner.lock().await;
        Ok(snapshot_from(&row.state, &runtime))
    }

    pub async fn attach_terminal(
        self: &Arc<Self>,
        id: &SessionId,
    ) -> Result<TerminalAttachment, SessionError> {
        let mut s = self.state.lock().await;
        let rec = s.sessions.get_mut(id).ok_or(SessionError::NotFound)?;
        if rec.state != shiitake_server_api::SessionState::Active {
            return Err(SessionError::NotActive);
        }
        if rec.terminal.is_some() {
            return Err(SessionError::TerminalBusy);
        }
        if rec.stateful_count > 0 {
            return Err(SessionError::TerminalBusy);
        }
        let (tx, rx) = mpsc::channel(64);
        rec.terminal = Some(tx.clone());
        rec.last_activity_at = SystemTime::now();
        Ok(TerminalAttachment {
            session_id: id.clone(),
            pool: self.clone(),
            sink: rec.sink.clone(),
            output: rx,
            reservation: tx,
            detached: false,
        })
    }

    async fn detach_terminal_if(&self, id: &SessionId, reservation: &mpsc::Sender<TerminalEvent>) {
        let mut s = self.state.lock().await;
        if let Some(rec) = s.sessions.get_mut(id)
            && rec
                .terminal
                .as_ref()
                .is_some_and(|current| current.same_channel(reservation))
        {
            rec.terminal = None;
            rec.last_activity_at = SystemTime::now();
        }
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
            profile: worker.profile,
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
                profile: session.profile,
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
        if !valid_capture_id(handle_id.as_str()) {
            return Err(DispatchError::Send("invalid command handle id".into()));
        }
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

        let dir = capture::handle_dir(&self.capture_root, &handle_id);
        let setup = async {
            tokio::fs::create_dir_all(&dir).await?;
            for stream in [capture::Stream::Stdout, capture::Stream::Stderr] {
                tokio::fs::File::create(capture::stream_path(
                    &self.capture_root,
                    &handle_id,
                    stream,
                ))
                .await?;
            }
            Ok::<(), std::io::Error>(())
        }
        .await;
        if let Err(e) = setup {
            let _ = capture::purge(&self.capture_root, &handle_id).await;
            self.state.lock().await.idle.push_front(worker);
            return Err(DispatchError::Send(format!("capture setup: {e}")));
        }
        let serialized = match serde_json::to_string(&Frame::Execute(execute)) {
            Ok(serialized) => serialized,
            Err(e) => {
                let _ = capture::purge(&self.capture_root, &handle_id).await;
                self.state.lock().await.idle.push_front(worker);
                return Err(DispatchError::Send(e.to_string()));
            }
        };

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
            spool_failed: false,
            span: Some(span),
        };
        let row = HandleRow {
            state: Arc::new(HandleStateInner {
                handle_id: handle_id.clone(),
                session_id: None,
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
                    sink: worker.sink.clone(),
                    worker_id: worker.worker_id.clone(),
                    profile: worker.profile,
                    session_id: None,
                    mode: SessionCommandMode::Parallel,
                    started_at: Instant::now(),
                },
            );
            s.handles.insert(handle_id.clone(), row.clone());
        }

        if let Err(e) = worker
            .sink
            .lock()
            .await
            .send(Message::Text(serialized.into()))
            .await
        {
            {
                let mut s = self.state.lock().await;
                s.inflight.remove(&handle_id);
                s.handles.remove(&handle_id);
            }
            let _ = capture::purge(&self.capture_root, &handle_id).await;
            self.evict_worker(&worker.worker_id).await;
            return Err(DispatchError::Send(format!(
                "worker {}: {e}",
                worker.worker_id
            )));
        }

        Ok(HandleSnapshot {
            handle_id: row.state.handle_id.clone(),
            session_id: None,
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

    pub async fn touch_session_handle(
        &self,
        session_id: &SessionId,
        handle_id: &ExecId,
    ) -> Option<HandleSnapshot> {
        let row = {
            let s = self.state.lock().await;
            s.handles.get(handle_id).cloned()
        }?;
        if row.state.session_id.as_ref() != Some(session_id) {
            return None;
        }
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
        profile: String,
        location: Option<WorkerLocation>,
        sink: WorkerSink,
        mut stream: WorkerStream,
    ) -> Result<()> {
        let shutdown = Arc::new(Notify::new());
        {
            let mut s = self.state.lock().await;
            if s.liveness.contains_key(&worker_id) {
                return Err(anyhow::anyhow!("duplicate worker id {worker_id}"));
            }
            s.idle.push_back(WorkerEntry {
                worker_id: worker_id.clone(),
                profile: profile.clone(),
                sink: Arc::new(Mutex::new(sink)),
            });
            s.liveness.insert(
                worker_id.clone(),
                WorkerLiveness {
                    last_seen: Instant::now(),
                    shutdown: shutdown.clone(),
                    location,
                    profile,
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
                            } else if let Ok(chunk) = OutputChunk::decode(&b) {
                                self.route_output(&worker_id, chunk).await;
                            }
                        }
                        Ok(Message::Text(t)) => match serde_json::from_str::<Frame>(&t) {
                            Ok(Frame::SessionReady { session_id, error }) => {
                                self.session_ready(&worker_id, &session_id, error).await;
                            }
                            Ok(Frame::SessionClosed { session_id, error, reusable }) => {
                                self.session_closed(&worker_id, &session_id, error, reusable).await;
                            }
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

    async fn route_output(&self, worker_id: &WorkerId, chunk: OutputChunk<'_>) {
        match chunk.stream {
            OutputStream::Stdout | OutputStream::Stderr => {
                // Output IDs on the wire are untrusted. Require a single safe
                // path component and an in-flight handle owned by this worker.
                if !valid_capture_id(chunk.id) {
                    return;
                }
                let id = ExecId::new(chunk.id);
                let authorized = {
                    let s = self.state.lock().await;
                    s.inflight
                        .get(&id)
                        .is_some_and(|p| p.worker_id == *worker_id)
                };
                if !authorized {
                    return;
                }
                let which = if chunk.stream == OutputStream::Stdout {
                    capture::Stream::Stdout
                } else {
                    capture::Stream::Stderr
                };
                let Some(dir) = self.capture_dir() else {
                    warn!(%id, "capture spool is unavailable");
                    self.mark_spool_failed(&id).await;
                    return;
                };
                match open_capture_stream(&dir, &id, which) {
                    Ok(file) => {
                        let mut file = tokio::fs::File::from_std(file);
                        if let Err(e) = file.write_all(chunk.bytes).await {
                            warn!(%id, "could not spool command output: {e}");
                            self.mark_spool_failed(&id).await;
                        }
                    }
                    Err(e) => {
                        warn!(%id, "could not open command spool: {e}");
                        self.mark_spool_failed(&id).await;
                    }
                }
            }
            OutputStream::Terminal => {
                let tx = {
                    let s = self.state.lock().await;
                    s.worker_sessions
                        .get(worker_id)
                        .filter(|id| id.as_str() == chunk.id)
                        .and_then(|id| s.sessions.get(id))
                        .and_then(|rec| rec.terminal.clone())
                };
                if let Some(tx) = tx {
                    let _ = tx.send(TerminalEvent::Output(chunk.bytes.to_vec())).await;
                }
            }
            OutputStream::Stdin => warn!(%worker_id, "unexpected terminal input from worker"),
        }
    }

    async fn mark_spool_failed(&self, id: &ExecId) {
        let row = { self.state.lock().await.handles.get(id).cloned() };
        if let Some(row) = row {
            row.state.inner.lock().await.spool_failed = true;
        }
    }

    async fn session_ready(&self, worker_id: &WorkerId, id: &SessionId, error: Option<String>) {
        let mut s = self.state.lock().await;
        let Some(rec) = s.sessions.get_mut(id) else {
            return;
        };
        if rec.worker_id != *worker_id {
            return;
        }
        if rec.state == shiitake_server_api::SessionState::Opening {
            rec.state = if error.is_some() {
                shiitake_server_api::SessionState::Failed
            } else {
                shiitake_server_api::SessionState::Active
            };
            if error.is_some() {
                rec.finished_at = Some(SystemTime::now());
            }
        }
        if let Some(tx) = rec.opened.take() {
            let _ = tx.send(error.map_or(Ok(()), Err));
        }
    }

    async fn session_closed(
        &self,
        worker_id: &WorkerId,
        id: &SessionId,
        error: Option<String>,
        reusable: bool,
    ) {
        let (pending, closed, terminal, cause) = {
            let mut s = self.state.lock().await;
            let Some(rec) = s.sessions.get_mut(id) else {
                return;
            };
            if rec.worker_id != *worker_id {
                return;
            }
            let cause =
                if rec.state == shiitake_server_api::SessionState::Closing && error.is_none() {
                    ExitCause::Cancelled
                } else {
                    ExitCause::WorkerError
                };
            rec.state = if error.is_some() {
                shiitake_server_api::SessionState::Failed
            } else {
                shiitake_server_api::SessionState::Closed
            };
            rec.finished_at = Some(SystemTime::now());
            let terminal = rec.terminal.take();
            if let Some(tx) = rec.opened.take() {
                let _ = tx.send(Err(error.clone().unwrap_or_else(|| "shell closed".into())));
            }
            let closed = std::mem::take(&mut rec.closed);
            let profile = rec.profile.clone();
            let sink = rec.sink.clone();
            s.worker_sessions.remove(worker_id);
            if reusable && s.liveness.contains_key(worker_id) {
                s.idle.push_back(WorkerEntry {
                    worker_id: worker_id.clone(),
                    profile,
                    sink,
                });
            }
            let ids: Vec<_> = s
                .inflight
                .iter()
                .filter(|(_, p)| p.session_id.as_ref() == Some(id))
                .map(|(rid, _)| rid.clone())
                .collect();
            for rid in &ids {
                s.inflight.remove(rid);
            }
            (ids, closed, terminal, cause)
        };
        if let Some(tx) = terminal {
            tokio::spawn(async move {
                let _ = tx.send(TerminalEvent::Closed(error)).await;
            });
        }
        for rid in pending {
            if let Some(row) = { self.state.lock().await.handles.get(&rid).cloned() } {
                let finished_at = SystemTime::now();
                let mut runtime = row.state.inner.lock().await;
                if runtime.status == HandleStatus::Running {
                    runtime.finished_at = Some(finished_at);
                    runtime.status = HandleStatus::Error;
                    runtime.cancelled = cause == ExitCause::Cancelled;
                    runtime.exit_cause = Some(cause);
                    self.finalize_telemetry(
                        &mut runtime,
                        &row.state,
                        finished_at,
                        cause,
                        &ResourceUsage::default(),
                    )
                    .await;
                    row.state.completion.notify_waiters();
                }
            }
        }
        for tx in closed {
            let _ = tx.send(());
        }
        self.report_occupancy().await;
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
                if let Some(id) = p.session_id {
                    if let Some(rec) = s.sessions.get_mut(&id) {
                        rec.last_activity_at = SystemTime::now();
                        if p.mode == SessionCommandMode::Stateful {
                            rec.stateful_count = rec.stateful_count.saturating_sub(1);
                        }
                    }
                } else {
                    s.idle.push_back(WorkerEntry {
                        worker_id: p.worker_id,
                        profile: p.profile,
                        sink: p.sink,
                    });
                }
            }
            s.handles.get(&result.request_id).cloned()
        };
        let Some(row) = row else { return };
        if let Some(error) = &result.error {
            warn!(request_id = %result.request_id, "worker could not run the command: {error}");
        }
        let finished_at = SystemTime::now();
        {
            let mut runtime = row.state.inner.lock().await;
            // Ignore a late Result for a handle already marked terminal (e.g.
            // cancelled), which set its own outcome and won't be overwritten.
            if runtime.status != HandleStatus::Running {
                return;
            }
            let (status, cause) = if runtime.spool_failed {
                (HandleStatus::Error, ExitCause::OutputError)
            } else {
                classify(&result)
            };
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
        let stdout_bytes = self.capture_len(&state.handle_id, capture::Stream::Stdout);
        let stderr_bytes = self.capture_len(&state.handle_id, capture::Stream::Stderr);
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
        let (location, closed, terminal) = {
            let mut s = self.state.lock().await;
            let mut closed = Vec::new();
            let mut terminal = None;
            s.idle.retain(|w| w.worker_id != *worker_id);
            if let Some(id) = s.worker_sessions.remove(worker_id)
                && let Some(rec) = s.sessions.get_mut(&id)
            {
                rec.state = shiitake_server_api::SessionState::Failed;
                rec.finished_at = Some(SystemTime::now());
                terminal = rec.terminal.take();
                if let Some(tx) = rec.opened.take() {
                    let _ = tx.send(Err("worker disconnected".into()));
                }
                closed = std::mem::take(&mut rec.closed);
            }
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
            (
                s.liveness.remove(worker_id).and_then(|l| l.location),
                closed,
                terminal,
            )
        };
        if let Some(tx) = terminal {
            tokio::spawn(async move {
                let _ = tx
                    .send(TerminalEvent::Closed(Some("worker disconnected".into())))
                    .await;
            });
        }
        self.report_occupancy().await;
        if pending.is_empty() {
            for tx in closed {
                let _ = tx.send(());
            }
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
                    if runtime.status != HandleStatus::Running {
                        continue;
                    }
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
        for tx in closed {
            let _ = tx.send(());
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
                    HandleStatus::Running if snap.session_id.is_none() && idle > running_ttl => {
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
            let expired_sessions: Vec<_> = {
                let s = self.state.lock().await;
                s.sessions
                    .iter()
                    .filter(|(id, rec)| {
                        rec.state == shiitake_server_api::SessionState::Active
                            && rec.terminal.is_none()
                            && !s
                                .inflight
                                .values()
                                .any(|p| p.session_id.as_ref() == Some(id))
                            && now.duration_since(rec.last_activity_at).unwrap_or_default()
                                > running_ttl
                    })
                    .map(|(id, _)| id.clone())
                    .collect()
            };
            for id in expired_sessions {
                warn!(%id, "idle session TTL reached; releasing worker");
                if let Err(e) = self
                    .close_session_inner(&id, Some((now, running_ttl)))
                    .await
                {
                    warn!(%id, "could not close idle session: {e}");
                }
            }
            let mut s = self.state.lock().await;
            s.sessions.retain(|_, rec| {
                rec.finished_at
                    .is_none_or(|at| now.duration_since(at).unwrap_or_default() <= terminal_ttl)
            });
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
                let sessions = s
                    .worker_sessions
                    .iter()
                    .filter_map(|(wid, id)| s.sessions.get(id).map(|r| (wid, &r.sink)));
                let mut seen = HashSet::new();
                idle.chain(inflight)
                    .chain(sessions)
                    .filter(|(worker_id, _)| seen.insert((*worker_id).clone()))
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
        session_id: state.session_id.clone(),
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
    if r.error.is_some() {
        return (HandleStatus::Error, ExitCause::WorkerError);
    }
    (HandleStatus::Completed, ExitCause::Normal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Write, os::unix::fs::symlink};
    use tempfile::TempDir;

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

    #[test]
    fn output_spool_uses_only_real_files_under_the_pinned_root() {
        let root = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let dir = std::fs::File::open(root.path()).unwrap();
        let good = ExecId::new(uuid::Uuid::new_v4().simple().to_string());
        let good_dir = capture::handle_dir(root.path(), &good);
        std::fs::create_dir(&good_dir).unwrap();
        std::fs::write(good_dir.join("stdout"), b"before").unwrap();
        open_capture_stream(&dir, &good, capture::Stream::Stdout)
            .unwrap()
            .write_all(b" after")
            .unwrap();
        assert_eq!(
            std::fs::read(good_dir.join("stdout")).unwrap(),
            b"before after"
        );

        let forged = ExecId::new(uuid::Uuid::new_v4().simple().to_string());
        symlink(outside.path(), capture::handle_dir(root.path(), &forged)).unwrap();
        assert!(open_capture_stream(&dir, &forged, capture::Stream::Stdout).is_err());
        let swapped = ExecId::new(uuid::Uuid::new_v4().simple().to_string());
        let swapped_dir = capture::handle_dir(root.path(), &swapped);
        std::fs::create_dir(&swapped_dir).unwrap();
        let outside_file = outside.path().join("output");
        std::fs::write(&outside_file, b"untouched").unwrap();
        symlink(&outside_file, swapped_dir.join("stdout")).unwrap();
        assert!(open_capture_stream(&dir, &swapped, capture::Stream::Stdout).is_err());
        assert_eq!(std::fs::read(outside_file).unwrap(), b"untouched");
        assert!(
            open_capture_stream(&dir, &ExecId::new("../escape"), capture::Stream::Stdout).is_err()
        );
    }

    #[test]
    fn capture_root_can_appear_after_pool_construction() {
        let parent = TempDir::new().unwrap();
        let root = parent.path().join("late-root");
        let pool = WorkerPool::new(None, "server".into(), "test".into(), root.clone());
        let id = ExecId::new(uuid::Uuid::new_v4().simple().to_string());
        assert_eq!(pool.capture_len(&id, capture::Stream::Stdout), 0);
        let dir = capture::handle_dir(&root, &id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("stdout"), b"ready").unwrap();
        assert_eq!(pool.capture_len(&id, capture::Stream::Stdout), 5);
    }

    #[tokio::test]
    async fn wrong_session_cannot_refresh_a_handle() {
        let pool = pool();
        let handle = ExecId::new("one-handle");
        let owner = SessionId::new("owner");
        let before = SystemTime::UNIX_EPOCH;
        let row = HandleRow {
            state: Arc::new(HandleStateInner {
                handle_id: handle.clone(),
                session_id: Some(owner.clone()),
                worker_id: WorkerId::new("worker"),
                started_at: before,
                inner: Mutex::new(HandleRuntime {
                    last_polled_at: before,
                    status: HandleStatus::Completed,
                    finished_at: Some(before),
                    exit_code: Some(0),
                    exit_signal: None,
                    exit_cause: Some(ExitCause::Normal),
                    timed_out: false,
                    cancelled: false,
                    spool_failed: false,
                    span: None,
                }),
                completion: Notify::new(),
            }),
        };
        pool.state
            .lock()
            .await
            .handles
            .insert(handle.clone(), row.clone());
        assert!(
            pool.touch_session_handle(&SessionId::new("intruder"), &handle)
                .await
                .is_none()
        );
        assert_eq!(row.state.inner.lock().await.last_polled_at, before);
        assert!(pool.touch_session_handle(&owner, &handle).await.is_some());
        assert!(row.state.inner.lock().await.last_polled_at > before);
    }
}
