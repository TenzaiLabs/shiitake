//! Cross-component integration harness for shiitake, as a small test-support
//! library: stand up the real server (pool + dispatch + `/api/v1` listeners, no
//! Kubernetes probe), attach the real worker binary, and speak the client
//! protocols. The `tests/` scenarios consume this crate; it carries no
//! production code and is never published.

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use shiitake_server::{
    http::{AppState, build_api_router, build_dispatch_router},
    pool::WorkerPool,
};
use std::{path::PathBuf, sync::Arc, sync::OnceLock, time::Duration};
use tempfile::TempDir;
use tokio::{net::TcpListener, process::Child, process::Command, time::timeout};
use tokio_tungstenite::tungstenite::{
    Message,
    client::IntoClientRequest,
    http::header::{AUTHORIZATION, HeaderValue},
};

/// Per-run random bearer tokens, so no fixed secret ever lives in the tree —
/// even for a local/integration test. Memoized: the server, worker, and client
/// within one run all read the same value.
fn api_token() -> &'static str {
    static T: OnceLock<String> = OnceLock::new();
    T.get_or_init(|| random_hex(16))
}

fn dispatch_token() -> &'static str {
    static T: OnceLock<String> = OnceLock::new();
    T.get_or_init(|| random_hex(16))
}

/// `n` random bytes from the OS RNG, hex-encoded. Unix-only, which these tests
/// already are (they spawn the worker's `openpty`/`setuid`).
fn random_hex(n: usize) -> String {
    use std::io::Read;
    let mut buf = vec![0u8; n];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut buf))
        .expect("read /dev/urandom");
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

/// Build (once) and locate the real `shiitake-worker` binary. A cross-crate test
/// can't use `CARGO_BIN_EXE_*`, so escargot builds it on demand and caches.
pub fn worker_binary() -> PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        escargot::CargoBuild::new()
            .package("shiitake-worker")
            .bin("shiitake-worker")
            .run()
            .expect("build shiitake-worker")
            .path()
            .to_path_buf()
    })
    .clone()
}

/// A running server plus the pool handle, so a scenario can drive the API and
/// also inspect pool state (idle / pinned counts).
pub struct TestServer {
    pub pool: Arc<WorkerPool>,
    pub api_port: u16,
    pub dispatch_port: u16,
    capture: TempDir,
}

impl TestServer {
    /// Start with the default PTY keepalive (10s).
    pub async fn start() -> Self {
        Self::start_with_keepalive(Duration::from_secs(10)).await
    }

    /// Start with an explicit `/pty` keepalive interval — a short one lets a
    /// scenario prove a quiet session survives past a short worker lease.
    pub async fn start_with_keepalive(pty_keepalive: Duration) -> Self {
        let capture = TempDir::new().unwrap();
        let pool = Arc::new(WorkerPool::new(
            None, // no ClusterProbe -> no kube client, so this runs with no cluster
            "shiitake-test".into(),
            "test".into(),
            capture.path().to_path_buf(),
        ));

        let dispatch_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dispatch_port = dispatch_listener.local_addr().unwrap().port();
        let dispatch_router = build_dispatch_router(pool.clone(), dispatch_token());
        tokio::spawn(async move {
            axum::serve(dispatch_listener, dispatch_router.into_make_service())
                .await
                .unwrap();
        });

        let api_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_port = api_listener.local_addr().unwrap().port();
        let api_router = build_api_router(AppState {
            pool: pool.clone(),
            auth_token: Arc::new(api_token().to_string()),
            default_workdir: std::env::temp_dir(),
            max_body_bytes: 16 * 1024 * 1024,
            min_ready_workers: 1,
            pty_keepalive,
        });
        tokio::spawn(async move {
            axum::serve(api_listener, api_router.into_make_service())
                .await
                .unwrap();
        });

        Self {
            pool,
            api_port,
            dispatch_port,
            capture,
        }
    }

    /// Spawn one real worker against this server and wait until it registers idle.
    pub async fn spawn_worker(&self) -> Child {
        self.spawn_workers(1).await.pop().unwrap()
    }

    /// Spawn `n` real workers (distinct ids) and wait until all `n` are idle.
    /// Scenarios that need concurrent pinned sessions size the pool this way.
    pub async fn spawn_workers(&self, n: usize) -> Vec<Child> {
        self.spawn_workers_with_env(n, &[]).await
    }

    /// As `spawn_workers`, with extra env on each worker — for a scenario that
    /// exercises a worker-level setting (e.g. `SHIITAKE_HOME_ROOT`).
    pub async fn spawn_workers_with_env(&self, n: usize, extra_env: &[(&str, &str)]) -> Vec<Child> {
        let workers: Vec<Child> = (0..n)
            .map(|i| self.spawn_one(&format!("w{}", i + 1), extra_env))
            .collect();
        let ready = timeout(Duration::from_secs(30), async {
            while self.pool.snapshot().await.0 < n {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(
            ready.is_ok(),
            "only {}/{n} workers registered idle",
            self.pool.snapshot().await.0
        );
        workers
    }

    fn spawn_one(&self, id: &str, extra_env: &[(&str, &str)]) -> Child {
        let mut cmd = Command::new(worker_binary());
        cmd.env("SHIITAKE_WORKER_ID", id)
            .env(
                "SHIITAKE_DISPATCH_URL",
                format!("ws://127.0.0.1:{}/dispatch", self.dispatch_port),
            )
            .env("SHIITAKE_DISPATCH_TOKEN", dispatch_token())
            .env("SHIITAKE_CAPTURE_ROOT", self.capture.path())
            .env("PATH", std::env::var("PATH").unwrap())
            .kill_on_drop(true);
        cmd.envs(extra_env.iter().copied());
        cmd.spawn().expect("spawn worker binary")
    }
}

pub type PtyWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Connect to `/api/v1/pty` (bearer-gated) and send the opening control frame.
pub async fn connect_pty(api_port: u16, open: Value) -> PtyWs {
    let mut req = format!("ws://127.0.0.1:{api_port}/api/v1/pty")
        .into_client_request()
        .unwrap();
    req.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::try_from(format!("Bearer {}", api_token())).unwrap(),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("connect /pty");
    ws.send(Message::Text(open.to_string().into()))
        .await
        .unwrap();
    ws
}

/// Open `/api/v1/pty` with `open`, drain the byte stream until the server closes
/// it (the shell exited), and return it as text.
pub async fn open_pty(api_port: u16, open: Value) -> String {
    String::from_utf8_lossy(&open_pty_bytes(api_port, open).await).into_owned()
}

/// As `open_pty`, but returns the raw bytes — for asserting that arbitrary /
/// non-UTF-8 / TUI output survives the transport untouched.
pub async fn open_pty_bytes(api_port: u16, open: Value) -> Vec<u8> {
    drain_raw(connect_pty(api_port, open).await).await
}

/// Drain the raw bytes, but pause once after ~`pause_after` bytes so the server's
/// per-session backlog fills and back-pressures the worker — then keep reading.
/// For proving a large output still arrives in full (backpressure is lossless and
/// the awaited send does not deadlock).
pub async fn open_pty_bytes_with_pause(
    api_port: u16,
    open: Value,
    pause_after: usize,
    pause: Duration,
) -> Vec<u8> {
    let mut ws = connect_pty(api_port, open).await;
    let mut out: Vec<u8> = Vec::new();
    let mut paused = false;
    let read = timeout(Duration::from_secs(30), async {
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Binary(b)) => {
                    out.extend_from_slice(&b);
                    if !paused && out.len() >= pause_after {
                        tokio::time::sleep(pause).await;
                        paused = true;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
    assert!(read.is_ok(), "pty stream did not end within the deadline");
    out
}

/// Open a session, wait until `ready` appears in the output, then send `keys`
/// as one stdin (binary) frame, and return everything received until the shell
/// exits. Used to inject a control byte (e.g. Ctrl-C) once the program is armed.
pub async fn pty_send_after_ready(api_port: u16, open: Value, ready: &str, keys: &[u8]) -> String {
    let mut ws = connect_pty(api_port, open).await;
    let mut out: Vec<u8> = Vec::new();
    let mut sent = false;
    let read = timeout(Duration::from_secs(20), async {
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Binary(b)) => {
                    out.extend_from_slice(&b);
                    if !sent && contains(&out, ready.as_bytes()) {
                        ws.send(Message::Binary(keys.to_vec().into()))
                            .await
                            .unwrap();
                        sent = true;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
    assert!(read.is_ok(), "pty stream did not end within the deadline");
    assert!(
        sent,
        "ready marker {ready:?} never appeared; got: {:?}",
        String::from_utf8_lossy(&out)
    );
    String::from_utf8_lossy(&out).into_owned()
}

async fn drain_raw(mut ws: PtyWs) -> Vec<u8> {
    let mut out = Vec::new();
    let read = timeout(Duration::from_secs(20), async {
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Binary(b)) => out.extend_from_slice(&b),
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
    assert!(read.is_ok(), "pty stream did not end within the deadline");
    out
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Drain output on `ws` until `marker` appears (panics on timeout / early end).
pub async fn recv_until(ws: &mut PtyWs, marker: &str) {
    let mut out: Vec<u8> = Vec::new();
    let seen = timeout(Duration::from_secs(10), async {
        while let Some(msg) = ws.next().await {
            if let Ok(Message::Binary(b)) = msg {
                out.extend_from_slice(&b);
                if contains(&out, marker.as_bytes()) {
                    return true;
                }
            }
        }
        false
    })
    .await;
    assert!(
        matches!(seen, Ok(true)),
        "marker {marker:?} never appeared; got: {:?}",
        String::from_utf8_lossy(&out)
    );
}

/// Send a `{op:"resize"}` control frame (window resize).
pub async fn resize(ws: &mut PtyWs, cols: u16, rows: u16) {
    let frame = json!({ "op": "resize", "cols": cols, "rows": rows }).to_string();
    ws.send(Message::Text(frame.into())).await.unwrap();
}

/// Read `ws` until the server closes it; return the (code, reason) of the close
/// frame, or `None` if the socket ended without one.
pub async fn recv_close(ws: &mut PtyWs) -> Option<(u16, String)> {
    let res = timeout(Duration::from_secs(10), async {
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Close(frame)) => {
                    return frame.map(|f| (u16::from(f.code), f.reason.to_string()));
                }
                Ok(_) => {}
                Err(_) => return None,
            }
        }
        None
    })
    .await;
    res.ok().flatten()
}

/// A `/pty` open frame running `command`, default size, cwd = the temp dir.
pub fn open_frame(command: &[&str]) -> Value {
    json!({
        "op": "open",
        "command": command,
        "working_dir": std::env::temp_dir(),
        "env": { "PATH": std::env::var("PATH").unwrap_or_default() },
        "cols": 80,
        "rows": 24,
    })
}

/// Whether a scenario that actually creates a unix account may run: only when it
/// is root on Linux AND explicitly opted in (`SHIITAKE_PTY_USERADD_TEST=1`). The
/// opt-in must be set deliberately — only inside a disposable container — so a
/// host `cargo test`, even as root, never mutates the real `/etc/passwd`.
pub fn may_create_accounts() -> bool {
    #[cfg(target_os = "linux")]
    let root = nix::unistd::Uid::effective().is_root();
    #[cfg(not(target_os = "linux"))]
    let root = false;
    root && std::env::var("SHIITAKE_PTY_USERADD_TEST").as_deref() == Ok("1")
}
