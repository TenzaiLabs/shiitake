//! What a worker does when it loses the server — the case the split topology
//! introduces, since a server pod dying no longer takes its workers with it.
//!
//! The rule is asymmetric, and the asymmetry is the point. **Idle**, nothing has
//! run, so the sandbox is clean and the worker reconnects (re-resolving the
//! dispatch URL, which is how it finds a replacement server pod). **Mid-command**
//! the command is SIGKILLed and no reset has happened, so the worker exits for a
//! fresh container rather than serving the next command on a dirty sandbox —
//! which also fences it, since it can't come back and take work while the
//! killed command's debris is still around.
//!
//! Both are reached two ways: the socket closing, and the lease lapsing with the
//! socket still open (a partition, where TCP reports nothing).

use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use std::{collections::BTreeMap, process::Stdio, time::Duration};
use tempfile::TempDir;
use tokio::{net::TcpListener, process::Command, sync::mpsc, time::timeout};

/// How the fake dispatcher treats a worker once it has said Hello.
#[derive(Clone, Copy, PartialEq)]
enum Behaviour {
    /// Send an Execute for a long command, then drop the socket.
    ExecuteThenClose,
    /// Send an Execute for a long command, then go silent but stay connected —
    /// a partition, which TCP does not report.
    ExecuteThenGoSilent,
    /// Never send anything; drop the socket.
    CloseWhileIdle,
    /// Never send anything and stay connected. Silence is all the worker gets.
    GoSilentWhileIdle,
}

struct Harness {
    child: tokio::process::Child,
    hellos: mpsc::Receiver<()>,
    _capture: TempDir,
}

impl Harness {
    /// Spawn a fake dispatcher with the given behaviour plus a worker pointed at
    /// it. `lease_secs` is the worker's SHIITAKE_LEASE_TIMEOUT.
    async fn start(behaviour: Behaviour, lease_secs: u64) -> Self {
        let bin = std::env::var("CARGO_BIN_EXE_shiitake-worker")
            .expect("CARGO_BIN_EXE_shiitake-worker not set");
        let capture = TempDir::new().unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // One slot per connection the worker makes, so a reconnect is visible as
        // a second Hello.
        let (hello_tx, hellos) = mpsc::channel::<()>(8);
        let app: Router = Router::new().route(
            "/dispatch",
            get(move |upgrade: WebSocketUpgrade| {
                let hello_tx = hello_tx.clone();
                async move { upgrade.on_upgrade(move |s| drive(s, hello_tx, behaviour)) }
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service())
                .await
                .unwrap();
        });

        let child = Command::new(&bin)
            .env("SHIITAKE_WORKER_ID", "lease-worker")
            .env(
                "SHIITAKE_DISPATCH_URL",
                format!("ws://127.0.0.1:{}/dispatch", addr.port()),
            )
            .env("SHIITAKE_DISPATCH_TOKEN", "test-dispatch-token")
            .env("SHIITAKE_CAPTURE_ROOT", capture.path())
            .env("SHIITAKE_LEASE_TIMEOUT", lease_secs.to_string())
            .env("PATH", std::env::var("PATH").unwrap())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn worker binary");

        Self {
            child,
            hellos,
            _capture: capture,
        }
    }

    /// Wait for the worker process to exit, asserting it exits 0 — a recycle
    /// must never read as a crash (CrashLoopBackOff).
    async fn expect_clean_exit(&mut self, within: Duration) {
        let status = timeout(within, self.child.wait())
            .await
            .expect("worker did not exit; it reconnected instead of recycling")
            .expect("wait failed");
        assert_eq!(
            status.code(),
            Some(0),
            "a recycle must exit 0 so it reads as Completed, not a crash"
        );
    }

    /// Assert the worker came back on a new connection rather than exiting.
    async fn expect_reconnect(&mut self, within: Duration) {
        timeout(within, self.hellos.recv())
            .await
            .expect("worker never reconnected")
            .expect("channel closed");
    }
}

#[tokio::test]
async fn recycles_when_the_socket_closes_mid_command() {
    let mut h = Harness::start(Behaviour::ExecuteThenClose, 60).await;
    h.hellos.recv().await.expect("first Hello");
    // The command was SIGKILLed and the sandbox never reset, so the worker must
    // exit for a fresh container rather than reconnect onto a dirty sandbox.
    h.expect_clean_exit(Duration::from_secs(20)).await;
}

#[tokio::test]
async fn recycles_when_the_lease_lapses_mid_command() {
    // The socket stays open and the server says nothing — TCP reports nothing,
    // so only the lease catches this.
    let mut h = Harness::start(Behaviour::ExecuteThenGoSilent, 2).await;
    h.hellos.recv().await.expect("first Hello");
    h.expect_clean_exit(Duration::from_secs(30)).await;
}

#[tokio::test]
async fn reconnects_when_the_socket_closes_while_idle() {
    let mut h = Harness::start(Behaviour::CloseWhileIdle, 60).await;
    h.hellos.recv().await.expect("first Hello");
    // Nothing ran, so the sandbox is still clean: come back, don't recycle.
    h.expect_reconnect(Duration::from_secs(20)).await;
    h.child.kill().await.ok();
}

#[tokio::test]
async fn reconnects_when_the_lease_lapses_while_idle() {
    let mut h = Harness::start(Behaviour::GoSilentWhileIdle, 2).await;
    h.hellos.recv().await.expect("first Hello");
    h.expect_reconnect(Duration::from_secs(30)).await;
    h.child.kill().await.ok();
}

async fn drive(socket: WebSocket, hello_tx: mpsc::Sender<()>, behaviour: Behaviour) {
    let (mut sink, mut stream) = socket.split();
    match stream.next().await {
        Some(Ok(Message::Text(_))) => {}
        other => panic!("expected Hello, got {other:?}"),
    }
    hello_tx.send(()).await.ok();

    let sends_command = matches!(
        behaviour,
        Behaviour::ExecuteThenClose | Behaviour::ExecuteThenGoSilent
    );
    if sends_command {
        let mut env = BTreeMap::new();
        env.insert(
            "PATH".to_string(),
            std::env::var("PATH").unwrap_or_default(),
        );
        // Long enough that the command is unambiguously still running when the
        // session dies — the timeout must never be what ends this test.
        let frame = serde_json::json!({
            "kind": "execute",
            "request_id": "req-lease",
            "command": "sleep 600",
            "working_dir": std::env::temp_dir(),
            "env": env,
            "timeout_secs": 600.0,
        });
        sink.send(Message::Text(frame.to_string().into()))
            .await
            .unwrap();
    }

    match behaviour {
        Behaviour::ExecuteThenClose | Behaviour::CloseWhileIdle => {
            // Give the worker a moment to actually start the command, so the
            // close lands mid-exec rather than racing the Execute.
            if sends_command {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            drop(sink);
            drop(stream);
        }
        // Hold the socket open and send nothing at all — no frames, and crucially
        // no keepalive pings. Only the lease can notice.
        Behaviour::ExecuteThenGoSilent | Behaviour::GoSilentWhileIdle => {
            tokio::time::sleep(Duration::from_secs(120)).await;
        }
    }
}
