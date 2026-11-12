//! A planned teardown must read as a clean stop, not a crash or a reconnect:
//! drive a worker to a completed handshake, send SIGTERM, and assert it exits 0
//! without opening a second dispatch connection.

#![cfg(unix)]

use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tempfile::TempDir;
use tokio::{net::TcpListener, process::Command, sync::mpsc, time::timeout};

#[tokio::test]
async fn worker_exits_zero_on_sigterm_without_reconnecting() {
    exits_cleanly_on(Signal::SIGTERM).await;
}

// SIGTERM and SIGINT take separate arms in the worker's signal handler, so a
// broken interrupt arm would pass the SIGTERM case above; cover it too.
#[tokio::test]
async fn worker_exits_zero_on_sigint_without_reconnecting() {
    exits_cleanly_on(Signal::SIGINT).await;
}

async fn exits_cleanly_on(signal: Signal) {
    let bin = std::env::var("CARGO_BIN_EXE_shiitake-worker")
        .expect("CARGO_BIN_EXE_shiitake-worker not set");
    let capture_root = TempDir::new().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Count completed handshakes; a planned SIGTERM must not produce a second.
    let handshakes = Arc::new(AtomicUsize::new(0));
    let (hello_tx, mut hello_rx) = mpsc::channel::<()>(4);
    let app: Router = Router::new().route(
        "/dispatch",
        get({
            let handshakes = handshakes.clone();
            move |upgrade: WebSocketUpgrade| {
                let handshakes = handshakes.clone();
                let hello_tx = hello_tx.clone();
                async move { upgrade.on_upgrade(move |socket| drive(socket, handshakes, hello_tx)) }
            }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });

    // Lease 0 keeps the worker on this one idle connection forever, so the only
    // thing that ends it is the signal under test — not a lapsed lease.
    let mut child = Command::new(&bin)
        .env("SHIITAKE_WORKER_ID", "test-worker")
        .env(
            "SHIITAKE_DISPATCH_URL",
            format!("ws://127.0.0.1:{}/dispatch", addr.port()),
        )
        .env("SHIITAKE_DISPATCH_TOKEN", "test-dispatch-token")
        .env("SHIITAKE_CAPTURE_ROOT", capture_root.path())
        .env("SHIITAKE_LEASE_TIMEOUT", "0")
        .env("PATH", std::env::var("PATH").unwrap())
        .spawn()
        .expect("spawn worker binary");

    // Wait until the worker has connected and completed the handshake.
    timeout(Duration::from_secs(15), hello_rx.recv())
        .await
        .expect("worker never completed the dispatch handshake")
        .expect("handshake channel closed");

    // Planned teardown.
    let pid = Pid::from_raw(child.id().expect("worker has a pid") as i32);
    kill(pid, signal).expect("send signal to worker");

    // It must exit, and exit 0, so the teardown reads as Completed, not a crash.
    let status = match timeout(Duration::from_secs(10), child.wait()).await {
        Ok(status) => status.expect("wait on worker"),
        Err(_) => {
            let _ = child.kill().await;
            panic!("worker did not exit after {signal:?}");
        }
    };
    assert!(
        status.success(),
        "worker should exit 0 on {signal:?}, got {status:?}"
    );

    // And it must not have reconnected: exactly one handshake over its lifetime.
    assert_eq!(
        handshakes.load(Ordering::SeqCst),
        1,
        "worker opened a second dispatch connection instead of exiting on {signal:?}"
    );
}

/// Accept one worker: read its Hello, acknowledge, then hold the connection
/// open and idle so the worker sits waiting until the signal arrives.
async fn drive(socket: WebSocket, handshakes: Arc<AtomicUsize>, hello_tx: mpsc::Sender<()>) {
    let (mut sink, mut stream) = socket.split();
    match stream.next().await {
        Some(Ok(Message::Text(_))) => {}
        other => panic!("expected Hello, got {other:?}"),
    }
    sink.send(Message::Text(
        r#"{"kind":"hello_ack","protocol_version":3}"#.into(),
    ))
    .await
    .unwrap();
    handshakes.fetch_add(1, Ordering::SeqCst);
    hello_tx.send(()).await.ok();

    // Stay connected and idle until the worker drops the socket (on exit).
    while let Some(msg) = stream.next().await {
        if matches!(msg, Ok(Message::Close(_)) | Err(_)) {
            break;
        }
    }
}
