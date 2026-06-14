//! End-to-end test of the resident worker: one worker process serves two
//! commands on a single connection (proving it no longer exits per command),
//! and a file a command writes into a configured `SHIITAKE_RESET_PATHS`
//! directory is gone for the next command — proving the between-command sandbox
//! reset clears the configured scratch paths.

use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use std::{collections::BTreeMap, time::Duration};
use tempfile::TempDir;
use tokio::{net::TcpListener, process::Command, sync::mpsc, time::timeout};

#[tokio::test]
async fn worker_serves_two_commands_and_resets_scratch_between_them() {
    let bin = std::env::var("CARGO_BIN_EXE_shiitake-worker")
        .expect("CARGO_BIN_EXE_shiitake-worker not set");

    let capture_root = TempDir::new().unwrap();
    // The configured scratch dir the worker must empty between commands.
    let scratch = TempDir::new().unwrap();
    let scratch_path = scratch.path().to_string_lossy().into_owned();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (result_tx, mut result_rx) = mpsc::channel::<serde_json::Value>(2);
    let scratch_for_app = scratch_path.clone();
    let app: Router = Router::new().route(
        "/dispatch",
        get(move |upgrade: WebSocketUpgrade| {
            let result_tx = result_tx.clone();
            let scratch = scratch_for_app.clone();
            async move { upgrade.on_upgrade(move |socket| drive(socket, result_tx, scratch)) }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });

    let mut child = Command::new(&bin)
        .env("SHIITAKE_WORKER_ID", "test-worker")
        .env("SHIITAKE_DISPATCH_PORT", addr.port().to_string())
        .env("SHIITAKE_CAPTURE_ROOT", capture_root.path())
        .env("SHIITAKE_RESET_PATHS", &scratch_path)
        .env("PATH", std::env::var("PATH").unwrap())
        .spawn()
        .expect("spawn worker binary");

    // First command's Result.
    let r1 = timeout(Duration::from_secs(15), result_rx.recv())
        .await
        .expect("no Result for command 1")
        .expect("channel closed");
    // Second command's Result — only possible if the worker stayed resident.
    let r2 = timeout(Duration::from_secs(15), result_rx.recv())
        .await
        .expect("no Result for command 2 — worker did not stay resident")
        .expect("channel closed");

    // Resident worker doesn't exit; stop it explicitly and reap it.
    child.kill().await.expect("kill worker");
    let _ = child.wait().await;

    assert_eq!(r1["kind"], "result");
    assert_eq!(r1["request_id"], "req-1");
    assert_eq!(r1["exit_code"], 0);
    let out1 = capture_root.path().join("req-1/stdout");
    assert!(
        std::fs::read_to_string(&out1).unwrap().starts_with("hi"),
        "command 1 stdout should be 'hi'"
    );

    assert_eq!(r2["request_id"], "req-2");
    assert_eq!(r2["exit_code"], 0);
    let out2 = capture_root.path().join("req-2/stdout");
    let body2 = std::fs::read_to_string(&out2).unwrap();
    assert_eq!(
        body2.trim(),
        "ABSENT",
        "the file command 1 wrote into the reset path must be gone for command 2"
    );
}

async fn drive(socket: WebSocket, result_tx: mpsc::Sender<serde_json::Value>, scratch: String) {
    let (mut sink, mut stream) = socket.split();
    // Hello.
    match stream.next().await {
        Some(Ok(Message::Text(_))) => {}
        other => panic!("expected Hello, got {other:?}"),
    }

    let mut env = BTreeMap::new();
    env.insert(
        "PATH".to_string(),
        std::env::var("PATH").unwrap_or_default(),
    );

    // Command 1: drop a marker into the reset path, print "hi".
    send_execute(
        &mut sink,
        "req-1",
        &format!("echo marker > {scratch}/leftover; echo hi"),
        &env,
    )
    .await;
    forward_result(&mut stream, &result_tx).await;

    // Command 2: the reset that ran after command 1 should have emptied the
    // scratch dir, so the marker is gone.
    send_execute(
        &mut sink,
        "req-2",
        &format!("if [ -e {scratch}/leftover ]; then echo PRESENT; else echo ABSENT; fi"),
        &env,
    )
    .await;
    forward_result(&mut stream, &result_tx).await;
}

async fn send_execute(
    sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    request_id: &str,
    command: &str,
    env: &BTreeMap<String, String>,
) {
    let frame = serde_json::json!({
        "kind": "execute",
        "request_id": request_id,
        "command": command,
        "working_dir": std::env::temp_dir(),
        "env": env,
        "timeout_secs": 10.0,
    });
    sink.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
}

async fn forward_result(
    stream: &mut futures_util::stream::SplitStream<WebSocket>,
    result_tx: &mpsc::Sender<serde_json::Value>,
) {
    while let Some(msg) = stream.next().await {
        if let Ok(Message::Text(t)) = msg {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            if v.get("kind").and_then(|k| k.as_str()) == Some("result") {
                result_tx.send(v).await.ok();
                return;
            }
        }
    }
}
