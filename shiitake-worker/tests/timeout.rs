//! Worker enforces the per-command timeout and reports `timed_out=true`,
//! with a "timed out" marker appended to the stderr capture file.

use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    routing::get,
};
use futures_util::StreamExt;
use std::{collections::BTreeMap, process::Command, time::Duration};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::mpsc, time::timeout};

#[tokio::test]
async fn worker_kills_command_on_timeout() {
    let bin = std::env::var("CARGO_BIN_EXE_shiitake-worker")
        .expect("CARGO_BIN_EXE_shiitake-worker not set");

    let capture_root = TempDir::new().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (result_tx, mut result_rx) = mpsc::channel::<serde_json::Value>(1);
    let app: Router = Router::new().route(
        "/dispatch",
        get(move |upgrade: WebSocketUpgrade| {
            let result_tx = result_tx.clone();
            async move { upgrade.on_upgrade(move |socket| handle(socket, result_tx)) }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });

    let mut child = Command::new(&bin)
        .env("SHIITAKE_WORKER_ID", "timeout-worker")
        .env("SHIITAKE_DISPATCH_PORT", addr.port().to_string())
        .env("SHIITAKE_CAPTURE_ROOT", capture_root.path())
        .env("PATH", std::env::var("PATH").unwrap())
        .spawn()
        .expect("spawn worker");

    let result = timeout(Duration::from_secs(10), result_rx.recv())
        .await
        .expect("no result frame")
        .expect("channel closed");

    assert!(child.wait().expect("worker exit").success());
    assert_eq!(result["timed_out"], true);
    assert_eq!(result["cancelled"], false);

    let stderr = capture_root.path().join("req-timeout/stderr");
    let body = std::fs::read_to_string(&stderr).expect("stderr capture should exist");
    assert!(
        body.contains("timed out"),
        "expected timeout marker in stderr capture, got {body:?}"
    );
}

async fn handle(mut socket: WebSocket, result_tx: mpsc::Sender<serde_json::Value>) {
    let _hello = socket.next().await;

    let mut env = BTreeMap::new();
    env.insert(
        "PATH".to_string(),
        std::env::var("PATH").unwrap_or_default(),
    );

    let exec_frame = serde_json::json!({
        "kind": "execute",
        "request_id": "req-timeout",
        "command": "sleep 5",
        "working_dir": std::env::temp_dir(),
        "env": env,
        "timeout_secs": 0.5,
    });

    socket
        .send(Message::Text(exec_frame.to_string().into()))
        .await
        .unwrap();

    while let Some(msg) = socket.next().await {
        if let Ok(Message::Text(t)) = msg {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            if v.get("kind").and_then(|k| k.as_str()) == Some("result") {
                result_tx.send(v).await.ok();
                return;
            }
        }
    }
}
