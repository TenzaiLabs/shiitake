//! Worker honors a Cancel frame sent mid-exec: it SIGKILLs the process
//! group, appends a "cancelled" marker to the stderr capture file, and reports
//! `cancelled = true` in the Result frame.

use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use std::{collections::BTreeMap, process::Command, time::Duration};
use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    sync::mpsc,
    time::{sleep, timeout},
};

#[tokio::test]
async fn worker_honors_cancel_mid_exec() {
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
        .env("SHIITAKE_WORKER_ID", "cancel-worker")
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
    assert_eq!(result["cancelled"], true);
    assert_eq!(result["timed_out"], false);

    let stderr = capture_root.path().join("req-cancel/stderr");
    let body = std::fs::read_to_string(&stderr).expect("stderr capture should exist");
    assert!(
        body.contains("cancelled"),
        "expected cancel marker in stderr capture, got {body:?}"
    );
}

async fn handle(socket: WebSocket, result_tx: mpsc::Sender<serde_json::Value>) {
    let (mut sink, mut stream) = socket.split();
    let _hello = stream.next().await;

    let mut env = BTreeMap::new();
    env.insert(
        "PATH".to_string(),
        std::env::var("PATH").unwrap_or_default(),
    );

    let exec_frame = serde_json::json!({
        "kind": "execute",
        "request_id": "req-cancel",
        "command": "sleep 30",
        "working_dir": std::env::temp_dir(),
        "env": env,
        "timeout_secs": 60.0,
    });

    sink.send(Message::Text(exec_frame.to_string().into()))
        .await
        .unwrap();

    // Give the worker time to start the subprocess, then cancel.
    sleep(Duration::from_millis(300)).await;
    let cancel = serde_json::json!({
        "kind": "cancel",
        "request_id": "req-cancel",
    });
    sink.send(Message::Text(cancel.to_string().into()))
        .await
        .unwrap();

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
