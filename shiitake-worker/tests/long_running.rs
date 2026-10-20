//! A long-running command completes without a per-command deadline.

use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    routing::get,
};
use futures_util::StreamExt;
use std::{collections::BTreeMap, time::Duration};
use tempfile::TempDir;
use tokio::{net::TcpListener, process::Command, sync::mpsc, time::timeout};

#[tokio::test]
async fn long_running_command_completes_without_deadline() {
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
        .env("SHIITAKE_WORKER_ID", "long-running-worker")
        .env(
            "SHIITAKE_DISPATCH_URL",
            format!("ws://127.0.0.1:{}/dispatch", addr.port()),
        )
        .env("SHIITAKE_DISPATCH_TOKEN", "test-dispatch-token")
        .env("SHIITAKE_CAPTURE_ROOT", capture_root.path())
        .env("PATH", std::env::var("PATH").unwrap())
        .spawn()
        .expect("spawn worker");

    let result = timeout(Duration::from_secs(10), result_rx.recv())
        .await
        .expect("no result frame")
        .expect("channel closed");

    child.kill().await.expect("kill worker");
    let _ = child.wait().await;
    assert_eq!(result["exit_code"], 0);
    assert_eq!(result["cancelled"], false);
}

async fn handle(mut socket: WebSocket, result_tx: mpsc::Sender<serde_json::Value>) {
    let _hello = socket.next().await;
    socket
        .send(Message::Text(
            r#"{"kind":"hello_ack","protocol_version":3}"#.into(),
        ))
        .await
        .unwrap();

    let mut env = BTreeMap::new();
    env.insert(
        "PATH".to_string(),
        std::env::var("PATH").unwrap_or_default(),
    );

    let exec_frame = serde_json::json!({
        "kind": "execute",
        "request_id": "req-long",
        "command": "sleep 1.2",
        "working_dir": std::env::temp_dir(),
        "env": env,
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
