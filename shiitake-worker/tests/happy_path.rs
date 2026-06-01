//! End-to-end happy-path test: spawn the shiitake-worker binary against a
//! tiny stub WebSocket server, send an Execute frame, assert the Result
//! comes back clean and the captured stdout file holds "hi\n".

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
async fn worker_runs_one_command_and_exits() {
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
            async move { upgrade.on_upgrade(move |socket| handle_socket(socket, result_tx)) }
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
        .env("PATH", std::env::var("PATH").unwrap())
        .spawn()
        .expect("spawn worker binary");

    let result_value = timeout(Duration::from_secs(15), result_rx.recv())
        .await
        .expect("server did not receive Result in time")
        .expect("server channel closed");

    let status = child.wait().expect("worker process should exit");
    assert!(
        status.success(),
        "worker exited with non-zero status: {status:?}"
    );

    assert_eq!(result_value["kind"], "result");
    assert_eq!(result_value["exit_code"], 0);
    assert_eq!(result_value["timed_out"], false);
    assert_eq!(result_value["cancelled"], false);

    let stdout = capture_root.path().join("req-1/stdout");
    let body = std::fs::read_to_string(&stdout).expect("stdout capture file should exist");
    assert!(
        body.starts_with("hi"),
        "expected capture to start with 'hi', got {body:?}"
    );
}

async fn handle_socket(mut socket: WebSocket, result_tx: mpsc::Sender<serde_json::Value>) {
    let _hello = match socket.next().await {
        Some(Ok(Message::Text(t))) => t,
        other => panic!("expected text Hello frame, got {other:?}"),
    };

    let mut env = BTreeMap::new();
    env.insert(
        "PATH".to_string(),
        std::env::var("PATH").unwrap_or_default(),
    );
    env.insert(
        "HOME".to_string(),
        std::env::var("HOME").unwrap_or_else(|_| "/tmp".into()),
    );

    let exec_frame = serde_json::json!({
        "kind": "execute",
        "request_id": "req-1",
        "command": "echo hi",
        "working_dir": std::env::temp_dir(),
        "env": env,
        "timeout_secs": 5.0,
    });

    socket
        .send(Message::Text(exec_frame.to_string().into()))
        .await
        .unwrap();

    while let Some(msg) = socket.next().await {
        match msg {
            Ok(Message::Text(t)) => {
                let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                if v.get("kind").and_then(|k| k.as_str()) == Some("result") {
                    result_tx.send(v).await.ok();
                    return;
                }
            }
            Ok(Message::Close(_)) | Err(_) => return,
            _ => {}
        }
    }
}
