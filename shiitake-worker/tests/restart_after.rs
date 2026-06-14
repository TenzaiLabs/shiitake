//! `SHIITAKE_RESTART_AFTER=N` makes the worker exit (for a fresh container)
//! after N commands. This drives a worker with the quota set to 2: it must
//! serve two commands on one connection and then exit 0 on its own — proving
//! the quota both keeps it resident up to N and recycles it at N.

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
async fn worker_exits_after_restart_after_quota() {
    let bin = std::env::var("CARGO_BIN_EXE_shiitake-worker")
        .expect("CARGO_BIN_EXE_shiitake-worker not set");

    let capture_root = TempDir::new().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (result_tx, mut result_rx) = mpsc::channel::<serde_json::Value>(2);
    let app: Router = Router::new().route(
        "/dispatch",
        get(move |upgrade: WebSocketUpgrade| {
            let result_tx = result_tx.clone();
            async move { upgrade.on_upgrade(move |socket| drive(socket, result_tx)) }
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
        .env("SHIITAKE_RESTART_AFTER", "2")
        .env("PATH", std::env::var("PATH").unwrap())
        .spawn()
        .expect("spawn worker binary");

    // Both commands must come back — proves the worker stayed resident for #1
    // and #2 rather than exiting after the first.
    for n in 1..=2 {
        timeout(Duration::from_secs(15), result_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("no Result for command {n}"))
            .expect("channel closed");
    }

    // After the 2nd command it must exit on its own (quota reached) — and exit
    // 0, so the recycle reads as Completed, not a crash.
    let status = match timeout(Duration::from_secs(10), child.wait()).await {
        Ok(status) => status.expect("wait on worker"),
        Err(_) => {
            let _ = child.kill().await;
            panic!("worker did not exit after its restart-after quota of 2");
        }
    };
    assert!(
        status.success(),
        "worker should exit 0 at its quota, got {status:?}"
    );
}

async fn drive(socket: WebSocket, result_tx: mpsc::Sender<serde_json::Value>) {
    let (mut sink, mut stream) = socket.split();
    match stream.next().await {
        Some(Ok(Message::Text(_))) => {}
        other => panic!("expected Hello, got {other:?}"),
    }

    let mut env = BTreeMap::new();
    env.insert(
        "PATH".to_string(),
        std::env::var("PATH").unwrap_or_default(),
    );

    for id in ["req-1", "req-2"] {
        let frame = serde_json::json!({
            "kind": "execute",
            "request_id": id,
            "command": "echo hi",
            "working_dir": std::env::temp_dir(),
            "env": env,
            "timeout_secs": 10.0,
        });
        if sink
            .send(Message::Text(frame.to_string().into()))
            .await
            .is_err()
        {
            return;
        }
        while let Some(msg) = stream.next().await {
            if let Ok(Message::Text(t)) = msg {
                let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                if v.get("kind").and_then(|k| k.as_str()) == Some("result") {
                    result_tx.send(v).await.ok();
                    break;
                }
            }
        }
    }
}
