//! The worker reaches the dispatcher by URL, authenticates, and says where it
//! runs — the three things that let it live in a different pod from the server.

use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
};
use futures_util::StreamExt;
use std::time::Duration;
use tempfile::TempDir;
use tokio::{net::TcpListener, process::Command, sync::mpsc, time::timeout};

const TOKEN: &str = "dispatch-secret";

#[tokio::test]
async fn worker_dials_the_configured_url_with_a_bearer_token_and_reports_its_location() {
    let bin = std::env::var("CARGO_BIN_EXE_shiitake-worker")
        .expect("CARGO_BIN_EXE_shiitake-worker not set");
    let capture_root = TempDir::new().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (hello_tx, mut hello_rx) = mpsc::channel::<serde_json::Value>(1);
    // Deliberately NOT `/dispatch`: a worker that hardcoded the path or host
    // instead of dialling SHIITAKE_DISPATCH_URL would never arrive here.
    let app: Router = Router::new().route(
        "/some/other/path",
        get(move |headers: HeaderMap, upgrade: WebSocketUpgrade| {
            let hello_tx = hello_tx.clone();
            async move {
                // Reject as the real dispatch router does, so a worker that
                // forgot the token fails the handshake.
                if headers.get("authorization").and_then(|v| v.to_str().ok())
                    != Some(format!("Bearer {TOKEN}").as_str())
                {
                    return StatusCode::UNAUTHORIZED.into_response();
                }
                upgrade.on_upgrade(move |socket| capture_hello(socket, hello_tx))
            }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });

    let mut child = Command::new(&bin)
        .env("SHIITAKE_WORKER_ID", "shiitake-workers-xyz")
        .env(
            "SHIITAKE_DISPATCH_URL",
            format!("ws://127.0.0.1:{}/some/other/path", addr.port()),
        )
        .env("SHIITAKE_DISPATCH_TOKEN", TOKEN)
        .env("SHIITAKE_CAPTURE_ROOT", capture_root.path())
        .env("POD_NAME", "shiitake-workers-xyz")
        .env("POD_NAMESPACE", "shiitake")
        .env("SHIITAKE_CONTAINER_NAME", "worker")
        .env("PATH", std::env::var("PATH").unwrap())
        .spawn()
        .expect("spawn worker binary");

    let hello = timeout(Duration::from_secs(15), hello_rx.recv())
        .await
        .expect("worker never completed an authenticated handshake")
        .expect("channel closed");

    child.kill().await.expect("kill worker");
    let _ = child.wait().await;

    assert_eq!(hello["kind"], "hello");
    assert_eq!(hello["worker_id"], "shiitake-workers-xyz");
    // What the OOM probe will query: the worker's pod, not the server's.
    assert_eq!(hello["location"]["pod"], "shiitake-workers-xyz");
    assert_eq!(hello["location"]["namespace"], "shiitake");
    assert_eq!(hello["location"]["container"], "worker");
}

#[tokio::test]
async fn worker_omits_the_location_when_it_has_no_pod_identity() {
    let bin = std::env::var("CARGO_BIN_EXE_shiitake-worker")
        .expect("CARGO_BIN_EXE_shiitake-worker not set");
    let capture_root = TempDir::new().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (hello_tx, mut hello_rx) = mpsc::channel::<serde_json::Value>(1);
    let app: Router = Router::new().route(
        "/dispatch",
        get(move |upgrade: WebSocketUpgrade| {
            let hello_tx = hello_tx.clone();
            async move { upgrade.on_upgrade(move |socket| capture_hello(socket, hello_tx)) }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });

    // No POD_NAME / POD_NAMESPACE: a local run. The server falls back to its
    // own pod.
    let mut child = Command::new(&bin)
        .env("SHIITAKE_WORKER_ID", "worker-0")
        .env(
            "SHIITAKE_DISPATCH_URL",
            format!("ws://127.0.0.1:{}/dispatch", addr.port()),
        )
        .env("SHIITAKE_DISPATCH_TOKEN", TOKEN)
        .env("SHIITAKE_CAPTURE_ROOT", capture_root.path())
        .env_remove("POD_NAME")
        .env_remove("POD_NAMESPACE")
        .env("PATH", std::env::var("PATH").unwrap())
        .spawn()
        .expect("spawn worker binary");

    let hello = timeout(Duration::from_secs(15), hello_rx.recv())
        .await
        .expect("no Hello")
        .expect("channel closed");

    child.kill().await.expect("kill worker");
    let _ = child.wait().await;

    assert_eq!(hello["worker_id"], "worker-0");
    assert!(
        hello.get("location").is_none(),
        "a worker with no pod identity must omit location entirely, got {hello}"
    );
}

async fn capture_hello(socket: WebSocket, hello_tx: mpsc::Sender<serde_json::Value>) {
    let (_sink, mut stream) = socket.split();
    if let Some(Ok(Message::Text(t))) = stream.next().await {
        let v: serde_json::Value = serde_json::from_str(&t).unwrap();
        hello_tx.send(v).await.ok();
    }
}
