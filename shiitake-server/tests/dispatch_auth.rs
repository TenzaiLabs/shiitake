//! The dispatch listener is bearer-authenticated. Once the server and its
//! workers can live in separate pods, being loopback-bound is no longer what
//! protects the dispatcher. The token is checked on the upgrade, before a
//! WebSocket exists, so an unauthenticated peer never sends a frame.

use futures_util::SinkExt;
use shiitake_server::{http::build_dispatch_router, pool::WorkerPool};
use shiitake_worker_api::{Frame, WorkerId, WorkerLocation};
use std::{sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::{net::TcpListener, time::sleep};
use tokio_tungstenite::tungstenite::{
    Message,
    client::IntoClientRequest,
    handshake::client::Request,
    http::header::{AUTHORIZATION, HeaderValue},
};

const DISPATCH_TOKEN: &str = "test-dispatch-token";

/// Stand up a dispatch listener on an ephemeral port; returns its port.
async fn dispatch_listener(pool: Arc<WorkerPool>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = build_dispatch_router(pool, DISPATCH_TOKEN);
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });
    port
}

fn request(port: u16, bearer: Option<&str>) -> Request {
    let mut req = format!("ws://127.0.0.1:{port}/dispatch")
        .into_client_request()
        .unwrap();
    if let Some(b) = bearer {
        req.headers_mut()
            .insert(AUTHORIZATION, HeaderValue::try_from(b).unwrap());
    }
    req
}

/// Hold both, or the capture dir is removed the moment this returns.
fn new_pool() -> (Arc<WorkerPool>, TempDir) {
    let capture = TempDir::new().unwrap();
    let pool = Arc::new(WorkerPool::new(
        None,
        "shiitake-test".into(),
        "test".into(),
        capture.path().to_path_buf(),
    ));
    (pool, capture)
}

#[tokio::test]
async fn rejects_a_worker_with_no_or_wrong_token() {
    let (pool, _capture) = new_pool();
    let port = dispatch_listener(pool.clone()).await;

    for bearer in [None, Some("Bearer wrong-token"), Some("not-even-bearer")] {
        assert!(
            tokio_tungstenite::connect_async(request(port, bearer))
                .await
                .is_err(),
            "the upgrade must fail for {bearer:?}"
        );
    }

    // Nothing reached the pool: a rejected upgrade never becomes a worker.
    sleep(Duration::from_millis(100)).await;
    assert_eq!(pool.snapshot().await, (0, 0));
}

#[tokio::test]
async fn accepts_a_worker_presenting_the_token() {
    let (pool, _capture) = new_pool();
    let port = dispatch_listener(pool.clone()).await;

    let (mut ws, _) =
        tokio_tungstenite::connect_async(request(port, Some("Bearer test-dispatch-token")))
            .await
            .expect("upgrade with the right token must succeed");
    // A Hello from another pod, the shape the split topology sends.
    let hello = serde_json::to_string(&Frame::Hello {
        worker_id: WorkerId::new("shiitake-workers-xyz"),
        location: Some(WorkerLocation {
            pod: "shiitake-workers-xyz".into(),
            namespace: "shiitake".into(),
            container: "worker".into(),
        }),
    })
    .unwrap();
    ws.send(Message::Text(hello.into())).await.unwrap();

    // The worker registers as idle, so the handshake completed end to end.
    for _ in 0..50 {
        if pool.snapshot().await == (1, 0) {
            return;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("worker never registered: {:?}", pool.snapshot().await);
}
