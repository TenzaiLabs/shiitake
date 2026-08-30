//! `/ready` vs `/health`: readiness follows the pool, liveness does not.
//!
//! A fake worker connects and then disconnects while the probes are polled,
//! so the endpoint is exercised across the whole registration lifecycle —
//! empty pool, worker registered, worker gone.

use futures_util::SinkExt;
use shiitake_server::{
    http::{AppState, build_api_router, build_dispatch_router},
    pool::WorkerPool,
};
use shiitake_worker_api::{Frame, WorkerId};
use std::{sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::{
    net::{TcpListener, TcpStream},
    time::sleep,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

const TOKEN: &str = "test-token";

/// A fake worker's end of the dispatch socket. Held by the test so the
/// worker stays registered; dropping it deregisters the worker.
type WorkerConn = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// The probe pair as the orchestrator sees it: (status code, body).
async fn probe(base: &str, path: &str) -> (reqwest::StatusCode, serde_json::Value) {
    let resp = reqwest::get(format!("{base}{path}")).await.unwrap();
    let status = resp.status();
    (status, resp.json().await.unwrap())
}

/// Poll `/ready` until it reports `expected`, so the test doesn't race the
/// pool's own registration/drop bookkeeping.
async fn await_ready(base: &str, expected: bool) -> serde_json::Value {
    for _ in 0..100 {
        let (_, body) = probe(base, "/ready").await;
        if body["ready"] == expected {
            return body;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("/ready never reported ready={expected}");
}

/// Bring up the API + dispatch routers on ephemeral ports. Returns the
/// `/api/v1` base URL, the dispatch port, and the capture dir's guard (kept
/// alive by the caller so it is removed when the test ends).
async fn serve(min_ready_workers: usize) -> (String, u16, TempDir) {
    let capture = TempDir::new().unwrap();
    let pool = Arc::new(WorkerPool::new(
        None,
        "shiitake-test".into(),
        "test".into(),
        capture.path().to_path_buf(),
    ));

    let dispatch_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dispatch_port = dispatch_listener.local_addr().unwrap().port();
    let dispatch_router = build_dispatch_router(pool.clone());
    tokio::spawn(async move {
        axum::serve(dispatch_listener, dispatch_router.into_make_service())
            .await
            .unwrap();
    });

    let api_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_port = api_listener.local_addr().unwrap().port();
    let api_router = build_api_router(AppState {
        pool,
        auth_token: Arc::new(TOKEN.into()),
        default_workdir: std::env::temp_dir(),
        max_body_bytes: 256 * 1024 * 1024,
        min_ready_workers,
    });
    tokio::spawn(async move {
        axum::serve(api_listener, api_router.into_make_service())
            .await
            .unwrap();
    });

    (
        format!("http://127.0.0.1:{api_port}/api/v1"),
        dispatch_port,
        capture,
    )
}

async fn connect_worker(dispatch_port: u16, id: &str) -> WorkerConn {
    let url = format!("ws://127.0.0.1:{dispatch_port}/dispatch");
    let (mut ws, _) = connect_async(url).await.unwrap();
    let hello = serde_json::to_string(&Frame::Hello {
        worker_id: WorkerId::new(id),
    })
    .unwrap();
    ws.send(Message::Text(hello.into())).await.unwrap();
    ws
}

#[tokio::test]
async fn ready_follows_the_pool_while_health_stays_up() {
    let (base, dispatch_port, _capture) = serve(1).await;

    // No worker has registered yet: unready, but alive.
    let (status, body) = probe(&base, "/ready").await;
    assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["ready"], false);
    assert_eq!(body["service"], "shiitake");
    assert_eq!(body["workers_idle"], 0);
    assert_eq!(body["workers_required"], 1);
    let (status, body) = probe(&base, "/health").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body["status"], "ok");

    let mut ws = connect_worker(dispatch_port, "fake-0").await;
    let body = await_ready(&base, true).await;
    assert_eq!(body["workers_idle"], 1);
    let (status, _) = probe(&base, "/ready").await;
    assert_eq!(status, reqwest::StatusCode::OK);

    // Every worker dies: unready again, and still not a liveness failure.
    ws.close(None).await.unwrap();
    let body = await_ready(&base, false).await;
    assert_eq!(body["workers_idle"], 0);
    let (status, _) = probe(&base, "/health").await;
    assert_eq!(status, reqwest::StatusCode::OK);

    // Readiness is unauthenticated, like liveness.
    let unauth = reqwest::get(format!("{base}/ready")).await.unwrap();
    assert_eq!(unauth.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn threshold_holds_the_pod_back_until_enough_workers_register() {
    let (base, dispatch_port, _capture) = serve(2).await;

    let _first = connect_worker(dispatch_port, "fake-0").await;
    // One worker registered, two required — still unready.
    sleep(Duration::from_millis(200)).await;
    let (status, body) = probe(&base, "/ready").await;
    assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["workers_idle"], 1);
    assert_eq!(body["workers_required"], 2);

    let _second = connect_worker(dispatch_port, "fake-1").await;
    let body = await_ready(&base, true).await;
    assert_eq!(body["workers_idle"], 2);
}
