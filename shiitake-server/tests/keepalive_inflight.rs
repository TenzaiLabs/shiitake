//! The keepalive pings in-flight workers, not just idle ones.
//!
//! A busy worker sends nothing between its Execute and its Result, so without
//! this a command that outlives its server is invisible from both ends: the pool
//! holds the handle open, and the worker's own lease has no traffic to measure.
//! Pinging in-flight workers is what gives the worker something to time out
//! against, and what lets the pool evict one that has genuinely wedged.

use futures_util::{SinkExt, StreamExt};
use shiitake_server::{
    http::build_dispatch_router,
    pool::{ExitCause, WorkerPool},
};
use shiitake_worker_api::{ExecId, ExecuteFrame, Frame, WorkerId};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::{
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::header::{AUTHORIZATION, HeaderValue},
    },
};

const DISPATCH_TOKEN: &str = "test-dispatch-token";

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

async fn connect_worker(port: u16) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
    let mut req = format!("ws://127.0.0.1:{port}/dispatch")
        .into_client_request()
        .unwrap();
    req.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::try_from(format!("Bearer {DISPATCH_TOKEN}")).unwrap(),
    );
    let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws
}

fn frame(id: &str) -> ExecuteFrame {
    ExecuteFrame {
        request_id: ExecId::new(id),
        command: "sleep 600".into(),
        working_dir: "/tmp".into(),
        env: BTreeMap::new(),
        timeout_secs: 600.0,
        drop_to: None,
    }
}

#[tokio::test]
async fn an_inflight_worker_is_pinged() {
    let capture = TempDir::new().unwrap();
    let pool = Arc::new(WorkerPool::new(
        None,
        "shiitake-test".into(),
        "test".into(),
        capture.path().to_path_buf(),
    ));
    let port = dispatch_listener(pool.clone()).await;
    let mut ws = connect_worker(port).await;

    let hello = serde_json::to_string(&Frame::Hello {
        worker_id: WorkerId::new("busy-0"),
        location: None,
    })
    .unwrap();
    ws.send(Message::Text(hello.into())).await.unwrap();
    sleep(Duration::from_millis(150)).await;

    // Take the worker in-flight and leave it there: no Result is ever sent.
    pool.dispatch(frame("req-busy")).await.expect("dispatch");

    // Ping fast so the test doesn't wait on production's 10s cadence. A long
    // eviction window keeps this test about the ping, not about eviction.
    let keepalive = pool.clone();
    tokio::spawn(async move {
        keepalive
            .run_keepalive(Duration::from_millis(100), Duration::from_secs(600))
            .await;
    });

    // A Ping must arrive even though this worker is busy, never idle.
    let pinged = timeout(Duration::from_secs(10), async {
        while let Some(Ok(msg)) = ws.next().await {
            if matches!(msg, Message::Ping(_)) {
                return true;
            }
        }
        false
    })
    .await
    .expect("timed out waiting for a keepalive ping on an in-flight worker");
    assert!(pinged, "the socket closed before an in-flight ping arrived");
}

#[tokio::test]
async fn an_inflight_worker_that_stops_answering_is_evicted() {
    let capture = TempDir::new().unwrap();
    let pool = Arc::new(WorkerPool::new(
        None,
        "shiitake-test".into(),
        "test".into(),
        capture.path().to_path_buf(),
    ));
    let port = dispatch_listener(pool.clone()).await;
    let mut ws = connect_worker(port).await;

    let hello = serde_json::to_string(&Frame::Hello {
        worker_id: WorkerId::new("wedged-0"),
        location: None,
    })
    .unwrap();
    ws.send(Message::Text(hello.into())).await.unwrap();
    sleep(Duration::from_millis(150)).await;

    let snap = pool.dispatch(frame("req-wedged")).await.expect("dispatch");

    // Hold the socket open but never poll it again: tungstenite only answers
    // pings while the stream is polled, so this is a worker that has gone silent
    // mid-command — the wedge the eviction window exists for.
    let _wedged = ws;

    let keepalive = pool.clone();
    tokio::spawn(async move {
        keepalive
            .run_keepalive(Duration::from_millis(50), Duration::from_millis(300))
            .await;
    });

    // The handle must be reconciled rather than left Running forever.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let snap = pool
            .touch_and_snapshot(&snap.handle_id)
            .await
            .expect("handle still known");
        if let Some(cause) = snap.exit_cause {
            assert_eq!(cause, ExitCause::WorkerDied, "{snap:?}");
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "in-flight worker was never evicted: {snap:?}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}
