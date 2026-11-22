//! Records the pool's protocol steps while real requests run against fake
//! workers. Each scenario asserts its outcome here; with `SHIITAKE_TRACE_DIR`
//! set it also writes the recorded steps as JSON, so a model of the protocol
//! can be checked against what the server actually did.

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header::CONTENT_TYPE},
};
use futures_util::{SinkExt, StreamExt};
use shiitake_server::{
    http::{AppState, build_api_router, build_dispatch_router_with_profiles},
    pool::WorkerPool,
};
use shiitake_worker_api::{Frame, WorkerId};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tempfile::TempDir;
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::tungstenite::{
    Message,
    client::IntoClientRequest,
    http::header::{AUTHORIZATION, HeaderValue},
};
use tower::ServiceExt;
use tracing_subscriber::layer::SubscriberExt;

type Steps = Arc<Mutex<Vec<serde_json::Value>>>;

/// Collects the fields of every `shiitake::protocol` event.
struct Recorder(Steps);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Recorder {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "shiitake::protocol" {
            return;
        }
        let mut fields = serde_json::Map::new();
        event.record(&mut Fields(&mut fields));
        self.0.lock().unwrap().push(fields.into());
    }
}

struct Fields<'a>(&'a mut serde_json::Map<String, serde_json::Value>);

impl tracing::field::Visit for Fields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), format!("{value:?}").into());
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().into(), value.into());
    }
}

struct Scenario {
    pool: Arc<WorkerPool>,
    app: Router,
    port: u16,
    steps: Steps,
    _capture: TempDir,
    _recording: tracing::subscriber::DefaultGuard,
}

async fn scenario() -> Scenario {
    let steps = Steps::default();
    let recording = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(Recorder(steps.clone())),
    );
    let capture = TempDir::new().unwrap();
    let pool = Arc::new(
        WorkerPool::new(None, "server".into(), "test".into(), capture.path().into())
            .with_probe_timeout(Duration::from_millis(100))
            .with_session_open_timeout(Duration::from_millis(200))
            .with_live_worker_wait(Duration::from_millis(300)),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let dispatch = build_dispatch_router_with_profiles(
        pool.clone(),
        "default-token",
        BTreeMap::from([("root".into(), "root-token".into())]),
    )
    .unwrap();
    tokio::spawn(async move { axum::serve(listener, dispatch).await.unwrap() });
    let app = build_api_router(AppState {
        pool: pool.clone(),
        auth_token: Arc::new("api-token".into()),
        default_workdir: "/tmp".into(),
        max_body_bytes: 1024 * 1024,
        min_ready_workers: 1,
        pty_keepalive: Duration::from_secs(1),
    });
    Scenario {
        pool,
        app,
        port,
        steps,
        _capture: capture,
        _recording: recording,
    }
}

impl Scenario {
    async fn create(&self) -> StatusCode {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/sessions")
            .header(AUTHORIZATION, "Bearer api-token")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"profile":"default"}"#))
            .unwrap();
        let response = self.app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        to_bytes(response.into_body(), usize::MAX).await.unwrap();
        status
    }

    /// Register a worker and wait until the pool holds `registered` of them.
    async fn join(&self, kind: Kind, id: &'static str, profile: &'static str, registered: usize) {
        tokio::spawn(worker(self.port, kind, id, profile));
        timeout(Duration::from_secs(3), async {
            let count = || async {
                let (idle, busy) = self.pool.snapshot().await;
                idle + busy
            };
            while count().await != registered {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // The server cannot see a connection die, but this test knows which
        // worker it made silent, so the recording says so.
        if matches!(kind, Kind::Silent) {
            self.steps
                .lock()
                .unwrap()
                .push(serde_json::json!({"step": "NetKill", "worker": id}));
        }
        // Long enough for a reading worker's answer to its registration ping.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    /// Let dropped workers leave the pool, then write the recording out.
    async fn finish(self, name: &str) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let Some(dir) = std::env::var_os("SHIITAKE_TRACE_DIR") else {
            return;
        };
        let steps = self.steps.lock().unwrap().clone();
        std::fs::write(
            std::path::Path::new(&dir).join(format!("{name}.json")),
            serde_json::to_vec_pretty(&steps).unwrap(),
        )
        .unwrap();
    }
}

#[derive(Clone, Copy)]
enum Kind {
    /// Answers pings and opens sessions.
    Live,
    /// Registers and never reads again: a connection that died silently.
    Silent,
    /// Answers pings, never acknowledges a session.
    Mute,
    /// Answers pings, hangs up when handed a session.
    Quitter,
}

async fn worker(port: u16, kind: Kind, id: &'static str, profile: &'static str) {
    let mut req = format!("ws://127.0.0.1:{port}/dispatch")
        .into_client_request()
        .unwrap();
    req.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {profile}-token")).unwrap(),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let hello = serde_json::to_string(&Frame::Hello {
        protocol_version: 3,
        worker_id: WorkerId::new(id),
        profile: profile.into(),
        location: None,
    })
    .unwrap();
    ws.send(Message::Text(hello.into())).await.unwrap();
    if matches!(kind, Kind::Silent) {
        return std::future::pending().await;
    }
    while let Some(Ok(msg)) = ws.next().await {
        let Message::Text(text) = msg else { continue };
        let Ok(Frame::SessionOpen(open)) = serde_json::from_str(&text) else {
            continue;
        };
        match kind {
            Kind::Quitter => return,
            Kind::Live => {
                let ready = serde_json::to_string(&Frame::SessionReady {
                    session_id: open.session_id,
                    error: None,
                })
                .unwrap();
                ws.send(Message::Text(ready.into())).await.unwrap();
            }
            Kind::Mute | Kind::Silent => {}
        }
    }
}

#[tokio::test]
async fn one_live_worker() {
    let s = scenario().await;
    s.join(Kind::Live, "live-0", "default", 1).await;
    assert_eq!(s.create().await, StatusCode::CREATED);
    s.finish("one_live_worker").await;
}

#[tokio::test]
async fn silent_worker_ahead_of_a_live_one() {
    let s = scenario().await;
    s.join(Kind::Silent, "silent-0", "default", 1).await;
    s.join(Kind::Live, "live-0", "default", 2).await;
    assert_eq!(s.create().await, StatusCode::CREATED);
    s.finish("silent_worker_ahead_of_a_live_one").await;
}

#[tokio::test]
async fn two_lost_workers_ahead_of_a_live_one() {
    let s = scenario().await;
    s.join(Kind::Mute, "mute-0", "default", 1).await;
    s.join(Kind::Quitter, "quitter-0", "default", 2).await;
    s.join(Kind::Live, "live-0", "default", 3).await;
    assert_eq!(s.create().await, StatusCode::CREATED);
    s.finish("two_lost_workers_ahead_of_a_live_one").await;
}

#[tokio::test]
async fn only_a_silent_worker() {
    let s = scenario().await;
    s.join(Kind::Silent, "silent-0", "default", 1).await;
    assert_eq!(s.create().await, StatusCode::SERVICE_UNAVAILABLE);
    s.finish("only_a_silent_worker").await;
}

#[tokio::test]
async fn only_a_root_worker() {
    let s = scenario().await;
    s.join(Kind::Live, "root-0", "root", 1).await;
    assert_eq!(s.create().await, StatusCode::SERVICE_UNAVAILABLE);
    s.finish("only_a_root_worker").await;
}

#[tokio::test]
async fn second_create_finds_the_only_worker_busy() {
    let s = scenario().await;
    s.join(Kind::Live, "live-0", "default", 1).await;
    assert_eq!(s.create().await, StatusCode::CREATED);
    assert_eq!(s.create().await, StatusCode::TOO_MANY_REQUESTS);
    s.finish("second_create_finds_the_only_worker_busy").await;
}

#[tokio::test]
async fn three_lost_workers() {
    let s = scenario().await;
    s.join(Kind::Mute, "mute-0", "default", 1).await;
    s.join(Kind::Mute, "mute-1", "default", 2).await;
    s.join(Kind::Quitter, "quitter-0", "default", 3).await;
    s.join(Kind::Live, "live-0", "default", 4).await;
    assert_eq!(s.create().await, StatusCode::BAD_GATEWAY);
    s.finish("three_lost_workers").await;
}

#[tokio::test]
async fn only_a_worker_that_never_opens() {
    let s = scenario().await;
    s.join(Kind::Mute, "mute-0", "default", 1).await;
    // The create moves on before the pool has dropped the worker it gave up
    // on; that worker is not "busy", so the answer is 503 and not 429.
    assert_eq!(s.create().await, StatusCode::SERVICE_UNAVAILABLE);
    s.finish("only_a_worker_that_never_opens").await;
}
