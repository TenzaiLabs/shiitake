use futures_util::{SinkExt, StreamExt};
use shiitake_server::{
    http::{AppState, build_api_router, build_dispatch_router_with_profiles},
    pool::WorkerPool,
};
use shiitake_server_api::{
    CreateSessionRequest, SessionCommandRequest, SessionResponse, SpawnResponse, StatusResponse,
};
use shiitake_worker_api::{Frame, OutputChunk, OutputStream, ResultFrame, WorkerId};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::tungstenite::{
    Message,
    client::IntoClientRequest,
    http::header::{AUTHORIZATION, HeaderValue},
};

async fn fake_worker(port: u16, id: &'static str, profile: &'static str) {
    let mut req = format!("ws://127.0.0.1:{port}/dispatch")
        .into_client_request()
        .unwrap();
    req.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {profile}-token")).unwrap(),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(Message::Text(
        serde_json::to_string(&Frame::Hello {
            worker_id: WorkerId::new(id),
            profile: profile.into(),
            location: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();
    while let Some(Ok(msg)) = ws.next().await {
        let Message::Text(t) = msg else { continue };
        let frame: Frame = serde_json::from_str(&t).unwrap();
        match frame {
            Frame::SessionOpen(open) => {
                ws.send(Message::Text(
                    serde_json::to_string(&Frame::SessionReady {
                        session_id: open.session_id,
                        error: None,
                    })
                    .unwrap()
                    .into(),
                ))
                .await
                .unwrap();
            }
            Frame::SessionRun { execute, .. } => {
                let chunk = OutputChunk {
                    stream: OutputStream::Stdout,
                    id: execute.request_id.as_str(),
                    bytes: b"profile-output\n",
                };
                ws.send(Message::Binary(chunk.encode().into()))
                    .await
                    .unwrap();
                ws.send(Message::Text(
                    serde_json::to_string(&Frame::Result(ResultFrame {
                        request_id: execute.request_id,
                        exit_code: Some(0),
                        exit_signal: None,
                        timed_out: false,
                        cancelled: false,
                        usage: Default::default(),
                        error: None,
                    }))
                    .unwrap()
                    .into(),
                ))
                .await
                .unwrap();
            }
            Frame::SessionClose { session_id } => {
                ws.send(Message::Text(
                    serde_json::to_string(&Frame::SessionClosed {
                        session_id,
                        error: None,
                        reusable: true,
                    })
                    .unwrap()
                    .into(),
                ))
                .await
                .unwrap();
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn profiles_reserve_workers_and_output_is_spooled_on_server() {
    let capture = TempDir::new().unwrap();
    let pool = Arc::new(WorkerPool::new(
        None,
        "server".into(),
        "test".into(),
        capture.path().into(),
    ));
    let dispatch = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dispatch_port = dispatch.local_addr().unwrap().port();
    let dispatch_app = build_dispatch_router_with_profiles(
        pool.clone(),
        "default-token",
        BTreeMap::from([
            ("small".into(), "small-token".into()),
            ("root".into(), "root-token".into()),
        ]),
    )
    .unwrap();
    tokio::spawn(async move {
        axum::serve(dispatch, dispatch_app).await.unwrap();
    });
    let api = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_port = api.local_addr().unwrap().port();
    let state = AppState {
        pool: pool.clone(),
        auth_token: Arc::new("api-token".into()),
        default_workdir: "/tmp".into(),
        max_body_bytes: 1024 * 1024,
        min_ready_workers: 1,
        pty_keepalive: Duration::from_secs(1),
    };
    tokio::spawn(async move {
        axum::serve(api, build_api_router(state)).await.unwrap();
    });
    tokio::spawn(fake_worker(dispatch_port, "small-0", "small"));
    tokio::spawn(fake_worker(dispatch_port, "root-0", "root"));
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{api_port}/api/v1");
    timeout(Duration::from_secs(5), async {
        while pool.profiles().await.len() != 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    for (profile, expected_worker) in [("small", "small-0"), ("root", "root-0")] {
        let created = http
            .post(format!("{base}/sessions"))
            .bearer_auth("api-token")
            .json(&CreateSessionRequest {
                profile: profile.into(),
                workdir: None,
                env: BTreeMap::new(),
                drop_to: None,
                cols: 80,
                rows: 24,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(created.status(), 201);
        let session: SessionResponse = created.json().await.unwrap();
        assert_eq!(session.worker_id.as_str(), expected_worker);
        let busy = http
            .post(format!("{base}/sessions"))
            .bearer_auth("api-token")
            .json(&CreateSessionRequest {
                profile: profile.into(),
                workdir: None,
                env: BTreeMap::new(),
                drop_to: None,
                cols: 80,
                rows: 24,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(busy.status(), 429);
        let command = http
            .post(format!("{base}/sessions/{}/commands", session.session_id))
            .bearer_auth("api-token")
            .json(&SessionCommandRequest {
                command: "echo ignored by fake".into(),
                timeout: 5.0,
                mode: shiitake_server_api::SessionCommandMode::Parallel,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(command.status(), 202);
        let spawned: SpawnResponse = command.json().await.unwrap();
        let status_url = format!(
            "{base}/sessions/{}/commands/{}",
            session.session_id, spawned.handle
        );
        timeout(Duration::from_secs(5), async {
            loop {
                let status: StatusResponse = http
                    .get(&status_url)
                    .bearer_auth("api-token")
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                if status.status != shiitake_server_api::HandleStatus::Running {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let body = http
            .get(format!("{status_url}/stdout"))
            .bearer_auth("api-token")
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&body[..], b"profile-output\n");
        let closed = http
            .delete(format!("{base}/sessions/{}", session.session_id))
            .bearer_auth("api-token")
            .send()
            .await
            .unwrap();
        assert_eq!(closed.status(), 200);
        let closed: SessionResponse = closed.json().await.unwrap();
        assert_eq!(closed.state, shiitake_server_api::SessionState::Closed);
    }
}

#[tokio::test]
async fn dispatch_credential_cannot_claim_another_profile() {
    let capture = TempDir::new().unwrap();
    let pool = Arc::new(WorkerPool::new(
        None,
        "server".into(),
        "test".into(),
        capture.path().into(),
    ));
    let dispatch = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = dispatch.local_addr().unwrap().port();
    let router = build_dispatch_router_with_profiles(
        pool.clone(),
        "default-token",
        BTreeMap::from([("isolated".into(), "isolated-token".into())]),
    )
    .unwrap();
    tokio::spawn(async move {
        axum::serve(dispatch, router).await.unwrap();
    });
    let mut req = format!("ws://127.0.0.1:{port}/dispatch")
        .into_client_request()
        .unwrap();
    req.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer isolated-token"),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(Message::Text(
        serde_json::to_string(&Frame::Hello {
            worker_id: WorkerId::new("spoof"),
            profile: "default".into(),
            location: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();
    let closed = timeout(Duration::from_secs(2), ws.next()).await.unwrap();
    assert!(matches!(
        closed,
        None | Some(Err(_)) | Some(Ok(Message::Close(_)))
    ));
    assert!(pool.profiles().await.is_empty());
}

#[tokio::test]
async fn dispatch_rejects_ambiguous_profile_credentials() {
    let capture = TempDir::new().unwrap();
    let pool = Arc::new(WorkerPool::new(
        None,
        "server".into(),
        "test".into(),
        capture.path().into(),
    ));
    for extra in [
        BTreeMap::from([("default".into(), "other-token".into())]),
        BTreeMap::from([("isolated".into(), "default-token".into())]),
        BTreeMap::from([("isolated".into(), String::new())]),
    ] {
        assert!(build_dispatch_router_with_profiles(pool.clone(), "default-token", extra).is_err());
    }
}
