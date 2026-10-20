use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    http::{Method, Request, StatusCode, header::CONTENT_TYPE},
};
use futures_util::{SinkExt, StreamExt};
use shiitake_server::{
    http::{AppState, build_api_router, build_dispatch_router_with_profiles},
    pool::{SessionError, WorkerPool},
};
use shiitake_server_api::{
    CreateSessionRequest, SessionCommandRequest, SessionResponse, SpawnResponse, StatusResponse,
};
use shiitake_worker_api::{
    Frame, OutputChunk, OutputStream, ResultFrame, SessionId, SessionOpenFrame, WorkerId,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::tungstenite::{
    Message,
    client::IntoClientRequest,
    http::header::{AUTHORIZATION, HeaderValue},
};
use tower::ServiceExt;

async fn api_call(
    app: &Router,
    method: Method,
    uri: String,
    body: Option<serde_json::Value>,
) -> (StatusCode, Bytes) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(AUTHORIZATION, "Bearer api-token")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            body.map_or_else(String::new, |value| value.to_string()),
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, bytes)
}

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
            protocol_version: 2,
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
    let state = AppState {
        pool: pool.clone(),
        auth_token: Arc::new("api-token".into()),
        default_workdir: "/tmp".into(),
        max_body_bytes: 1024 * 1024,
        min_ready_workers: 1,
        pty_keepalive: Duration::from_secs(1),
    };
    let app = build_api_router(state);
    tokio::spawn(fake_worker(dispatch_port, "small-0", "small"));
    tokio::spawn(fake_worker(dispatch_port, "root-0", "root"));
    timeout(Duration::from_secs(5), async {
        while pool.profiles().await.len() != 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    for (profile, expected_worker) in [("small", "small-0"), ("root", "root-0")] {
        let create = serde_json::to_value(CreateSessionRequest {
            profile: profile.into(),
            workdir: None,
            env: BTreeMap::new(),
            drop_to: None,
            cols: 80,
            rows: 24,
        })
        .unwrap();
        let (status, body) = api_call(
            &app,
            Method::POST,
            "/api/v1/sessions".into(),
            Some(create.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let session: SessionResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(session.worker_id.as_str(), expected_worker);
        assert_eq!(session.session_id.as_str().len(), 18);
        assert!(session.session_id.as_str().starts_with("s-"));
        let (status, _) = api_call(
            &app,
            Method::POST,
            format!("/api/v1/sessions/{}/commands", session.session_id),
            Some(serde_json::json!({"command":"echo never", "timeout":1e100})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) =
            api_call(&app, Method::POST, "/api/v1/sessions".into(), Some(create)).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        let (status, body) = api_call(
            &app,
            Method::POST,
            format!("/api/v1/sessions/{}/commands", session.session_id),
            Some(
                serde_json::to_value(SessionCommandRequest {
                    command: "echo ignored by fake".into(),
                    timeout: 5.0,
                    mode: shiitake_server_api::SessionCommandMode::Parallel,
                })
                .unwrap(),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let spawned: SpawnResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(spawned.handle.as_str().len(), 18);
        assert!(spawned.handle.as_str().starts_with("c-"));
        let status_url = format!(
            "/api/v1/sessions/{}/commands/{}",
            session.session_id, spawned.handle
        );
        timeout(Duration::from_secs(5), async {
            loop {
                let (status_code, body) =
                    api_call(&app, Method::GET, status_url.clone(), None).await;
                assert_eq!(status_code, StatusCode::OK);
                let status: StatusResponse = serde_json::from_slice(&body).unwrap();
                if status.status != shiitake_server_api::HandleStatus::Running {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let (status, body) =
            api_call(&app, Method::GET, format!("{status_url}/stdout"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body[..], b"profile-output\n");
        let attachment = pool.attach_terminal(&session.session_id).await.unwrap();
        assert!(matches!(
            pool.attach_terminal(&session.session_id).await,
            Err(SessionError::TerminalBusy)
        ));
        drop(attachment); // A failed HTTP upgrade drops its captured attachment.
        let mut replacement = timeout(Duration::from_secs(2), async {
            loop {
                match pool.attach_terminal(&session.session_id).await {
                    Ok(attachment) => break attachment,
                    Err(SessionError::TerminalBusy) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(e) => panic!("unexpected terminal error: {e}"),
                }
            }
        })
        .await
        .unwrap();
        replacement.detach().await;
        let (status, body) = api_call(
            &app,
            Method::DELETE,
            format!("/api/v1/sessions/{}", session.session_id),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let closed: SessionResponse = serde_json::from_slice(&body).unwrap();
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
            protocol_version: 2,
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

#[tokio::test]
async fn idle_session_expires_and_releases_its_worker() {
    let capture = TempDir::new().unwrap();
    let pool = Arc::new(WorkerPool::new(
        None,
        "server".into(),
        "test".into(),
        capture.path().into(),
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let router =
        build_dispatch_router_with_profiles(pool.clone(), "default-token", BTreeMap::new())
            .unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    tokio::spawn(fake_worker(port, "idle-0", "default"));
    timeout(Duration::from_secs(3), async {
        while pool.profiles().await.first().is_none_or(|p| p.idle == 0) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let id = SessionId::new(uuid::Uuid::new_v4().simple().to_string());
    pool.create_session(
        "default".into(),
        SessionOpenFrame {
            session_id: id.clone(),
            working_dir: "/tmp".into(),
            env: BTreeMap::new(),
            drop_to: None,
            cols: 80,
            rows: 24,
        },
    )
    .await
    .unwrap();
    let sweeper = tokio::spawn(pool.clone().run_sweeper(
        Duration::from_millis(10),
        Duration::from_millis(80),
        Duration::from_secs(1),
    ));
    timeout(Duration::from_secs(3), async {
        while pool
            .profiles()
            .await
            .first()
            .is_none_or(|p| p.idle != 1 || p.allocated != 0)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    sweeper.abort();
    assert_eq!(
        pool.session_snapshot(&id).await.unwrap().state,
        shiitake_server_api::SessionState::Closed
    );
}
