//! Ranged HTTP reads of output streamed over dispatch from a worker with no
//! access to the server's capture directory.

use futures_util::{SinkExt, StreamExt};
use shiitake_server::{
    http::{AppState, build_api_router, build_dispatch_router_with_profiles},
    pool::WorkerPool,
};
use shiitake_worker_api::{Frame, OutputChunk, OutputStream, ResultFrame, WorkerId};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::{net::TcpListener, time::sleep};
use tokio_tungstenite::tungstenite::{
    Message,
    client::IntoClientRequest,
    http::header::{AUTHORIZATION, HeaderValue},
};

fn payload() -> Vec<u8> {
    (0u8..251).cycle().take(200_000).collect()
}

#[tokio::test]
async fn reads_full_and_range_and_suffix() {
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
        BTreeMap::from([("isolated".into(), "isolated-token".into())]),
    )
    .unwrap();
    tokio::spawn(async move {
        axum::serve(dispatch, dispatch_app).await.unwrap();
    });
    let api = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_port = api.local_addr().unwrap().port();
    let state = AppState {
        pool,
        auth_token: Arc::new("api-token".into()),
        default_workdir: "/tmp".into(),
        max_body_bytes: 1024 * 1024,
        min_ready_workers: 1,
        pty_keepalive: Duration::from_secs(1),
    };
    tokio::spawn(async move {
        axum::serve(api, build_api_router(state)).await.unwrap();
    });
    let worker = tokio::spawn(async move {
        let mut req = format!("ws://127.0.0.1:{dispatch_port}/dispatch")
            .into_client_request()
            .unwrap();
        req.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer isolated-token"),
        );
        let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        ws.send(Message::Text(
            serde_json::to_string(&Frame::Hello {
                worker_id: WorkerId::new("fake"),
                profile: "isolated".into(),
                location: None,
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
        while let Some(Ok(Message::Text(t))) = ws.next().await {
            match serde_json::from_str::<Frame>(&t).unwrap() {
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
                    let bytes = payload();
                    for part in bytes.chunks(32 * 1024) {
                        let chunk = OutputChunk {
                            stream: OutputStream::Stdout,
                            id: execute.request_id.as_str(),
                            bytes: part,
                        };
                        ws.send(Message::Binary(chunk.encode().into()))
                            .await
                            .unwrap();
                    }
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
                    break;
                }
                _ => {}
            }
        }
    });
    sleep(Duration::from_millis(100)).await;
    let base = format!("http://127.0.0.1:{api_port}/api/v1");
    let client = reqwest::Client::new();
    let created: serde_json::Value = client
        .post(format!("{base}/sessions"))
        .bearer_auth("api-token")
        .json(&serde_json::json!({"profile": "isolated"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let sid = created["session_id"].as_str().unwrap();
    let spawned: serde_json::Value = client
        .post(format!("{base}/sessions/{sid}/commands"))
        .bearer_auth("api-token")
        .json(&serde_json::json!({"command": "true", "mode": "parallel"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let handle = spawned["handle"].as_str().unwrap();
    let status_url = format!("{base}/sessions/{sid}/commands/{handle}");
    for _ in 0..100 {
        let status: serde_json::Value = client
            .get(&status_url)
            .bearer_auth("api-token")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["status"] != "running" {
            assert_eq!(status["stdout_bytes_written"], 200_000);
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    let expected = payload();
    let stdout_url = format!("{status_url}/stdout");
    let full = client
        .get(&stdout_url)
        .bearer_auth("api-token")
        .send()
        .await
        .unwrap();
    assert_eq!(full.status(), reqwest::StatusCode::OK);
    assert_eq!(full.bytes().await.unwrap().as_ref(), expected.as_slice());
    let ranged = client
        .get(&stdout_url)
        .bearer_auth("api-token")
        .header(reqwest::header::RANGE, "bytes=0-9")
        .send()
        .await
        .unwrap();
    assert_eq!(ranged.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(ranged.bytes().await.unwrap().as_ref(), &expected[0..10]);
    let suffix = client
        .get(&stdout_url)
        .bearer_auth("api-token")
        .header(reqwest::header::RANGE, "bytes=-10")
        .send()
        .await
        .unwrap();
    assert_eq!(suffix.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        suffix.bytes().await.unwrap().as_ref(),
        &expected[expected.len() - 10..]
    );
    assert_eq!(
        client.get(&stdout_url).send().await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(&stdout_url)
            .bearer_auth("wrong")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    client
        .delete(format!("{base}/sessions/{sid}"))
        .bearer_auth("api-token")
        .send()
        .await
        .unwrap();
    worker.await.unwrap();
}
