//! End-to-end HTTP read path: a fake worker writes a capture file, then the
//! client reads it back through `GET /exec/{handle}/stdout` — full body, a
//! byte range (`206 Partial Content`), and a suffix range (last N bytes).
//! Exercises the tower-http range wiring, auth, and handle validation.

use futures_util::{SinkExt, StreamExt};
use shiitake_server::{
    http::{AppState, build_api_router, build_dispatch_router},
    pool::WorkerPool,
};
use shiitake_worker_api::{
    Frame, ResultFrame,
    capture::{Stream, handle_dir, stream_path},
};
use std::{sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::{net::TcpListener, time::sleep};
use tokio_tungstenite::tungstenite::Message;

const TOKEN: &str = "test-token";

fn payload() -> Vec<u8> {
    (0..200_000u32).map(|i| (i % 251) as u8).collect()
}

#[tokio::test]
async fn reads_full_and_range_and_suffix() {
    let capture = TempDir::new().unwrap();
    let capture_path = capture.path().to_path_buf();
    let pool = Arc::new(WorkerPool::new(
        None,
        "shiitake-test".into(),
        "test".into(),
        capture_path.clone(),
    ));

    let dispatch_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dispatch_addr = dispatch_listener.local_addr().unwrap();
    let dispatch_router = build_dispatch_router(pool.clone());
    tokio::spawn(async move {
        axum::serve(dispatch_listener, dispatch_router.into_make_service())
            .await
            .unwrap();
    });

    // Public API listener.
    let api_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = api_listener.local_addr().unwrap();
    let state = AppState {
        pool: pool.clone(),
        auth_token: Arc::new(TOKEN.into()),
        default_workdir: std::env::temp_dir(),
        max_body_bytes: 256 * 1024 * 1024,
    };
    let api_router = build_api_router(state);
    tokio::spawn(async move {
        axum::serve(api_listener, api_router.into_make_service())
            .await
            .unwrap();
    });

    // Fake worker: on Execute, write the capture file for the request and
    // report success.
    let capture_for_worker = capture_path.clone();
    let worker = tokio::spawn(async move {
        let url = format!("ws://127.0.0.1:{}/dispatch", dispatch_addr.port());
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        ws.send(Message::Text(
            serde_json::to_string(&Frame::Hello {
                worker_id: "fake".into(),
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
        let exec = loop {
            if let Message::Text(t) = ws.next().await.unwrap().unwrap()
                && let Frame::Execute(e) = serde_json::from_str::<Frame>(&t).unwrap()
            {
                break e;
            }
        };
        tokio::fs::create_dir_all(handle_dir(&capture_for_worker, &exec.request_id))
            .await
            .unwrap();
        tokio::fs::write(
            stream_path(&capture_for_worker, &exec.request_id, Stream::Stdout),
            payload(),
        )
        .await
        .unwrap();
        let result = ResultFrame {
            request_id: exec.request_id,
            exit_code: Some(0),
            exit_signal: None,
            timed_out: false,
            cancelled: false,
            usage: Default::default(),
        };
        ws.send(Message::Text(
            serde_json::to_string(&Frame::Result(result))
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
        ws.close(None).await.ok();
    });

    // Wait for the worker to register.
    sleep(Duration::from_millis(200)).await;

    let base = format!("http://127.0.0.1:{}/api/v1", api_addr.port());
    let client = reqwest::Client::new();

    let spawn: serde_json::Value = client
        .post(format!("{base}/exec"))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "command": "true" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let handle = spawn["handle"].as_str().unwrap().to_string();
    worker.await.unwrap();

    // Poll status until terminal so the capture file is fully written.
    for _ in 0..50 {
        let status: serde_json::Value = client
            .get(format!("{base}/exec/{handle}"))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["status"] != "running" {
            assert_eq!(status["stdout_bytes_written"].as_u64().unwrap(), 200_000);
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    let expected = payload();
    let stdout_url = format!("{base}/exec/{handle}/stdout");

    let full = client
        .get(&stdout_url)
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(full.status(), reqwest::StatusCode::OK);
    assert_eq!(full.bytes().await.unwrap().as_ref(), expected.as_slice());

    let ranged = client
        .get(&stdout_url)
        .bearer_auth(TOKEN)
        .header(reqwest::header::RANGE, "bytes=0-9")
        .send()
        .await
        .unwrap();
    assert_eq!(ranged.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert!(
        ranged
            .headers()
            .contains_key(reqwest::header::CONTENT_RANGE)
    );
    assert_eq!(ranged.bytes().await.unwrap().as_ref(), &expected[0..10]);

    let suffix = client
        .get(&stdout_url)
        .bearer_auth(TOKEN)
        .header(reqwest::header::RANGE, "bytes=-10")
        .send()
        .await
        .unwrap();
    assert_eq!(suffix.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        suffix.bytes().await.unwrap().as_ref(),
        &expected[expected.len() - 10..]
    );

    let unauth = client.get(&stdout_url).send().await.unwrap();
    assert_eq!(unauth.status(), reqwest::StatusCode::UNAUTHORIZED);

    let wrong = client
        .get(&stdout_url)
        .bearer_auth("nope")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), reqwest::StatusCode::UNAUTHORIZED);
}
