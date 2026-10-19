use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    routing::get,
};
use futures_util::StreamExt;
use shiitake_worker_api::{
    ExecId, ExecuteFrame, Frame, OutputChunk, OutputStream, SessionCommandMode, SessionId,
    SessionOpenFrame,
};
use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tokio::{net::TcpListener, process::Command, sync::mpsc, time::timeout};

#[tokio::test]
async fn one_session_runs_jobs_in_parallel_and_resets_only_after_release() {
    let capture = TempDir::new().unwrap();
    let scratch = TempDir::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, mut rx) = mpsc::channel(1);
    let scratch_path = scratch.path().to_string_lossy().into_owned();
    let app = Router::new().route(
        "/dispatch",
        get(move |ws: WebSocketUpgrade| {
            let scratch_path = scratch_path.clone();
            let tx = tx.clone();
            async move {
                ws.on_upgrade(move |socket| async move {
                    let result = drive(socket, &scratch_path).await;
                    let _ = tx.send(result).await;
                })
            }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let bin = std::env::var("CARGO_BIN_EXE_shiitake-worker").unwrap();
    let mut worker = Command::new(bin)
        .env("SHIITAKE_WORKER_ID", "profiled-worker")
        .env("SHIITAKE_PROFILE", "isolated")
        .env(
            "SHIITAKE_DISPATCH_URL",
            format!("ws://127.0.0.1:{port}/dispatch"),
        )
        .env("SHIITAKE_DISPATCH_TOKEN", "test-token")
        .env("SHIITAKE_CAPTURE_ROOT", capture.path())
        .env("SHIITAKE_RESET_PATHS", scratch.path())
        .spawn()
        .unwrap();
    let result = timeout(Duration::from_secs(20), rx.recv())
        .await
        .unwrap()
        .unwrap();
    worker.kill().await.unwrap();
    let _ = worker.wait().await;
    result.unwrap();
}

async fn next_frame(ws: &mut WebSocket) -> Frame {
    loop {
        if let Message::Text(t) = ws.next().await.unwrap().unwrap() {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

async fn send(ws: &mut WebSocket, frame: Frame) {
    ws.send(Message::Text(serde_json::to_string(&frame).unwrap().into()))
        .await
        .unwrap();
}

async fn drive(mut ws: WebSocket, scratch: &str) -> anyhow::Result<()> {
    let Frame::Hello {
        worker_id, profile, ..
    } = next_frame(&mut ws).await
    else {
        anyhow::bail!("expected Hello")
    };
    assert_eq!(worker_id.as_str(), "profiled-worker");
    assert_eq!(profile, "isolated");
    let session = SessionId::new("session-a");
    let mut env = BTreeMap::new();
    env.insert("PATH".into(), std::env::var("PATH").unwrap());
    send(
        &mut ws,
        Frame::SessionOpen(SessionOpenFrame {
            session_id: session.clone(),
            working_dir: "/tmp".into(),
            env: env.clone(),
            drop_to: None,
            cols: 80,
            rows: 24,
        }),
    )
    .await;
    assert!(matches!(
        next_frame(&mut ws).await,
        Frame::SessionReady { error: None, .. }
    ));
    send(
        &mut ws,
        Frame::SessionRun {
            session_id: session.clone(),
            mode: SessionCommandMode::Stateful,
            execute: ExecuteFrame {
                request_id: ExecId::new("setup"),
                command: format!("cd {scratch}; export SESSION_VALUE=kept"),
                working_dir: "/tmp".into(),
                env: env.clone(),
                timeout_secs: 10.0,
                drop_to: None,
            },
        },
    )
    .await;
    loop {
        if let Frame::Result(result) = next_frame(&mut ws).await {
            assert_eq!(result.request_id.as_str(), "setup");
            assert_eq!(result.exit_code, Some(0));
            break;
        }
    }
    let started = Instant::now();
    for (rid, cmd) in [
        (
            "job-a",
            "sleep 1; echo A:$SESSION_VALUE; echo marker > marker".into(),
        ),
        ("job-b", "sleep 1; echo B:$SESSION_VALUE; pwd".into()),
    ] {
        send(
            &mut ws,
            Frame::SessionRun {
                session_id: session.clone(),
                mode: SessionCommandMode::Parallel,
                execute: ExecuteFrame {
                    request_id: ExecId::new(rid),
                    command: cmd,
                    working_dir: "/tmp".into(),
                    env: env.clone(),
                    timeout_secs: 10.0,
                    drop_to: None,
                },
            },
        )
        .await;
    }
    let mut output: HashMap<String, Vec<u8>> = HashMap::new();
    let mut results = 0;
    while results < 2 {
        match ws.next().await.unwrap().unwrap() {
            Message::Binary(b) => {
                let chunk = OutputChunk::decode(&b).unwrap();
                if chunk.stream == OutputStream::Stdout {
                    output
                        .entry(chunk.id.into())
                        .or_default()
                        .extend_from_slice(chunk.bytes);
                }
            }
            Message::Text(t) => {
                if let Frame::Result(r) = serde_json::from_str(&t).unwrap() {
                    assert_eq!(r.exit_code, Some(0));
                    results += 1;
                }
            }
            _ => {}
        }
    }
    assert!(
        started.elapsed() < Duration::from_millis(1900),
        "jobs did not overlap"
    );
    assert_eq!(output["job-a"], b"A:kept\n");
    assert_eq!(output["job-b"], format!("B:kept\n{scratch}\n").as_bytes());
    assert!(std::path::Path::new(scratch).join("marker").exists());
    send(
        &mut ws,
        Frame::SessionRun {
            session_id: session.clone(),
            mode: SessionCommandMode::Stateful,
            execute: ExecuteFrame {
                request_id: ExecId::new("timed"),
                command: "sleep 30".into(),
                working_dir: "/tmp".into(),
                env: env.clone(),
                timeout_secs: 0.2,
                drop_to: None,
            },
        },
    )
    .await;
    loop {
        match next_frame(&mut ws).await {
            Frame::Result(result) if result.request_id.as_str() == "timed" => {
                assert!(result.timed_out);
                break;
            }
            Frame::SessionClosed { error, .. } => {
                anyhow::bail!("session closed after timeout: {error:?}")
            }
            _ => {}
        }
    }
    send(
        &mut ws,
        Frame::SessionRun {
            session_id: session.clone(),
            mode: SessionCommandMode::Stateful,
            execute: ExecuteFrame {
                request_id: ExecId::new("after-timeout"),
                command: "test \"$SESSION_VALUE\" = kept".into(),
                working_dir: "/tmp".into(),
                env: env.clone(),
                timeout_secs: 10.0,
                drop_to: None,
            },
        },
    )
    .await;
    loop {
        if let Frame::Result(result) = next_frame(&mut ws).await {
            assert_eq!(result.request_id.as_str(), "after-timeout");
            assert_eq!(result.exit_code, Some(0));
            break;
        }
    }
    send(
        &mut ws,
        Frame::SessionClose {
            session_id: session,
        },
    )
    .await;
    assert!(matches!(
        next_frame(&mut ws).await,
        Frame::SessionClosed { error: None, .. }
    ));
    assert!(!std::path::Path::new(scratch).join("marker").exists());
    Ok(())
}
