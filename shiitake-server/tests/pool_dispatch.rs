//! Server's WorkerPool driven by fake workers over the dispatch WebSocket:
//! handle transitions on Result and on a worker drop, the empty-pool
//! rejection, and least-recently-used worker selection.

use futures_util::{SinkExt, StreamExt};
use shiitake_server::{
    http::build_dispatch_router,
    pool::{ExitCause, HandleStatus, WorkerPool},
};
use shiitake_worker_api::{ExecId, ExecuteFrame, Frame, ResultFrame, WorkerId};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    time::{sleep, timeout},
};
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn dispatch_with_one_fake_worker_marks_handle_completed() {
    let capture = TempDir::new().unwrap();
    let probe = None;
    let pool = Arc::new(WorkerPool::new(
        probe,
        "shiitake-test".into(),
        "test".into(),
        capture.path().to_path_buf(),
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = build_dispatch_router(pool.clone());
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });

    let worker_task = tokio::spawn(async move {
        let url = format!("ws://127.0.0.1:{}/dispatch", addr.port());
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let hello = serde_json::to_string(&Frame::Hello {
            worker_id: WorkerId::new("fake-0"),
        })
        .unwrap();
        ws.send(Message::Text(hello.into())).await.unwrap();

        let exec_msg = ws.next().await.unwrap().unwrap();
        let exec_text = match exec_msg {
            Message::Text(t) => t,
            other => panic!("expected text, got {other:?}"),
        };
        let frame: Frame = serde_json::from_str(&exec_text).unwrap();
        let exec = match frame {
            Frame::Execute(e) => e,
            other => panic!("expected Execute, got {other:?}"),
        };

        let result = ResultFrame {
            request_id: exec.request_id,
            exit_code: Some(0),
            exit_signal: None,
            timed_out: false,
            cancelled: false,
            usage: Default::default(),
            error: None,
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

    // Give the fake worker time to connect + Hello.
    sleep(Duration::from_millis(150)).await;

    let env: BTreeMap<String, String> = BTreeMap::new();
    let snap = pool
        .dispatch(ExecuteFrame {
            request_id: ExecId::new("req-A"),
            command: "echo hi".into(),
            working_dir: "/tmp".into(),
            env,
            timeout_secs: 5.0,
            drop_to: None,
        })
        .await
        .unwrap();
    assert_eq!(snap.status, HandleStatus::Running);

    timeout(
        Duration::from_secs(10),
        pool.wait_for_terminal(&snap.handle_id),
    )
    .await
    .expect("wait_for_terminal did not resolve");
    worker_task.await.unwrap();

    let after = pool.touch_and_snapshot(&snap.handle_id).await.unwrap();
    assert_eq!(after.status, HandleStatus::Completed);
    assert_eq!(after.exit_code, Some(0));
    assert_eq!(after.exit_cause, Some(ExitCause::Normal));
}

#[tokio::test]
async fn worker_drop_marks_handle_worker_died() {
    let capture = TempDir::new().unwrap();
    let probe = None;
    let pool = Arc::new(WorkerPool::new(
        probe,
        "shiitake-test".into(),
        "test".into(),
        capture.path().to_path_buf(),
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = build_dispatch_router(pool.clone());
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });

    let worker_task = tokio::spawn(async move {
        let url = format!("ws://127.0.0.1:{}/dispatch", addr.port());
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let hello = serde_json::to_string(&Frame::Hello {
            worker_id: WorkerId::new("doomed"),
        })
        .unwrap();
        ws.send(Message::Text(hello.into())).await.unwrap();
        // Receive Execute, then ABRUPTLY close.
        let _ = ws.next().await;
        ws.close(None).await.ok();
    });

    sleep(Duration::from_millis(150)).await;
    let snap = pool
        .dispatch(ExecuteFrame {
            request_id: ExecId::new("req-die"),
            command: "sleep 99".into(),
            working_dir: "/tmp".into(),
            env: BTreeMap::new(),
            timeout_secs: 60.0,
            drop_to: None,
        })
        .await
        .unwrap();

    timeout(
        Duration::from_secs(10),
        pool.wait_for_terminal(&snap.handle_id),
    )
    .await
    .expect("wait_for_terminal did not resolve");
    worker_task.await.unwrap();

    let after = pool.touch_and_snapshot(&snap.handle_id).await.unwrap();
    assert_eq!(after.status, HandleStatus::Error);
    assert_eq!(after.exit_cause, Some(ExitCause::WorkerDied));
    assert_eq!(after.worker_id.as_str(), "doomed");
}

#[tokio::test]
async fn dispatch_without_idle_worker_returns_no_idle() {
    let capture = TempDir::new().unwrap();
    let probe = None;
    let pool = WorkerPool::new(
        probe,
        "shiitake-test".into(),
        "test".into(),
        capture.path().to_path_buf(),
    );

    let err = pool
        .dispatch(ExecuteFrame {
            request_id: ExecId::new("req"),
            command: "echo".into(),
            working_dir: "/tmp".into(),
            env: BTreeMap::new(),
            timeout_secs: 1.0,
            drop_to: None,
        })
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        shiitake_server::pool::DispatchError::NoIdleWorker
    ));
}

/// A serial stream of commands — each finished before the next is submitted —
/// must rotate through the whole pool rather than landing on the worker that
/// just returned. The idle set is FIFO, so selection is least-recently-used.
#[tokio::test]
async fn serial_dispatch_rotates_through_idle_workers() {
    let capture = TempDir::new().unwrap();
    let probe = None;
    let pool = Arc::new(WorkerPool::new(
        probe,
        "shiitake-test".into(),
        "test".into(),
        capture.path().to_path_buf(),
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = build_dispatch_router(pool.clone());
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });

    let worker_ids = ["rot-0", "rot-1", "rot-2"];
    let url = format!("ws://127.0.0.1:{}/dispatch", addr.port());
    for (i, id) in worker_ids.iter().enumerate() {
        // Connect and Hello one at a time, waiting for the pool to register
        // each before starting the next, so the idle order is deterministic.
        let (mut ws, _) = tokio_tungstenite::connect_async(url.clone()).await.unwrap();
        let hello = serde_json::to_string(&Frame::Hello {
            worker_id: WorkerId::new(*id),
        })
        .unwrap();
        ws.send(Message::Text(hello.into())).await.unwrap();

        // Serve Execute frames forever: reply Result, stay connected.
        tokio::spawn(async move {
            while let Some(Ok(msg)) = ws.next().await {
                let Message::Text(t) = msg else { continue };
                let Ok(Frame::Execute(exec)) = serde_json::from_str::<Frame>(&t) else {
                    continue;
                };
                let result = ResultFrame {
                    request_id: exec.request_id,
                    exit_code: Some(0),
                    exit_signal: None,
                    timed_out: false,
                    cancelled: false,
                    usage: Default::default(),
                    error: None,
                };
                ws.send(Message::Text(
                    serde_json::to_string(&Frame::Result(result))
                        .unwrap()
                        .into(),
                ))
                .await
                .unwrap();
            }
        });

        timeout(Duration::from_secs(10), async {
            while pool.snapshot().await.0 <= i {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker did not register");
    }

    // Four serial commands over a pool of three: each waits for the previous
    // to finish, so at every dispatch all three workers are idle.
    let mut served = Vec::new();
    for i in 0..4 {
        let snap = pool
            .dispatch(ExecuteFrame {
                request_id: ExecId::new(format!("rot-req-{i}")),
                command: "echo hi".into(),
                working_dir: "/tmp".into(),
                env: BTreeMap::new(),
                timeout_secs: 5.0,
                drop_to: None,
            })
            .await
            .unwrap();
        timeout(
            Duration::from_secs(10),
            pool.wait_for_terminal(&snap.handle_id),
        )
        .await
        .expect("wait_for_terminal did not resolve");
        let after = pool.touch_and_snapshot(&snap.handle_id).await.unwrap();
        assert_eq!(after.status, HandleStatus::Completed);
        served.push(after.worker_id.as_str().to_string());
    }

    // Least-recently-used order: each worker in turn, then back to the first.
    assert_eq!(served, ["rot-0", "rot-1", "rot-2", "rot-0"]);
}
