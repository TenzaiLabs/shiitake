use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    routing::get,
};
use futures_util::StreamExt;
use nix::sys::signal::{SigHandler, SigSet, SigmaskHow, Signal, signal, sigprocmask};
use shiitake_worker_api::{
    ExecId, ExecuteFrame, Frame, OutputChunk, OutputStream, ResultFrame, SessionCommandMode,
    SessionId, SessionOpenFrame,
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
    let capture_path = capture.path().to_string_lossy().into_owned();
    let app = Router::new().route(
        "/dispatch",
        get(move |ws: WebSocketUpgrade| {
            let scratch_path = scratch_path.clone();
            let capture_path = capture_path.clone();
            let tx = tx.clone();
            async move {
                ws.on_upgrade(move |socket| async move {
                    let result = drive(socket, &scratch_path, &capture_path).await;
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
    assert_eq!(std::fs::read_dir(capture.path()).unwrap().count(), 0);
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

async fn drive(mut ws: WebSocket, scratch: &str, capture: &str) -> anyhow::Result<()> {
    let Frame::Hello {
        worker_id, profile, ..
    } = next_frame(&mut ws).await
    else {
        anyhow::bail!("expected Hello")
    };
    assert_eq!(worker_id.as_str(), "profiled-worker");
    assert_eq!(profile, "isolated");
    send(
        &mut ws,
        Frame::HelloAck {
            protocol_version: 3,
        },
    )
    .await;
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
    send(
        &mut ws,
        Frame::SessionRun {
            session_id: session.clone(),
            mode: SessionCommandMode::Stateful,
            execute: ExecuteFrame {
                request_id: ExecId::new("forged"),
                command: format!("printf 0 > {capture}/forged/status; sleep 0.4; echo finished"),
                working_dir: "/tmp".into(),
                env: env.clone(),
                drop_to: None,
            },
        },
    )
    .await;
    let early = timeout(Duration::from_millis(150), async {
        loop {
            if let Frame::Result(result) = next_frame(&mut ws).await {
                break result;
            }
        }
    })
    .await;
    assert!(early.is_err(), "forged status completed a running command");
    loop {
        if let Frame::Result(result) = next_frame(&mut ws).await {
            assert_eq!(result.request_id.as_str(), "forged");
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
                request_id: ExecId::new("long-running"),
                command: "sleep 30".into(),
                working_dir: "/tmp".into(),
                env: env.clone(),
                drop_to: None,
            },
        },
    )
    .await;
    // A command with unknown wall time stays running until the caller cancels it.
    assert!(
        timeout(Duration::from_millis(350), next_frame(&mut ws))
            .await
            .is_err()
    );
    send(
        &mut ws,
        Frame::Cancel {
            request_id: ExecId::new("long-running"),
        },
    )
    .await;
    loop {
        match next_frame(&mut ws).await {
            Frame::Result(result) if result.request_id.as_str() == "long-running" => {
                assert!(result.cancelled);
                break;
            }
            Frame::SessionClosed { error, .. } => {
                anyhow::bail!("session closed after cancellation: {error:?}")
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
                request_id: ExecId::new("after-cancel"),
                command: "test \"$SESSION_VALUE\" = kept".into(),
                working_dir: "/tmp".into(),
                env: env.clone(),
                drop_to: None,
            },
        },
    )
    .await;
    loop {
        if let Frame::Result(result) = next_frame(&mut ws).await {
            assert_eq!(result.request_id.as_str(), "after-cancel");
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

    let aborted = SessionId::new("session-b");
    send(
        &mut ws,
        Frame::SessionOpen(SessionOpenFrame {
            session_id: aborted.clone(),
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
            session_id: aborted.clone(),
            mode: SessionCommandMode::Stateful,
            execute: ExecuteFrame {
                request_id: ExecId::new("ignored-interrupt"),
                command: "trap '' INT; sleep 5".into(),
                working_dir: "/tmp".into(),
                env,
                drop_to: None,
            },
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    send(
        &mut ws,
        Frame::Cancel {
            request_id: ExecId::new("ignored-interrupt"),
        },
    )
    .await;
    let mut saw_result = false;
    loop {
        match next_frame(&mut ws).await {
            Frame::Result(result) if result.request_id.as_str() == "ignored-interrupt" => {
                assert!(result.cancelled);
                assert_eq!(
                    result.error.as_deref(),
                    Some("stateful command did not stop")
                );
                saw_result = true;
            }
            Frame::SessionClosed {
                error: Some(error), ..
            } => {
                assert_eq!(error, "stateful command did not stop");
                break;
            }
            _ => {}
        }
    }
    assert!(saw_result);
    Ok(())
}

const DISPATCH_TOKEN: &str = "session-test-dispatch-token";

struct Worker {
    process: tokio::process::Child,
    ws: WebSocket,
    _capture: TempDir,
}

/// Starts the worker binary against an in-test dispatcher and opens one session.
async fn start_session(configure: impl FnOnce(&mut Command)) -> Worker {
    let capture = TempDir::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, mut rx) = mpsc::channel(1);
    let app = Router::new().route(
        "/dispatch",
        get(move |ws: WebSocketUpgrade| {
            let tx = tx.clone();
            async move {
                ws.on_upgrade(move |socket| async move {
                    let _ = tx.send(socket).await;
                })
            }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut command = Command::new(std::env::var("CARGO_BIN_EXE_shiitake-worker").unwrap());
    command
        .env("SHIITAKE_WORKER_ID", "session-worker")
        .env(
            "SHIITAKE_DISPATCH_URL",
            format!("ws://127.0.0.1:{port}/dispatch"),
        )
        .env("SHIITAKE_DISPATCH_TOKEN", DISPATCH_TOKEN)
        .env("SHIITAKE_CAPTURE_ROOT", capture.path())
        .kill_on_drop(true);
    configure(&mut command);
    let process = command.spawn().unwrap();
    let mut ws = timeout(Duration::from_secs(10), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(next_frame(&mut ws).await, Frame::Hello { .. }));
    send(
        &mut ws,
        Frame::HelloAck {
            protocol_version: 3,
        },
    )
    .await;
    send(
        &mut ws,
        Frame::SessionOpen(SessionOpenFrame {
            session_id: SessionId::new("session"),
            working_dir: "/tmp".into(),
            env: path_env(),
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
    Worker {
        process,
        ws,
        _capture: capture,
    }
}

fn path_env() -> BTreeMap<String, String> {
    BTreeMap::from([("PATH".into(), std::env::var("PATH").unwrap())])
}

async fn submit(ws: &mut WebSocket, request_id: &str, mode: SessionCommandMode, command: &str) {
    send(
        ws,
        Frame::SessionRun {
            session_id: SessionId::new("session"),
            mode,
            execute: ExecuteFrame {
                request_id: ExecId::new(request_id),
                command: command.into(),
                working_dir: "/tmp".into(),
                env: path_env(),
                drop_to: None,
            },
        },
    )
    .await;
}

/// Waits for one command's Result and returns it with that command's stdout.
async fn result_of(ws: &mut WebSocket, request_id: &str) -> anyhow::Result<(ResultFrame, Vec<u8>)> {
    let mut stdout = Vec::new();
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Binary(b) => {
                let chunk = OutputChunk::decode(&b).unwrap();
                if chunk.stream == OutputStream::Stdout && chunk.id == request_id {
                    stdout.extend_from_slice(chunk.bytes);
                }
            }
            Message::Text(t) => match serde_json::from_str(&t).unwrap() {
                Frame::Result(result) if result.request_id.as_str() == request_id => {
                    return Ok((result, stdout));
                }
                Frame::SessionClosed { error, .. } => {
                    anyhow::bail!("session closed before {request_id} finished: {error:?}")
                }
                _ => {}
            },
            _ => {}
        }
    }
}

#[tokio::test]
async fn parallel_job_waits_for_an_earlier_stateful_job_and_inherits_its_state() {
    let mut worker = start_session(|_| {}).await;
    submit(
        &mut worker.ws,
        "setup",
        SessionCommandMode::Stateful,
        "sleep 0.5; cd /; export ORDERED=yes",
    )
    .await;
    submit(
        &mut worker.ws,
        "probe",
        SessionCommandMode::Parallel,
        "echo \"$ORDERED $PWD\"",
    )
    .await;
    let (result, stdout) = result_of(&mut worker.ws, "probe").await.unwrap();
    assert_eq!(result.exit_code, Some(0));
    assert_eq!(String::from_utf8_lossy(&stdout), "yes /\n");
}

#[tokio::test]
async fn cancel_works_when_the_worker_inherits_ignored_or_blocked_signals() {
    // `nohup worker &` from a non-interactive shell starts the worker with
    // these signals ignored. Exec preserves ignored dispositions and the
    // blocked mask, so a launcher can also leave signals blocked.
    let mut worker = start_session(|command| unsafe {
        command.pre_exec(|| {
            for sig in [Signal::SIGINT, Signal::SIGQUIT, Signal::SIGHUP] {
                signal(sig, SigHandler::SigIgn)?;
            }
            let mut blocked = SigSet::empty();
            blocked.add(Signal::SIGINT);
            blocked.add(Signal::SIGTERM);
            sigprocmask(SigmaskHow::SIG_BLOCK, Some(&blocked), None)?;
            Ok(())
        });
    })
    .await;
    submit(
        &mut worker.ws,
        "setup",
        SessionCommandMode::Stateful,
        "export KEPT=yes",
    )
    .await;
    let (setup, _) = result_of(&mut worker.ws, "setup").await.unwrap();
    assert_eq!(setup.exit_code, Some(0));
    submit(
        &mut worker.ws,
        "hung",
        SessionCommandMode::Stateful,
        "sleep 30",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    send(
        &mut worker.ws,
        Frame::Cancel {
            request_id: ExecId::new("hung"),
        },
    )
    .await;
    let (hung, _) = result_of(&mut worker.ws, "hung").await.unwrap();
    assert!(hung.cancelled);
    assert_eq!(hung.error, None);
    assert_eq!(hung.exit_code, Some(130));
    submit(
        &mut worker.ws,
        "after",
        SessionCommandMode::Stateful,
        "test \"$KEPT\" = yes",
    )
    .await;
    let (after, _) = result_of(&mut worker.ws, "after").await.unwrap();
    assert_eq!(after.exit_code, Some(0));
    submit(
        &mut worker.ws,
        "background",
        SessionCommandMode::Parallel,
        "sleep 30",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    send(
        &mut worker.ws,
        Frame::Cancel {
            request_id: ExecId::new("background"),
        },
    )
    .await;
    let (background, _) = result_of(&mut worker.ws, "background").await.unwrap();
    assert!(background.cancelled);
    // SIGTERM ends it; a blocked SIGTERM would leave it for the SIGKILL fallback.
    assert_eq!(background.exit_signal, Some(Signal::SIGTERM as i32));
}

#[tokio::test]
async fn session_commands_cannot_read_the_dispatch_credential() {
    let mut worker = start_session(|_| {}).await;
    let pid = worker.process.id().unwrap();
    let probe = format!(
        "if tr '\\0' '\\n' < /proc/{pid}/environ 2>/dev/null | grep -q {DISPATCH_TOKEN}; \
         then echo exposed; elif env | grep -q {DISPATCH_TOKEN}; then echo inherited; \
         else echo hidden; fi"
    );
    for (request_id, mode) in [
        ("stateful-probe", SessionCommandMode::Stateful),
        ("parallel-probe", SessionCommandMode::Parallel),
    ] {
        submit(&mut worker.ws, request_id, mode, &probe).await;
        let (result, stdout) = result_of(&mut worker.ws, request_id).await.unwrap();
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(String::from_utf8_lossy(&stdout), "hidden\n", "{request_id}");
    }
}
