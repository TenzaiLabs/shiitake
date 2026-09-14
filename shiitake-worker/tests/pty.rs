//! End-to-end tests of the worker's interactive PTY path: spawn the real worker
//! binary, act as the dispatch server, open a pty, and prove output flows
//! worker→server as WS binary, stdin flows server→worker, the shell's exit
//! status comes back as a `PtyExit`, and `SHIITAKE_PTY_SHELL` overrides the
//! default command. Runs entirely on the host (no docker) — same shape as
//! `happy_path.rs`.

use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};
use tempfile::TempDir;
use tokio::{net::TcpListener, process::Command, sync::mpsc, time::timeout};

#[tokio::test]
async fn pty_streams_output_accepts_stdin_and_reports_exit() {
    // `stty -echo` keeps the tty from echoing our stdin, so the asserted output
    // is exactly what the script prints; `read` pulls the line we inject.
    let open = json!({
        "kind": "pty_open",
        "session_id": "sess-1",
        "command": ["bash", "-c", "stty -echo; echo READY; read line; echo got=$line; exit 3"],
        "working_dir": std::env::temp_dir(),
        "env": path_env(),
        "cols": 80,
        "rows": 24,
    });
    let (output, exit) = run_pty(&[], open, Some(b"world\n")).await;

    let text = String::from_utf8_lossy(&output);
    assert!(
        text.contains("READY"),
        "pty output should carry the banner; got: {text:?}"
    );
    assert!(
        text.contains("got=world"),
        "shell should echo our stdin; got: {text:?}"
    );
    assert_eq!(exit["kind"], "pty_exit");
    assert_eq!(
        exit["exit_code"], 3,
        "the shell's exit status must reach the server"
    );
}

#[tokio::test]
async fn pty_default_command_comes_from_shiitake_pty_shell_env() {
    // No command in the frame → the worker uses its default, which the env
    // overrides. `printf CUSTOMSHELL` is unambiguous: the built-in bash default
    // would never print it.
    let open = json!({
        "kind": "pty_open",
        "session_id": "sess-2",
        "command": [],
        "working_dir": std::env::temp_dir(),
        "env": path_env(),
        "cols": 80,
        "rows": 24,
    });
    let (output, exit) = run_pty(&[("SHIITAKE_PTY_SHELL", "printf CUSTOMSHELL")], open, None).await;

    let text = String::from_utf8_lossy(&output);
    assert!(
        text.contains("CUSTOMSHELL"),
        "the env-configured default must run; got: {text:?}"
    );
    assert_eq!(exit["exit_code"], 0);
}

/// A throwaway per-run dispatch token — the worker requires one to boot; the
/// mock dispatcher here does not check it, so any unpredictable value works. No
/// fixed secret in the tree.
fn rand_token() -> String {
    use std::io::Read;
    let mut buf = [0u8; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut buf))
        .expect("read /dev/urandom");
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

fn path_env() -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    env.insert(
        "PATH".to_string(),
        std::env::var("PATH").unwrap_or_default(),
    );
    env
}

/// Spawn the worker binary against a mock dispatch server, send `open`, feed
/// `stdin` once the shell speaks (if any), and return (all pty output, PtyExit).
async fn run_pty(
    worker_env: &[(&str, &str)],
    open: Value,
    stdin: Option<&[u8]>,
) -> (Vec<u8>, Value) {
    let bin = std::env::var("CARGO_BIN_EXE_shiitake-worker")
        .expect("CARGO_BIN_EXE_shiitake-worker not set");
    let capture_root = TempDir::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(1);
    let (exit_tx, mut exit_rx) = mpsc::channel::<Value>(1);
    let stdin = stdin.map(<[u8]>::to_vec);
    let app: Router = Router::new().route(
        "/dispatch",
        get(move |upgrade: WebSocketUpgrade| {
            let (out_tx, exit_tx, open, stdin) =
                (out_tx.clone(), exit_tx.clone(), open.clone(), stdin.clone());
            async move { upgrade.on_upgrade(move |s| drive(s, out_tx, exit_tx, open, stdin)) }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap()
    });

    let mut child = Command::new(&bin)
        .env("SHIITAKE_WORKER_ID", "test-worker")
        .env(
            "SHIITAKE_DISPATCH_URL",
            format!("ws://127.0.0.1:{}/dispatch", addr.port()),
        )
        .env("SHIITAKE_DISPATCH_TOKEN", rand_token())
        .env("SHIITAKE_CAPTURE_ROOT", capture_root.path())
        .env("PATH", std::env::var("PATH").unwrap())
        .envs(worker_env.iter().copied())
        .spawn()
        .expect("spawn worker binary");

    let output = timeout(Duration::from_secs(20), out_rx.recv())
        .await
        .expect("no pty output within the deadline")
        .expect("output channel closed");
    let exit = timeout(Duration::from_secs(20), exit_rx.recv())
        .await
        .expect("no PtyExit within the deadline")
        .expect("exit channel closed");

    child.kill().await.expect("kill worker");
    let _ = child.wait().await;
    (output, exit)
}

async fn drive(
    socket: WebSocket,
    out_tx: mpsc::Sender<Vec<u8>>,
    exit_tx: mpsc::Sender<Value>,
    open: Value,
    stdin: Option<Vec<u8>>,
) {
    let (mut sink, mut stream) = socket.split();
    match stream.next().await {
        Some(Ok(Message::Text(_))) => {} // Hello
        other => panic!("expected Hello, got {other:?}"),
    }
    sink.send(Message::Text(open.to_string().into()))
        .await
        .unwrap();

    let mut collected: Vec<u8> = Vec::new();
    let mut to_send = stdin;
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(Message::Binary(b)) => {
                collected.extend_from_slice(&b);
                // Feed stdin once the shell has produced its first output.
                if let Some(data) = to_send.take() {
                    sink.send(Message::Binary(data.into())).await.unwrap();
                }
            }
            Ok(Message::Text(t)) => {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v.get("kind").and_then(|k| k.as_str()) == Some("pty_exit") {
                    out_tx.send(collected).await.ok();
                    exit_tx.send(v).await.ok();
                    return;
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }
    out_tx.send(collected).await.ok();
}
