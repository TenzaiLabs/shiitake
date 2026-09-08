//! `/pty` — an interactive terminal over a WebSocket.
//!
//! Bearer-gated like `/exec`. The client sends one JSON `open` control frame
//! (shell argv, cwd, env, size, identity); the server pins an idle worker, opens
//! a pty on it, and splices the sockets — client binary is stdin, worker output
//! is client binary, and `{"op":"resize"}` reflows the tty. The session ends
//! when the shell exits, the client disconnects, or no worker is idle.

use crate::http::AppState;
use crate::pool::{PtyEvent, SharedSink};
use axum::{
    extract::{
        State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use serde::Deserialize;
use shiitake_worker_api::{DropTo, ExecId, Frame, PtyOpenFrame};
use std::collections::BTreeMap;
use std::time::Duration;
use uuid::Uuid;

// WebSocket close codes (RFC 6455).
const CLOSE_NORMAL: u16 = 1000;
const CLOSE_INTERNAL: u16 = 1011;
const CLOSE_POLICY: u16 = 1008;
const CLOSE_TRY_AGAIN: u16 = 1013;

/// Client → server control frames. Binary frames are raw stdin; these JSON
/// frames carry setup and resize.
#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum ClientControl {
    /// First frame: open the shell. Empty `command` = the worker's default.
    Open {
        #[serde(default)]
        command: Vec<String>,
        working_dir: String,
        #[serde(default)]
        env: BTreeMap<String, String>,
        cols: u16,
        rows: u16,
        #[serde(default)]
        drop_to: Option<DropTo>,
    },
    Resize {
        cols: u16,
        rows: u16,
    },
}

struct OpenParams {
    command: Vec<String>,
    working_dir: String,
    env: BTreeMap<String, String>,
    cols: u16,
    rows: u16,
    drop_to: Option<DropTo>,
}

pub async fn pty(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| serve(state, socket))
}

async fn serve(state: AppState, socket: WebSocket) {
    let (mut client_tx, mut client_rx) = socket.split();

    // 1. The first client frame must be `open`.
    let Some(open) = first_open(&mut client_rx).await else {
        close(&mut client_tx, CLOSE_POLICY, "expected an open frame").await;
        return;
    };

    // 2. Pin an idle worker.
    let Some(mut session) = state.pool.acquire_pinned().await else {
        close(&mut client_tx, CLOSE_TRY_AGAIN, "no idle worker; retry").await;
        return;
    };
    let worker_sink = session.writer();
    let session_id = ExecId::new(Uuid::new_v4().simple().to_string());

    // 3. Open the pty on the worker.
    let open_frame = Frame::PtyOpen(PtyOpenFrame {
        session_id: session_id.clone(),
        command: open.command,
        working_dir: open.working_dir,
        env: open.env,
        cols: open.cols,
        rows: open.rows,
        drop_to: open.drop_to,
    });
    if send_frame(&worker_sink, &open_frame).await.is_err() {
        close(&mut client_tx, CLOSE_INTERNAL, "worker unreachable").await;
        state.pool.release_pinned(session).await;
        return;
    }

    // 4. Splice client ⇄ worker until either side ends. The two recv futures
    // borrow only `client_rx` and `session`; each arm writes to a *separate*
    // handle (`worker_sink` / `client_tx`), so there is no borrow conflict.
    let mut keepalive = tokio::time::interval(state.pty_keepalive);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = keepalive.tick() => {
                // Keep the worker's session lease alive while the client is here;
                // the worker resets it on any server frame. A quiet session (no
                // keystrokes, output-only, or idle) must not be reaped as if the
                // link were dead — only a *gone* server (pings stop) should be.
                if worker_sink.lock().await.send(Message::Ping(Vec::new().into())).await.is_err() {
                    break;
                }
            }
            cmsg = client_rx.next() => match cmsg {
                Some(Ok(Message::Binary(b))) => {
                    if worker_sink.lock().await.send(Message::Binary(b)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Text(t))) => {
                    if let Ok(ClientControl::Resize { cols, rows }) =
                        serde_json::from_str::<ClientControl>(&t)
                    {
                        let frame = Frame::PtyResize {
                            session_id: session_id.clone(),
                            cols,
                            rows,
                        };
                        let _ = send_frame(&worker_sink, &frame).await;
                    }
                }
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                _ => break, // Close, None, or error: client gone
            },
            ev = session.recv() => match ev {
                Some(PtyEvent::Output(b)) => {
                    if client_tx.send(Message::Binary(b.into())).await.is_err() {
                        break;
                    }
                }
                Some(PtyEvent::Exit { error, .. }) => {
                    // A clean shell exit (any exit code) is a normal close; an
                    // `error` means the pty failed to open or the worker dropped,
                    // which the client should see as an abnormal end.
                    let (code, reason) = match error {
                        None => (CLOSE_NORMAL, "session ended".to_string()),
                        Some(e) => (CLOSE_INTERNAL, e),
                    };
                    close(&mut client_tx, code, &reason).await;
                    state.pool.release_pinned(session).await;
                    return;
                }
                None => {
                    // Backstop: the session channel closed with no Exit. Tell the
                    // client cleanly instead of dropping the socket silently.
                    close(&mut client_tx, CLOSE_INTERNAL, "session ended unexpectedly").await;
                    state.pool.release_pinned(session).await;
                    return;
                }
            }
        }
    }

    // 5. Client left (or the worker socket died): hang up the shell and let the
    // worker reset before we hand it back, so the next caller never gets a dirty
    // sandbox.
    let _ = send_frame(&worker_sink, &Frame::PtyClose { session_id }).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(ev) = session.recv().await {
            if matches!(ev, PtyEvent::Exit { .. }) {
                break;
            }
        }
    })
    .await;
    state.pool.release_pinned(session).await;
}

/// Read control frames until the opening `open`, or give up (timeout, a wrong
/// first frame, or the socket closing).
async fn first_open(client_rx: &mut SplitStream<WebSocket>) -> Option<OpenParams> {
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            () = &mut deadline => return None,
            msg = client_rx.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    return match serde_json::from_str::<ClientControl>(&t) {
                        Ok(ClientControl::Open {
                            command,
                            working_dir,
                            env,
                            cols,
                            rows,
                            drop_to,
                        }) => Some(OpenParams {
                            command,
                            working_dir,
                            env,
                            cols,
                            rows,
                            drop_to,
                        }),
                        _ => None,
                    };
                }
                Some(Ok(Message::Ping(_)))
                | Some(Ok(Message::Pong(_)))
                | Some(Ok(Message::Binary(_))) => continue,
                _ => return None,
            }
        }
    }
}

async fn send_frame(sink: &SharedSink, frame: &Frame) -> Result<(), axum::Error> {
    let json = serde_json::to_string(frame).map_err(axum::Error::new)?;
    sink.lock().await.send(Message::Text(json.into())).await
}

async fn close(client_tx: &mut SplitSink<WebSocket, Message>, code: u16, reason: &str) {
    let frame = CloseFrame {
        code,
        reason: reason.to_string().into(),
    };
    let _ = client_tx.send(Message::Close(Some(frame))).await;
}
