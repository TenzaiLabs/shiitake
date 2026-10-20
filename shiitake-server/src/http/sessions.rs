//! Explicit session lifecycle. A session reserves one worker in a named
//! profile; command handles and a detachable terminal belong to that session.

use super::{AppState, exec};
use crate::pool::{SessionError, SessionSnapshot, TerminalAttachment, TerminalEvent};
use axum::{
    Json,
    extract::{
        Path, Request, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use serde::Deserialize;
use shiitake_server_api::{
    CreateSessionRequest, HandleSnapshotJson, ProfileResponse, SessionCommandRequest,
    SessionResponse, SpawnResponse, StatusResponse,
};
use shiitake_worker_api::{ExecId, Frame, OutputChunk, OutputStream, SessionId, SessionOpenFrame};
use std::time::{Duration, Instant};
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

type HttpError = (StatusCode, String);
const MAX_SESSION_ENV_ENTRIES: usize = 128;
const MAX_SESSION_ENV_BYTES: usize = 64 * 1024;
const MAX_SESSION_WORKDIR_BYTES: usize = 4096;

fn validate_session_request(req: &CreateSessionRequest) -> Result<(), HttpError> {
    if req.profile.is_empty()
        || !req
            .profile
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        || req.cols == 0
        || req.rows == 0
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid profile or terminal size".into(),
        ));
    }
    if req.env.len() > MAX_SESSION_ENV_ENTRIES
        || req.env.iter().fold(0usize, |size, (key, value)| {
            size.saturating_add(key.len()).saturating_add(value.len())
        }) > MAX_SESSION_ENV_BYTES
        || req
            .workdir
            .as_ref()
            .is_some_and(|path| path.len() > MAX_SESSION_WORKDIR_BYTES)
    {
        return Err((StatusCode::BAD_REQUEST, "session input is too large".into()));
    }
    Ok(())
}

/// Public ids use 80 random bits in a short, unambiguous lowercase alphabet.
/// UUID v4 supplies the random bytes; skip its version and variant bytes so
/// every encoded bit comes from the random portion.
fn new_public_id(kind: char) -> String {
    let uuid = Uuid::new_v4();
    let bytes = uuid.as_bytes();
    encode_public_id(
        kind,
        [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[7], bytes[9],
            bytes[10], bytes[11],
        ],
    )
}

fn encode_public_id(kind: char, bytes: [u8; 10]) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";
    let value = bytes
        .into_iter()
        .fold(0u128, |value, byte| (value << 8) | u128::from(byte));
    let mut id = String::with_capacity(18);
    id.push(kind);
    id.push('-');
    for shift in (0..16).rev().map(|n| n * 5) {
        id.push(ALPHABET[((value >> shift) & 31) as usize] as char);
    }
    id
}

fn map_error(error: SessionError, action: &'static str) -> HttpError {
    if let SessionError::Worker(details) = error {
        tracing::warn!(%details, action, "worker request failed");
        return (
            StatusCode::BAD_GATEWAY,
            format!("worker could not {action}"),
        );
    }
    let status = match error {
        SessionError::UnknownProfile | SessionError::NotFound => StatusCode::NOT_FOUND,
        SessionError::Busy => StatusCode::TOO_MANY_REQUESTS,
        SessionError::NotActive | SessionError::TerminalBusy => StatusCode::CONFLICT,
        SessionError::Worker(_) => unreachable!(),
    };
    (status, error.to_string())
}

fn response(s: SessionSnapshot) -> SessionResponse {
    SessionResponse {
        session_id: s.session_id,
        worker_id: s.worker_id,
        profile: s.profile,
        state: s.state,
        created_at: exec::to_epoch(s.created_at),
    }
}

pub async fn profiles(State(state): State<AppState>) -> Json<Vec<ProfileResponse>> {
    Json(state.pool.profiles().await)
}

pub async fn create(
    State(state): State<AppState>,
    Json(req): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<SessionResponse>), HttpError> {
    validate_session_request(&req)?;
    let id = SessionId::new(new_public_id('s'));
    let open = SessionOpenFrame {
        session_id: id,
        working_dir: req
            .workdir
            .unwrap_or_else(|| state.default_workdir.to_string_lossy().into_owned()),
        env: req.env,
        drop_to: req.drop_to,
        cols: req.cols,
        rows: req.rows,
    };
    let snap = state
        .pool
        .create_session(req.profile, open)
        .await
        .map_err(|e| map_error(e, "start session"))?;
    Ok((StatusCode::CREATED, Json(response(snap))))
}

pub async fn status(
    State(state): State<AppState>,
    Path(id): Path<SessionId>,
) -> Result<Json<SessionResponse>, HttpError> {
    let snap = state
        .pool
        .session_snapshot(&id)
        .await
        .ok_or_else(|| map_error(SessionError::NotFound, "find session"))?;
    Ok(Json(response(snap)))
}

pub async fn close(
    State(state): State<AppState>,
    Path(id): Path<SessionId>,
) -> Result<Json<SessionResponse>, HttpError> {
    let snap = state
        .pool
        .close_session(&id)
        .await
        .map_err(|e| map_error(e, "close session"))?;
    Ok(Json(response(snap)))
}

pub async fn run(
    State(state): State<AppState>,
    Path(id): Path<SessionId>,
    Json(req): Json<SessionCommandRequest>,
) -> Result<(StatusCode, Json<SpawnResponse>), HttpError> {
    let rid = ExecId::new(new_public_id('c'));
    let snap = state
        .pool
        .submit_session_command(&id, rid, req.command, req.mode)
        .await
        .map_err(|e| map_error(e, "submit command"))?;
    Ok((
        StatusCode::ACCEPTED,
        Json(SpawnResponse {
            handle: snap.handle_id,
            started_at: exec::to_epoch(snap.started_at),
        }),
    ))
}

pub async fn list_commands(
    State(state): State<AppState>,
    Path(id): Path<SessionId>,
) -> Result<Json<Vec<StatusResponse>>, HttpError> {
    let handles = state
        .pool
        .list_session_commands(&id)
        .await
        .ok_or((StatusCode::NOT_FOUND, "unknown session".into()))?;
    Ok(Json(
        handles
            .into_iter()
            .map(|snap| exec::status_response(&state, snap))
            .collect(),
    ))
}

async fn check_handle(state: &AppState, id: &SessionId, handle: &ExecId) -> Result<(), HttpError> {
    let snap = state
        .pool
        .touch_session_handle(id, handle)
        .await
        .ok_or((StatusCode::NOT_FOUND, "unknown command".into()))?;
    debug_assert_eq!(snap.session_id.as_ref(), Some(id));
    Ok(())
}

pub async fn command_status(
    State(state): State<AppState>,
    Path((id, handle)): Path<(SessionId, ExecId)>,
) -> Result<Json<StatusResponse>, HttpError> {
    check_handle(&state, &id, &handle).await?;
    exec::status(State(state), Path(handle)).await
}

pub async fn cancel(
    State(state): State<AppState>,
    Path((id, handle)): Path<(SessionId, ExecId)>,
) -> Result<Json<HandleSnapshotJson>, HttpError> {
    check_handle(&state, &id, &handle).await?;
    exec::kill(State(state), Path(handle)).await
}

pub async fn stdout(
    State(state): State<AppState>,
    Path((id, handle)): Path<(SessionId, ExecId)>,
    req: Request,
) -> Response {
    if let Err(e) = check_handle(&state, &id, &handle).await {
        return e.into_response();
    }
    exec::read_stdout(State(state), Path(handle), req).await
}

pub async fn stderr(
    State(state): State<AppState>,
    Path((id, handle)): Path<(SessionId, ExecId)>,
    req: Request,
) -> Response {
    if let Err(e) = check_handle(&state, &id, &handle).await {
        return e.into_response();
    }
    exec::read_stderr(State(state), Path(handle), req).await
}

pub async fn terminal(
    State(state): State<AppState>,
    Path(id): Path<SessionId>,
    ws: WebSocketUpgrade,
) -> Result<Response, HttpError> {
    let keepalive = state.pty_keepalive;
    let attachment = state
        .pool
        .attach_terminal(&id)
        .await
        .map_err(|e| map_error(e, "attach terminal"))?;
    Ok(ws.on_upgrade(move |socket| serve_terminal(socket, attachment, keepalive)))
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum TerminalControl {
    Resize { cols: u16, rows: u16 },
}

fn terminal_resize(text: &str) -> Result<(u16, u16), ()> {
    match serde_json::from_str::<TerminalControl>(text) {
        Ok(TerminalControl::Resize { cols, rows }) if cols > 0 && rows > 0 => Ok((cols, rows)),
        Ok(_) | Err(_) => Err(()),
    }
}

async fn send_terminal_client(
    client: &mut SplitSink<WebSocket, Message>,
    frame: Message,
    limit: Duration,
) -> bool {
    matches!(
        tokio::time::timeout(limit, client.send(frame)).await,
        Ok(Ok(()))
    )
}

async fn serve_terminal(
    socket: WebSocket,
    mut attachment: TerminalAttachment,
    keepalive: Duration,
) {
    let id = attachment.session_id.clone();
    let writer = attachment.writer();
    let (mut client_tx, mut client_rx) = socket.split();
    let ping_every = keepalive.max(Duration::from_secs(1));
    let max_silence = ping_every.saturating_mul(3);
    let mut ping = tokio::time::interval(ping_every);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_client_frame = Instant::now();
    loop {
        tokio::select! {
            _ = ping.tick() => {
                if last_client_frame.elapsed() >= max_silence {
                    tracing::warn!(%id, "terminal client stopped responding");
                    let _ = send_terminal_client(&mut client_tx, Message::Close(Some(CloseFrame {
                        code: 1001, reason: "terminal connection timed out".into(),
                    })), max_silence).await;
                    break;
                }
                if !send_terminal_client(&mut client_tx, Message::Ping(Vec::new().into()), max_silence).await { break; }
            }
            msg = client_rx.next() => match msg {
                Some(Ok(Message::Binary(b))) => {
                    last_client_frame = Instant::now();
                    let chunk = OutputChunk { stream: OutputStream::Stdin,
                        id: id.as_str(), bytes: &b };
                    if writer.lock().await.send(Message::Binary(chunk.encode().into())).await.is_err() { break; }
                }
                Some(Ok(Message::Text(t))) => {
                    last_client_frame = Instant::now();
                    let (cols, rows) = match terminal_resize(&t) {
                        Ok(size) => size,
                        Err(()) => {
                            tracing::warn!("invalid terminal control frame");
                            let _ = send_terminal_client(&mut client_tx, Message::Close(Some(CloseFrame {
                                code: 1007, reason: "invalid terminal control".into(),
                            })), max_silence).await;
                            break;
                        }
                    };
                    let frame = Frame::SessionResize { session_id: id.clone(), cols, rows };
                    match serde_json::to_string(&frame) {
                        Ok(json) => {
                            if writer.lock().await.send(Message::Text(json.into())).await.is_err() {
                                break;
                            }
                        }
                        Err(error) => {
                            tracing::error!(%error, "could not encode terminal resize frame");
                            let _ = send_terminal_client(&mut client_tx, Message::Close(Some(CloseFrame {
                                code: 1011, reason: "terminal control failed".into(),
                            })), max_silence).await;
                            break;
                        }
                    }
                }
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {
                    last_client_frame = Instant::now();
                }
                _ => break,
            },
            data = attachment.recv() => match data {
                Some(TerminalEvent::Output(bytes)) => {
                    if !send_terminal_client(&mut client_tx, Message::Binary(bytes.into()), max_silence).await { break; }
                }
                Some(TerminalEvent::Closed(error)) => {
                    let _ = send_terminal_client(&mut client_tx, Message::Close(Some(terminal_close(error))), max_silence).await;
                    break;
                }
                None => {
                    let _ = send_terminal_client(&mut client_tx, Message::Close(Some(CloseFrame {
                        code: 1011, reason: "worker disconnected".into(),
                    })), max_silence).await;
                    break;
                },
            }
        }
    }
    attachment.detach().await;
}

fn terminal_close(error: Option<String>) -> CloseFrame {
    match error {
        Some(details) => {
            tracing::warn!(%details, "terminal session ended with worker error");
            CloseFrame {
                code: 1011,
                reason: "session ended unexpectedly".into(),
            }
        }
        None => CloseFrame {
            code: 1000,
            reason: "session ended".into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_request_bounds_retained_input() {
        let mut req = CreateSessionRequest {
            profile: "default".into(),
            workdir: None,
            env: Default::default(),
            drop_to: None,
            cols: 80,
            rows: 24,
        };
        req.env
            .insert("ONE".into(), "x".repeat(MAX_SESSION_ENV_BYTES));
        assert_eq!(
            validate_session_request(&req).unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
        req.env.clear();
        for index in 0..=MAX_SESSION_ENV_ENTRIES {
            req.env.insert(format!("KEY_{index}"), "x".into());
        }
        assert_eq!(
            validate_session_request(&req).unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
        req.env.clear();
        req.workdir = Some("/".repeat(MAX_SESSION_WORKDIR_BYTES + 1));
        assert_eq!(
            validate_session_request(&req).unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn terminal_close_hides_worker_error_details() {
        let close = terminal_close(Some("private path /root/session-state".into()));
        assert_eq!(close.code, 1011);
        assert_eq!(close.reason.as_str(), "session ended unexpectedly");
        let normal = terminal_close(None);
        assert_eq!(normal.code, 1000);
        assert_eq!(normal.reason.as_str(), "session ended");
    }

    #[test]
    fn terminal_resize_rejects_invalid_client_frames() {
        assert_eq!(
            terminal_resize(r#"{"op":"resize","cols":80,"rows":24}"#),
            Ok((80, 24))
        );
        for frame in [
            "not json",
            r#"{"op":"unknown","cols":80,"rows":24}"#,
            r#"{"op":"resize","cols":0,"rows":24}"#,
            r#"{"op":"resize","cols":80,"rows":24,"extra":1}"#,
        ] {
            assert!(terminal_resize(frame).is_err(), "accepted {frame}");
        }
    }

    #[test]
    fn public_ids_are_short_and_unambiguous() {
        assert_eq!(encode_public_id('s', [0; 10]), "s-0000000000000000");
        assert_eq!(encode_public_id('c', [255; 10]), "c-zzzzzzzzzzzzzzzz");
        for kind in ['s', 'c'] {
            let id = new_public_id(kind);
            assert_eq!(id.len(), 18);
            assert!(id.starts_with(&format!("{kind}-")));
            assert!(
                id[2..]
                    .bytes()
                    .all(|byte| b"0123456789abcdefghjkmnpqrstvwxyz".contains(&byte))
            );
        }
    }

    #[test]
    fn worker_errors_hide_details_and_name_the_failed_operation() {
        let (status, body) = map_error(
            SessionError::Worker("/private/path: permission denied".into()),
            "submit command",
        );
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body, "worker could not submit command");
    }
}
