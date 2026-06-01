//! `/exec` — fire-and-forget command execution.
//!
//! POST /exec returns a handle the moment a worker has accepted the
//! command. Clients then poll GET /exec/{handle} and read output via
//! /exec/{handle}/stdout|stderr, which serve the capture files with HTTP
//! range support. The server never holds command output in memory.

use super::AppState;
use crate::pool::{DispatchError, HandleSnapshot};
use axum::{
    Json,
    extract::{Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use shiitake_server_api::{ExecRequest, HandleSnapshotJson, SpawnResponse, StatusResponse};
use shiitake_worker_api::{
    ExecuteFrame,
    capture::{self, Stream},
};
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;
use tower_http::services::ServeFile;
use uuid::Uuid;

/// Default `/exec` handler. Builds an ExecuteFrame from the request body
/// and dispatches it. Downstream crates that need to mutate env / inject
/// headers can call [`dispatch`] directly.
pub async fn exec(
    State(state): State<AppState>,
    Json(req): Json<ExecRequest>,
) -> Result<(StatusCode, Json<SpawnResponse>), (StatusCode, String)> {
    let workdir = req
        .workdir
        .unwrap_or_else(|| state.default_workdir.to_string_lossy().into_owned());
    let frame = ExecuteFrame {
        request_id: Uuid::new_v4().simple().to_string(),
        command: req.command,
        working_dir: workdir,
        env: req.env,
        timeout_secs: req.timeout,
        drop_to: req.drop_to,
    };
    let snap = dispatch(&state, frame).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(SpawnResponse {
            handle: snap.handle_id,
            started_at: to_epoch(snap.started_at),
        }),
    ))
}

/// Dispatch an already-built ExecuteFrame. Translates the pool's
/// DispatchError into HTTP status codes.
pub async fn dispatch(
    state: &AppState,
    frame: ExecuteFrame,
) -> Result<HandleSnapshot, (StatusCode, String)> {
    match state.pool.dispatch(frame).await {
        Ok(snap) => Ok(snap),
        Err(DispatchError::NoIdleWorker) => Err((
            StatusCode::TOO_MANY_REQUESTS,
            "all workers busy; retry shortly".into(),
        )),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("dispatch: {e}"))),
    }
}

pub async fn status(
    State(state): State<AppState>,
    Path(handle): Path<String>,
) -> Result<Json<StatusResponse>, (StatusCode, String)> {
    let snap = state
        .pool
        .touch_and_snapshot(&handle)
        .await
        .ok_or((StatusCode::NOT_FOUND, "unknown handle".into()))?;
    let root = state.pool.capture_root();
    Ok(Json(StatusResponse {
        handle: snap.handle_id.clone(),
        worker_id: snap.worker_id,
        status: snap.status,
        started_at: to_epoch(snap.started_at),
        finished_at: snap.finished_at.map(to_epoch),
        exit_code: snap.exit_code,
        exit_cause: snap.exit_cause,
        exit_signal: snap.exit_signal,
        timed_out: snap.timed_out,
        cancelled: snap.cancelled,
        stdout_bytes_written: capture::stream_len(root, &snap.handle_id, Stream::Stdout).await,
        stderr_bytes_written: capture::stream_len(root, &snap.handle_id, Stream::Stderr).await,
    }))
}

/// Serve the captured stdout file. Honours HTTP `Range` requests
/// (`206 Partial Content` / `416`) via tower-http, so clients tail the
/// growing output with `Range: bytes=N-` or fetch the last N bytes with a
/// suffix range — no custom offset protocol.
pub async fn read_stdout(
    State(state): State<AppState>,
    Path(handle): Path<String>,
    req: Request,
) -> Response {
    read_stream(state, handle, req, Stream::Stdout).await
}

pub async fn read_stderr(
    State(state): State<AppState>,
    Path(handle): Path<String>,
    req: Request,
) -> Response {
    read_stream(state, handle, req, Stream::Stderr).await
}

async fn read_stream(state: AppState, handle: String, req: Request, stream: Stream) -> Response {
    if state.pool.touch_and_snapshot(&handle).await.is_none() {
        return (StatusCode::NOT_FOUND, "unknown handle").into_response();
    }
    let path = capture::stream_path(state.pool.capture_root(), &handle, stream);
    // The worker creates both files at exec start, so a known handle with no
    // file means nothing was captured — serve an empty body rather than 404.
    if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
        return (StatusCode::OK, "").into_response();
    }
    match ServeFile::new(path).oneshot(req).await {
        Ok(res) => res.map(axum::body::Body::new).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("serve: {e}")).into_response(),
    }
}

pub async fn kill(
    State(state): State<AppState>,
    Path(handle): Path<String>,
) -> Result<Json<HandleSnapshotJson>, (StatusCode, String)> {
    let snap = state
        .pool
        .cancel(&handle)
        .await
        .ok_or((StatusCode::NOT_FOUND, "unknown handle".into()))?;
    Ok(Json(HandleSnapshotJson {
        handle: snap.handle_id,
        status: snap.status,
        exit_cause: snap.exit_cause,
        cancelled: snap.cancelled,
    }))
}

pub fn to_epoch(t: SystemTime) -> f64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}
