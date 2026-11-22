//! `/health` and `/ready` — the two probes, deliberately separate.
//!
//! Liveness (`/health`) answers "is this process up", so it succeeds as long
//! as the server can serve at all; an empty pool is not a reason to restart
//! it. Readiness (`/ready`) answers "can this pod serve a command", so its
//! status code reflects the pool: a deployment with no worker registered is
//! not sent traffic it would only reject.

use super::AppState;
use axum::{Json, extract::State, http::StatusCode};
use shiitake_server_api::{HealthResponse, ReadyResponse};

const SERVICE: &str = "shiitake";

pub async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let (idle, inflight) = state.pool.snapshot().await;
    Json(HealthResponse {
        status: "ok".into(),
        service: SERVICE.into(),
        workers_idle: idle,
        workers_inflight: inflight,
        workers_interactive: state.pool.interactive_count().await,
        workers_sessions: state.pool.session_count().await,
    })
}

/// Ready once at least `min_ready_workers` default-profile workers are live:
/// each has answered since it registered and is not silent on a ping. A
/// worker counts whether it is idle or in-flight — a busy pool is still able
/// to serve, and gating on idle alone would pull a pod out of rotation exactly
/// when it is doing the most work. Workers of additional profiles do not
/// count: they cannot serve a default-profile session, and a caller that
/// starts on their registration alone finds no worker to use. Neither does a
/// worker that has only registered, because its connection may be dead.
pub async fn ready(State(state): State<AppState>) -> (StatusCode, Json<ReadyResponse>) {
    let (idle, inflight) = state
        .pool
        .live_snapshot(&shiitake_worker_api::default_profile())
        .await;
    let ready = idle + inflight >= state.min_ready_workers;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(ReadyResponse {
            ready,
            service: SERVICE.into(),
            workers_idle: idle,
            workers_inflight: inflight,
            workers_sessions: state.pool.session_count().await,
            workers_required: state.min_ready_workers,
        }),
    )
}
