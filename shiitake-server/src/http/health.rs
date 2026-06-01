use super::AppState;
use axum::{Json, extract::State};
use serde::Serialize;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub service: &'static str,
    pub workers_idle: usize,
    pub workers_inflight: usize,
}

pub async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let (idle, inflight) = state.pool.snapshot().await;
    Json(HealthResponse {
        status: "ok",
        service: "shiitake",
        workers_idle: idle,
        workers_inflight: inflight,
    })
}
