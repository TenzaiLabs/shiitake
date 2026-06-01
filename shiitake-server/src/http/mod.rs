//! HTTP API layer — axum router builders and shared state.

pub mod dispatch;
pub mod exec;
pub mod health;

use crate::pool::WorkerPool;
use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::{get, post},
};
use std::{path::PathBuf, sync::Arc};
use tower_http::validate_request::ValidateRequestHeaderLayer;

/// Generic state for shiitake-server's built-in handlers.
///
/// Embedding crates that need richer per-request context (working-directory
/// policy, proxy settings, an identity registry, …) typically embed this in
/// their own state struct and write handlers that take `State<MyState>`. Both
/// `AppState` and the richer state can coexist in the same `Router`.
#[derive(Clone)]
pub struct AppState {
    pub pool: Arc<WorkerPool>,
    /// Bearer token guarding the `/exec` routes (the binary refuses to start
    /// if it is empty). `/health` stays unauthenticated.
    pub auth_token: Arc<String>,
    /// Used when the request body omits `workdir`.
    pub default_workdir: PathBuf,
    /// Maximum accepted request body size, in bytes.
    pub max_body_bytes: usize,
}

/// Default public-facing router. All routes are nested under `/api/v1`:
/// `/api/v1/health`, `/api/v1/exec`, `/api/v1/exec/{handle}*`.
pub fn build_api_router(state: AppState) -> Router {
    // tower-http's `bearer` is marked deprecated ("too basic"), but a static
    // shared secret is exactly our case, so we use it rather than hand-rolling.
    #[allow(deprecated)]
    let exec = Router::new()
        .route("/exec", post(exec::exec))
        .route("/exec/{handle}", get(exec::status).delete(exec::kill))
        .route("/exec/{handle}/stdout", get(exec::read_stdout))
        .route("/exec/{handle}/stderr", get(exec::read_stderr))
        .layer(ValidateRequestHeaderLayer::bearer(
            state.auth_token.as_str(),
        ));
    let v1 = Router::new()
        .route("/health", get(health::health))
        .merge(exec);
    let max_body_bytes = state.max_body_bytes;
    Router::new()
        .nest("/api/v1", v1)
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(state)
}

/// Worker-facing dispatch router — WS upgrade only.
pub fn build_dispatch_router(pool: Arc<WorkerPool>) -> Router {
    Router::new()
        .route("/dispatch", get(dispatch::connect))
        .with_state(pool)
}
