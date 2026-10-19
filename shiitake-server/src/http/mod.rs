//! HTTP API layer — axum router builders and shared state.

pub mod dispatch;
pub mod exec;
pub mod health;
pub mod pty;
pub mod sessions;

use crate::pool::WorkerPool;
use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::{get, post},
};
use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
    sync::Arc,
};
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
    /// Bearer token guarding session routes (the binary refuses to start
    /// if it is empty). `/health` and `/ready` stay unauthenticated.
    pub auth_token: Arc<String>,
    /// Used when the request body omits `workdir`.
    pub default_workdir: PathBuf,
    /// Maximum accepted request body size, in bytes.
    pub max_body_bytes: usize,
    /// Registered workers (idle + in-flight) the pool needs before `/ready`
    /// reports ready. `1` is the useful minimum; raise it to keep a large
    /// deployment out of rotation until some fraction of its pool is back.
    pub min_ready_workers: usize,
    /// How often the `/pty` handler pings a pinned worker to keep its session
    /// lease fresh while a client is attached (below the worker's lease).
    pub pty_keepalive: std::time::Duration,
}

/// Default public-facing router. All routes are nested under `/api/v1`:
/// `/api/v1/health`, `/api/v1/ready`, `/api/v1/profiles`, `/api/v1/sessions`.
pub fn build_api_router(state: AppState) -> Router {
    // tower-http's `bearer` is marked deprecated ("too basic"), but a static
    // shared secret is exactly our case, so we use it rather than hand-rolling.
    #[allow(deprecated)]
    let protected = Router::new()
        .route("/profiles", get(sessions::profiles))
        .route("/sessions", post(sessions::create))
        .route(
            "/sessions/{id}",
            get(sessions::status).delete(sessions::close),
        )
        .route("/sessions/{id}/commands", post(sessions::run))
        .route(
            "/sessions/{id}/commands/{handle}",
            get(sessions::command_status).delete(sessions::cancel),
        )
        .route(
            "/sessions/{id}/commands/{handle}/stdout",
            get(sessions::stdout),
        )
        .route(
            "/sessions/{id}/commands/{handle}/stderr",
            get(sessions::stderr),
        )
        .route("/sessions/{id}/terminal", get(sessions::terminal))
        .layer(ValidateRequestHeaderLayer::bearer(
            state.auth_token.as_str(),
        ));
    let v1 = Router::new()
        .route("/health", get(health::health))
        .route("/ready", get(health::ready))
        .merge(protected);
    let max_body_bytes = state.max_body_bytes;
    Router::new()
        .nest("/api/v1", v1)
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(state)
}

/// Worker-facing dispatch router — WS upgrade only, bearer-authenticated.
///
/// The token is checked on the upgrade request, before the WebSocket exists, so
/// an unauthenticated peer never reaches the frame loop. Separate from the API's
/// `auth_token`: the dispatch path may cross pods, where being loopback-bound is
/// no longer what protects it.
pub fn build_dispatch_router(pool: Arc<WorkerPool>, dispatch_token: &str) -> Router {
    build_dispatch_router_with_profiles(pool, dispatch_token, BTreeMap::new())
        .expect("default dispatch token must be non-empty")
}

/// Dispatch router that binds each worker credential to one profile. Use this
/// for deployments with more than one profile, especially root workers.
pub fn build_dispatch_router_with_profiles(
    pool: Arc<WorkerPool>,
    default_token: &str,
    extra_tokens: BTreeMap<String, String>,
) -> anyhow::Result<Router> {
    anyhow::ensure!(
        !default_token.is_empty(),
        "default dispatch token is required"
    );
    let mut seen = HashSet::from([default_token]);
    for (profile, token) in &extra_tokens {
        anyhow::ensure!(
            profile != "default"
                && !profile.is_empty()
                && profile
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "invalid additional profile name: {profile}"
        );
        anyhow::ensure!(
            !token.is_empty() && seen.insert(token.as_str()),
            "each profile needs a distinct non-empty dispatch token"
        );
    }
    let mut tokens = BTreeMap::from([("default".to_string(), default_token.to_string())]);
    tokens.extend(extra_tokens);
    Ok(Router::new()
        .route("/dispatch", get(dispatch::connect_scoped))
        .with_state(dispatch::ProfileCredentials {
            pool,
            tokens: Arc::new(tokens),
        }))
}
