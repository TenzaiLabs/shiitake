//! Request/response types for the shiitake HTTP API (`/api/v1`) — the contract
//! between the server and any client. Shared by the server (which serializes
//! responses / deserializes requests) and clients (the reverse), so every type
//! is both `Serialize` and `Deserialize`. Pure types, no transport.

use serde::{Deserialize, Serialize};
// `DropTo` is part of session creation and the worker wire frame. `WorkerId`
// appears on status and session responses.
pub use shiitake_worker_api::{DropTo, ExecId, SessionCommandMode, SessionId, WorkerId};
use std::collections::BTreeMap;
use strum::IntoStaticStr;

/// Legacy internal request type for the removed `/exec` route.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecRequest {
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drop_to: Option<DropTo>,
}

/// Allocate a worker from a profile and start its persistent shell. The
/// identity, environment and initial directory are fixed for the session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionRequest {
    #[serde(default = "shiitake_worker_api::default_profile")]
    pub profile: String,
    #[serde(default)]
    pub workdir: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub drop_to: Option<DropTo>,
    #[serde(default = "default_cols")]
    pub cols: u16,
    #[serde(default = "default_rows")]
    pub rows: u16,
}

fn default_cols() -> u16 {
    80
}
fn default_rows() -> u16 {
    24
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionResponse {
    pub session_id: SessionId,
    pub worker_id: WorkerId,
    pub profile: String,
    pub state: SessionState,
    pub created_at: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Opening,
    Active,
    Closing,
    Closed,
    Failed,
}

/// Submit one job to a session. Jobs inherit its identity and initial
/// environment/directory; several jobs may run simultaneously.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCommandRequest {
    pub command: String,
    #[serde(default)]
    pub mode: SessionCommandMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileResponse {
    pub name: String,
    pub idle: usize,
    pub allocated: usize,
}

/// `202` response to `POST /api/v1/sessions/{id}/commands`. The command is
/// running; poll its handle for completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnResponse {
    pub handle: ExecId,
    pub started_at: f64,
}

/// The status of a session command handle.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HandleStatus {
    Running,
    Completed,
    Oomkilled,
    Error,
}

/// Why a handle is no longer running. `IntoStaticStr` gives a `&'static str`
/// view (`(&cause).into()`) for span/metric labels; `serialize_all` matches
/// serde's `rename_all`, so those labels are the snake_case strings on the wire.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, IntoStaticStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ExitCause {
    Normal,
    Signal,
    OomContainer,
    WorkerDied,
    WorkerError,
    OutputError,
    Cancelled,
}

/// Command status response, also returned in the array from
/// `GET /api/v1/sessions/{id}/commands`. Output is read separately by byte
/// range from `/api/v1/sessions/{id}/commands/{handle}/{stdout,stderr}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub handle: ExecId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    pub worker_id: WorkerId,
    pub status: HandleStatus,
    pub started_at: f64,
    #[serde(default)]
    pub finished_at: Option<f64>,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub exit_cause: Option<ExitCause>,
    #[serde(default)]
    pub exit_signal: Option<i32>,
    #[serde(default)]
    pub cancelled: bool,
    #[serde(default)]
    pub stdout_bytes_written: u64,
    #[serde(default)]
    pub stderr_bytes_written: u64,
}

/// `DELETE /api/v1/sessions/{id}/commands/{handle}` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleSnapshotJson {
    pub handle: ExecId,
    pub status: HandleStatus,
    #[serde(default)]
    pub exit_cause: Option<ExitCause>,
    #[serde(default)]
    pub cancelled: bool,
}

/// `GET /api/v1/health` response, including a pool snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
    #[serde(default)]
    pub workers_idle: usize,
    #[serde(default)]
    pub workers_inflight: usize,
    /// Workers held out of the pool by an interactive session (a pinned PTY).
    /// Lets operators — and an idle reaper — see capacity a terminal is holding.
    #[serde(default)]
    pub workers_interactive: usize,
    #[serde(default)]
    pub workers_sessions: usize,
}

/// `GET /api/v1/ready` response — readiness, as distinct from liveness. The
/// HTTP status carries the same verdict as `ready` (`200` when ready, `503`
/// when not), so an orchestrator probe can gate on the status code alone.
///
/// A worker is *registered* whether it is idle or serving a command
/// (`workers_idle + workers_inflight`); readiness is that count against
/// `workers_required`, so a fully-busy pool stays ready.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyResponse {
    pub ready: bool,
    pub service: String,
    #[serde(default)]
    pub workers_idle: usize,
    #[serde(default)]
    pub workers_inflight: usize,
    #[serde(default)]
    pub workers_sessions: usize,
    /// Registered workers the pool must have to report ready
    /// (`SHIITAKE_MIN_READY_WORKERS`).
    #[serde(default)]
    pub workers_required: usize,
}
