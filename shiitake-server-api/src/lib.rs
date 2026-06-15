//! Request/response types for the shiitake HTTP API (`/api/v1`) — the contract
//! between the server and any client. Shared by the server (which serializes
//! responses / deserializes requests) and clients (the reverse), so every type
//! is both `Serialize` and `Deserialize`. Pure types, no transport.

use serde::{Deserialize, Serialize};
// `DropTo` is part of the `/exec` request body (and the on-the-wire frame), so
// re-export it for callers building requests against this API. `WorkerId`
// likewise appears on `StatusResponse`.
pub use shiitake_worker_api::{DropTo, WorkerId};
use std::collections::BTreeMap;
use strum::IntoStaticStr;

fn default_timeout() -> f64 {
    300.0
}

/// `POST /api/v1/exec` request body. The command is run as `bash -c <command>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecRequest {
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    #[serde(default = "default_timeout")]
    pub timeout: f64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drop_to: Option<DropTo>,
}

/// `202` response to `POST /api/v1/exec` — a handle to a freshly-spawned
/// command. No output or exit code yet; the command is running.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnResponse {
    pub handle: String,
    pub started_at: f64,
}

/// The status of a handle, as reported by `GET /api/v1/exec/{handle}`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HandleStatus {
    Running,
    Completed,
    Timeout,
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
    Timeout,
    WorkerDied,
    Cancelled,
}

/// `GET /api/v1/exec/{handle}` response. Output is read separately, by byte
/// range, from `/api/v1/exec/{handle}/{stdout,stderr}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub handle: String,
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
    pub timed_out: bool,
    #[serde(default)]
    pub cancelled: bool,
    #[serde(default)]
    pub stdout_bytes_written: u64,
    #[serde(default)]
    pub stderr_bytes_written: u64,
}

/// `DELETE /api/v1/exec/{handle}` response — the terminal snapshot after the
/// process group was killed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleSnapshotJson {
    pub handle: String,
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
}
