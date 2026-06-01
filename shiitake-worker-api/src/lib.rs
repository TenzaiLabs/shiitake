//! The shared contract between shiitake-server and shiitake-worker: the wire
//! frames (this module) and the on-disk capture layout ([`capture`]). Kept in
//! its own crate so the worker depends only on this, not on the server's full
//! dependency tree (axum, kube, OpenTelemetry, …).

pub mod capture;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Optional privilege-drop directive carried on an Execute frame.
///
/// When present, the worker performs `setgid → setgroups → setuid → umask`
/// in the post-fork `pre_exec` closure (before the bash exec). When absent,
/// the worker runs the command as whatever uid the worker process itself
/// holds. Shiitake is identity-agnostic — embedding layers decide how to
/// populate this (e.g. by mapping an authenticated principal to a uid/gid).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DropTo {
    pub uid: u32,
    pub gid: u32,
    #[serde(default)]
    pub supplementary_gids: Vec<u32>,
    /// Octal umask applied in the child after setuid. `None` keeps the
    /// worker's current umask.
    #[serde(default)]
    pub umask: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Frame {
    /// Worker → server: first frame after connect, advertises availability.
    Hello { worker_id: String },
    /// Server → worker: a command to run.
    Execute(ExecuteFrame),
    /// Server → worker: cancel the in-flight command. Worker SIGKILLs the
    /// process group and follows up with a Result frame whose `cancelled`
    /// flag is set.
    Cancel { request_id: String },
    /// Worker → server: the result of an Execute. The output lives on
    /// disk under `<capture_root>/<request_id>/{stdout,stderr}` — the server
    /// stats those files for byte counts, so only exit metadata and the
    /// per-command resource usage travel on the wire.
    Result(ResultFrame),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteFrame {
    pub request_id: String,
    pub command: String,
    pub working_dir: String,
    pub env: BTreeMap<String, String>,
    pub timeout_secs: f64,
    /// Optional privilege-drop directive. `None` means run as the worker uid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drop_to: Option<DropTo>,
}

/// Per-command resource usage, measured by the worker from its cgroup and
/// reported to the server, which turns it into metrics. Every field is
/// optional/zero on hosts without cgroup v2 (dev macOS, unconstrained
/// containers) so the worker degrades gracefully.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceUsage {
    /// High-water memory usage of the command's cgroup (`memory.peak`).
    pub memory_peak_bytes: Option<u64>,
    /// The cgroup memory limit in effect (`memory.max`).
    pub memory_limit_bytes: Option<u64>,
    /// CPU time the command consumed in user mode (`cpu.stat user_usec`).
    pub cpu_user_seconds: Option<f64>,
    /// CPU time the command consumed in kernel mode (`cpu.stat system_usec`).
    pub cpu_system_seconds: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultFrame {
    pub request_id: String,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
    pub timed_out: bool,
    pub cancelled: bool,
    #[serde(default)]
    pub usage: ResourceUsage,
}

impl ResultFrame {
    #[allow(dead_code)] // server doesn't construct these — worker does
    pub fn errored(request_id: String, message: impl Into<String>) -> Self {
        let _ = message.into();
        Self {
            request_id,
            exit_code: None,
            exit_signal: None,
            timed_out: false,
            cancelled: false,
            usage: ResourceUsage::default(),
        }
    }
}
