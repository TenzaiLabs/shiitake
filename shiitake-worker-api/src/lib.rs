//! The shared contract between shiitake-server and shiitake-worker: the wire
//! frames (this module) and the on-disk capture layout ([`capture`]). Kept in
//! its own crate so the worker depends only on this, not on the server's full
//! dependency tree (axum, kube, OpenTelemetry, …).

pub mod capture;

use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Display)]
#[serde(transparent)]
#[display("{_0}")]
pub struct WorkerId(String);

impl WorkerId {
    /// Wrap an identifier. Call at the true origin of a worker id — the worker's
    /// config / `Hello` handshake — never to convert an arbitrary string mid-chain.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Display)]
#[serde(transparent)]
#[display("{_0}")]
pub struct ExecId(String);

impl ExecId {
    /// Mint a fresh id. Call only at the true origin of an execution — the
    /// server's UUID mint — never to convert an arbitrary string mid-chain.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

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
    Hello { worker_id: WorkerId },
    /// Server → worker: a command to run.
    Execute(ExecuteFrame),
    /// Server → worker: cancel the in-flight command. Worker SIGKILLs the
    /// process group and follows up with a Result frame whose `cancelled`
    /// flag is set.
    Cancel { request_id: ExecId },
    /// Worker → server: the result of an Execute. The output lives on
    /// disk under `<capture_root>/<request_id>/{stdout,stderr}` — the server
    /// stats those files for byte counts, so only exit metadata and the
    /// per-command resource usage travel on the wire.
    Result(ResultFrame),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteFrame {
    pub request_id: ExecId,
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
    pub request_id: ExecId,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
    pub timed_out: bool,
    pub cancelled: bool,
    #[serde(default)]
    pub usage: ResourceUsage,
}

impl ResultFrame {
    #[allow(dead_code)] // server doesn't construct these — worker does
    pub fn errored(request_id: ExecId, message: impl Into<String>) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;

    // `WorkerId` is `#[serde(transparent)]`, so it must be wire-identical to the
    // bare `String` it replaced: a `Hello` frame serializes its worker_id as a
    // plain JSON string, and an old-format payload still deserializes. This
    // pins the protocol so the newtype can never silently change the wire shape.
    #[test]
    fn worker_id_is_wire_transparent() {
        let frame = Frame::Hello {
            worker_id: WorkerId::new("worker-0"),
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert_eq!(json, r#"{"kind":"hello","worker_id":"worker-0"}"#);

        // The pre-newtype payload (a bare string) still deserializes.
        let parsed: Frame =
            serde_json::from_str(r#"{"kind":"hello","worker_id":"worker-7"}"#).unwrap();
        match parsed {
            Frame::Hello { worker_id } => assert_eq!(worker_id.as_str(), "worker-7"),
            _ => panic!("expected Hello"),
        }
    }

    // `ExecId` is also `#[serde(transparent)]`: the request_id on every frame
    // stays a bare JSON string, and an old-format Cancel still deserializes.
    #[test]
    fn exec_id_is_wire_transparent() {
        let frame = Frame::Cancel {
            request_id: ExecId::new("req-1"),
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert_eq!(json, r#"{"kind":"cancel","request_id":"req-1"}"#);

        let parsed: Frame =
            serde_json::from_str(r#"{"kind":"cancel","request_id":"req-2"}"#).unwrap();
        match parsed {
            Frame::Cancel { request_id } => assert_eq!(request_id.as_str(), "req-2"),
            _ => panic!("expected Cancel"),
        }
    }
}
