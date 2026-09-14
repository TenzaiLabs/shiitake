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
    /// Optional login name for `uid`. When set, the worker — still privileged,
    /// before the drop — ensures a matching `/etc/passwd` entry exists so tools
    /// that resolve the uid (`whoami`, `id -un`, a shell's `\u` prompt) read a
    /// real name instead of a bare number. Idempotent (skipped when `uid`
    /// already resolves) and undone by the between-session reset. Shiitake stays
    /// identity-agnostic: it materializes the name the caller chose for the uid
    /// it is already dropping into — it never runs a caller command as root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Create (and own, by `uid:gid`) `/home/<name>` when ensuring the account,
    /// so the session has a writable home. Ignored without `name`.
    #[serde(default)]
    pub create_home: bool,
}

/// Where a worker's container lives, reported on `Hello` so the server can ask
/// the kubelet whether it was OOM-killed. Only the worker knows this once the
/// two can sit in different pods. `None` means "the server's own pod, container
/// named after the worker id" — the single-pod case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLocation {
    /// The worker pod's name (`metadata.name`).
    pub pod: String,
    /// The worker pod's namespace (`metadata.namespace`).
    pub namespace: String,
    /// The worker's container name within that pod.
    pub container: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Frame {
    /// Worker → server: first frame after connect, advertises availability.
    Hello {
        worker_id: WorkerId,
        /// For the server's OOM probe. Omitted when the worker has no pod
        /// identity (local runs); the server then falls back to its own pod.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        location: Option<WorkerLocation>,
    },
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

    /// Server → worker: open an interactive PTY and spawn a shell on it. Unlike
    /// `Execute`, the session is persistent and bidirectional: its byte stream
    /// travels as WS **binary** frames (server→worker = stdin, worker→server =
    /// pty output), while these control frames stay JSON. The worker is pinned
    /// to this one session until it ends.
    PtyOpen(PtyOpenFrame),
    /// Server → worker: window resize for the session's PTY.
    PtyResize {
        session_id: ExecId,
        cols: u16,
        rows: u16,
    },
    /// Server → worker: end the session — SIGHUP the process group and close the
    /// pty. Idempotent with a `PtyExit` the worker may already have sent.
    PtyClose { session_id: ExecId },
    /// Worker → server: the shell exited, or the pty could not be opened
    /// (`error` set). Terminal frame; the worker then resets and rejoins idle.
    PtyExit {
        session_id: ExecId,
        exit_code: Option<i32>,
        exit_signal: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
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

/// Parameters for an interactive PTY session (`Frame::PtyOpen`). Mirrors
/// `ExecuteFrame`'s identity/cwd/env handling; there is no `timeout_secs` — a
/// terminal lives until the shell exits or the client disconnects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PtyOpenFrame {
    pub session_id: ExecId,
    /// Shell argv. Empty means the worker's default (`["bash", "-i"]`).
    #[serde(default)]
    pub command: Vec<String>,
    pub working_dir: String,
    pub env: BTreeMap<String, String>,
    pub cols: u16,
    pub rows: u16,
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
    /// Why the worker could not run the command, when it failed before the
    /// command produced an exit status. `None` for a command that ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ResultFrame {
    #[allow(dead_code)] // server doesn't construct these — worker does
    pub fn errored(request_id: ExecId, message: impl Into<String>) -> Self {
        Self {
            request_id,
            exit_code: None,
            exit_signal: None,
            timed_out: false,
            cancelled: false,
            usage: ResourceUsage::default(),
            error: Some(message.into()),
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
            location: None,
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert_eq!(json, r#"{"kind":"hello","worker_id":"worker-0"}"#);

        // The pre-newtype payload (a bare string) still deserializes.
        let parsed: Frame =
            serde_json::from_str(r#"{"kind":"hello","worker_id":"worker-7"}"#).unwrap();
        match parsed {
            Frame::Hello { worker_id, .. } => assert_eq!(worker_id.as_str(), "worker-7"),
            _ => panic!("expected Hello"),
        }
    }

    // A worker that knows where it runs (the two-pod topology) puts that on the
    // Hello; a worker that doesn't omits the field entirely, so the frame stays
    // byte-identical to what a pre-location worker sends. Both directions are
    // pinned here because the server's OOM probe reads this field and must keep
    // accepting either shape.
    #[test]
    fn hello_carries_an_optional_worker_location() {
        let frame = Frame::Hello {
            worker_id: WorkerId::new("worker-0"),
            location: Some(WorkerLocation {
                pod: "shiitake-workers-abc".into(),
                namespace: "shiitake".into(),
                container: "worker".into(),
            }),
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert_eq!(
            json,
            r#"{"kind":"hello","worker_id":"worker-0","location":{"pod":"shiitake-workers-abc","namespace":"shiitake","container":"worker"}}"#
        );

        let parsed: Frame = serde_json::from_str(&json).unwrap();
        match parsed {
            Frame::Hello { location, .. } => {
                let loc = location.expect("location round-trips");
                assert_eq!(loc.pod, "shiitake-workers-abc");
                assert_eq!(loc.container, "worker");
            }
            _ => panic!("expected Hello"),
        }

        // A Hello with no location at all still parses (single-pod worker).
        let parsed: Frame =
            serde_json::from_str(r#"{"kind":"hello","worker_id":"worker-7"}"#).unwrap();
        match parsed {
            Frame::Hello { location, .. } => assert_eq!(location, None),
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

/// The frame examples published in `docs/docs.html` under "The dispatch
/// protocol", verbatim. Someone writing their own worker builds against those,
/// so a change here that leaves them stale is a change that misleads them — this
/// fails first.
#[cfg(test)]
mod documented_frames {
    use super::*;

    #[test]
    fn hello_with_location_matches_the_docs() {
        let f: Frame = serde_json::from_str(
            r#"{"kind": "hello",
                "worker_id": "shiitake-workers-6d4b8f9c7-x2k9p",
                "location": {"pod": "shiitake-workers-6d4b8f9c7-x2k9p",
                             "namespace": "shiitake",
                             "container": "worker"}}"#,
        )
        .expect("documented hello frame must deserialize");
        let Frame::Hello {
            worker_id,
            location,
        } = f
        else {
            panic!("expected Hello")
        };
        assert_eq!(worker_id.as_str(), "shiitake-workers-6d4b8f9c7-x2k9p");
        assert_eq!(location.expect("location").container, "worker");
    }

    #[test]
    fn execute_matches_the_docs() {
        let f: Frame = serde_json::from_str(
            r#"{"kind": "execute",
                "request_id": "0f3c",
                "command": "echo hi",
                "working_dir": "/tmp",
                "env": {"PATH": "/usr/bin:/bin"},
                "timeout_secs": 300.0,
                "drop_to": {"uid": 1000, "gid": 1000, "supplementary_gids": [], "umask": 7}}"#,
        )
        .expect("documented execute frame must deserialize");
        let Frame::Execute(e) = f else {
            panic!("expected Execute")
        };
        assert_eq!(e.command, "echo hi");
        assert_eq!(e.timeout_secs, 300.0);
        assert_eq!(e.drop_to.expect("drop_to").uid, 1000);
    }

    #[test]
    fn cancel_matches_the_docs() {
        let f: Frame = serde_json::from_str(r#"{"kind": "cancel", "request_id": "0f3c"}"#)
            .expect("documented cancel frame must deserialize");
        let Frame::Cancel { request_id } = f else {
            panic!("expected Cancel")
        };
        assert_eq!(request_id.as_str(), "0f3c");
    }

    #[test]
    fn result_matches_the_docs() {
        let f: Frame = serde_json::from_str(
            r#"{"kind": "result",
                "request_id": "0f3c",
                "exit_code": 0,
                "exit_signal": null,
                "timed_out": false,
                "cancelled": false,
                "usage": {"memory_peak_bytes": 1048576, "memory_limit_bytes": 536870912,
                          "cpu_user_seconds": 0.01, "cpu_system_seconds": 0.00},
                "error": null}"#,
        )
        .expect("documented result frame must deserialize");
        let Frame::Result(r) = f else {
            panic!("expected Result")
        };
        assert_eq!(r.exit_code, Some(0));
        assert_eq!(r.usage.memory_peak_bytes, Some(1_048_576));
        assert!(r.error.is_none());
    }
}
