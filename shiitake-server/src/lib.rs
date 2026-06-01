//! Shiitake server: a small dispatcher that accepts HTTP `/exec` requests
//! and hands them off to worker containers over a websocket pool. Workers
//! are end-user-built — they connect, run one command, exit. Neither the
//! server nor the workers hold command output in memory: workers redirect
//! the command's stdout/stderr straight into the capture files defined by
//! `shiitake_worker_api::capture`, and the server reads them back with HTTP
//! range support.
//!
//! This crate is intentionally identity-agnostic. The ExecuteFrame carries
//! an optional `drop_to` directive (uid/gid/supplementary_gids/umask) which
//! the worker honours before exec; how that field is populated is up to
//! downstream layers.

pub mod http;
pub mod metrics;
pub mod pool;
pub mod serve;
pub mod telemetry;
