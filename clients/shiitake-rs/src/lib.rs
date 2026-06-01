//! A ready-to-use async HTTP client for the shiitake API, over `reqwest`.
//!
//! The request/response types live in `shiitake-server-api` and are re-exported
//! here so callers need only depend on this one crate.

mod client;
pub use client::{Client, ClientError, ReadChunk, Stream};
pub use shiitake_server_api::{
    DropTo, ExecRequest, ExitCause, HandleSnapshotJson, HandleStatus, HealthResponse,
    SpawnResponse, StatusResponse,
};
