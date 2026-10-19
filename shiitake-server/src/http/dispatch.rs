//! GET /dispatch — the endpoint workers connect to and hand off their
//! connection to the pool.
//!
//! The bearer check lives on the router, so anything reaching this handler is
//! authenticated. The Hello that follows carries the worker's id and, when it
//! knows it, where its container runs — which the pool needs for the OOM probe.

use crate::pool::WorkerPool;
use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use shiitake_worker_api::{Frame, PROTOCOL_VERSION};
use std::{collections::BTreeMap, sync::Arc};
use tracing::{info, warn};

/// Profile-bound dispatch credentials. The default profile uses the ordinary
/// dispatch token; every additional profile gets a distinct token. A worker
/// cannot claim a different profile in Hello with its own credential.
#[derive(Clone)]
pub struct ProfileCredentials {
    pub pool: Arc<WorkerPool>,
    pub tokens: Arc<BTreeMap<String, String>>,
}

pub async fn connect_scoped(
    State(credentials): State<ProfileCredentials>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let bearer = headers
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    let Some(profile) = bearer.and_then(|token| {
        credentials
            .tokens
            .iter()
            .find(|(_, expected)| expected.as_str() == token)
            .map(|(name, _)| name.clone())
    }) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    upgrade.on_upgrade(move |socket| run_socket(credentials.pool, socket, Some(profile)))
}

async fn run_socket(pool: Arc<WorkerPool>, socket: WebSocket, expected_profile: Option<String>) {
    let (mut sink, mut stream) = socket.split();
    // Read the first frame; must be Hello.
    let (worker_id, profile, location) = match stream.next().await {
        Some(Ok(Message::Text(t))) => match serde_json::from_str::<Frame>(&t) {
            Ok(Frame::Hello {
                worker_id,
                protocol_version,
                profile,
                location,
            }) if protocol_version == PROTOCOL_VERSION => (worker_id, profile, location),
            Ok(Frame::Hello {
                protocol_version, ..
            }) => {
                warn!(
                    protocol_version,
                    expected = PROTOCOL_VERSION,
                    "rejecting incompatible worker protocol"
                );
                return;
            }
            Ok(other) => {
                warn!(?other, "first frame was not Hello; closing");
                return;
            }
            Err(e) => {
                warn!("parse Hello: {e}");
                return;
            }
        },
        other => {
            warn!(?other, "no Hello frame; closing");
            return;
        }
    };
    if profile.is_empty()
        || !profile
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        warn!(%worker_id, %profile, "invalid worker profile");
        return;
    }
    if expected_profile
        .as_deref()
        .is_some_and(|expected| expected != profile)
    {
        warn!(%worker_id, %profile, "worker tried to claim a different profile");
        return;
    }
    let ack = serde_json::to_string(&Frame::HelloAck {
        protocol_version: PROTOCOL_VERSION,
    })
    .expect("HelloAck serializes");
    if sink.send(Message::Text(ack.into())).await.is_err() {
        return;
    }
    info!(%worker_id, %profile, pod = ?location.as_ref().map(|l| &l.pod), "worker handshake complete");
    if let Err(e) = pool
        .register_and_run(worker_id, profile, location, sink, stream)
        .await
    {
        warn!("pool run error: {e}");
    }
}
