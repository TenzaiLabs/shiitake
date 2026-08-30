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
        ws::{Message, WebSocketUpgrade},
    },
    response::Response,
};
use futures_util::StreamExt;
use shiitake_worker_api::Frame;
use std::sync::Arc;
use tracing::{info, warn};

pub async fn connect(State(pool): State<Arc<WorkerPool>>, upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(move |socket| async move {
        let (sink, mut stream) = socket.split();
        // Read the first frame; must be Hello.
        let (worker_id, location) = match stream.next().await {
            Some(Ok(Message::Text(t))) => match serde_json::from_str::<Frame>(&t) {
                Ok(Frame::Hello {
                    worker_id,
                    location,
                }) => (worker_id, location),
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
        info!(%worker_id, pod = ?location.as_ref().map(|l| &l.pod), "worker handshake complete");
        if let Err(e) = pool
            .register_and_run(worker_id, location, sink, stream)
            .await
        {
            warn!("pool run error: {e}");
        }
    })
}
