//! GET /dispatch — the endpoint workers connect to and hand off their
//! connection to the pool.

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
        let worker_id = match stream.next().await {
            Some(Ok(Message::Text(t))) => match serde_json::from_str::<Frame>(&t) {
                Ok(Frame::Hello { worker_id }) => worker_id,
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
        info!(%worker_id, "worker handshake complete");
        if let Err(e) = pool.register_and_run(worker_id, sink, stream).await {
            warn!("pool run error: {e}");
        }
    })
}
