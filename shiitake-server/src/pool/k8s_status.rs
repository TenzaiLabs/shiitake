//! K8s API probe for resolving "did the worker container get OOMKilled?"
//! after the WebSocket drops. The kubelet only publishes the container's
//! terminated `lastState` once it has processed the kill and restart, which
//! lags the dropped connection by several seconds — so we poll the pod status
//! once a second until the `OOMKilled` reason appears, with a ceiling that
//! bounds a genuinely non-OOM death.

use k8s_openapi::api::core::v1::Pod;
use kube::{Client, api::Api, config::Config};
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

/// Probes the Kubernetes API for whether a worker container was OOM-killed
/// after its WebSocket drops, to distinguish that from a generic exit. Backed
/// by the in-cluster kube-rs client.
pub struct ClusterProbe {
    client: Client,
}

impl ClusterProbe {
    pub async fn new() -> anyhow::Result<Self> {
        // Tries in-cluster config first, falls back to KUBECONFIG. Sane
        // default for both Pod runtime and local dev.
        let config = Config::infer().await?;
        let client = Client::try_from(config)?;
        Ok(Self { client })
    }

    pub async fn was_oom_killed(&self, pod: &str, namespace: &str, container: &str) -> bool {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        // Poll until the kubelet publishes the fresh `OOMKilled` lastState (it
        // lags the WS drop by a few seconds). Only `OOMKilled` is a positive:
        // every other terminated reason — including a stale lastState from a
        // prior restart that's still visible while the kill is processed —
        // keeps us polling rather than short-circuiting to a wrong answer.
        // Read the whole pod with `get`, not `get_status`: the latter hits the
        // `pods/status` subresource, which the pod's RBAC does not grant.
        for _ in 0..15 {
            match api.get(pod).await {
                Ok(p) if oom_killed(&p, container) => return true,
                Ok(_) => {}
                Err(e) => warn!("kube get({pod}) error: {e}"),
            }
            sleep(Duration::from_secs(1)).await;
        }
        false
    }
}

/// Whether `container`'s terminated `lastState` within `pod` is `OOMKilled`.
pub fn oom_killed(pod: &Pod, container: &str) -> bool {
    let Some(status) = pod.status.as_ref() else {
        return false;
    };
    let Some(statuses) = status.container_statuses.as_ref() else {
        return false;
    };
    statuses
        .iter()
        .find(|c| c.name == container)
        .and_then(|cs| cs.last_state.as_ref())
        .and_then(|ls| ls.terminated.as_ref())
        .and_then(|t| t.reason.as_deref())
        == Some("OOMKilled")
}
