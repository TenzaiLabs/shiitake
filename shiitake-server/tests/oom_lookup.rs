//! `oom_killed` reads a container's `lastState.terminated.reason` — the actual
//! OOM detection behind the worker-drop reconciler.

use k8s_openapi::api::core::v1::{
    ContainerState, ContainerStateTerminated, ContainerStatus, Pod, PodStatus,
};
use shiitake_server::pool::k8s_status::oom_killed;

fn pod_with(reason: Option<&str>) -> Pod {
    Pod {
        status: Some(PodStatus {
            container_statuses: Some(vec![ContainerStatus {
                name: "worker-0".into(),
                last_state: Some(ContainerState {
                    terminated: reason.map(|r| ContainerStateTerminated {
                        reason: Some(r.into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn detects_oom_killed_only() {
    assert!(oom_killed(&pod_with(Some("OOMKilled")), "worker-0"));
    // Every other terminated reason is not an OOM — including a stale lastState
    // from a prior restart that's still visible while the fresh kill is processed.
    assert!(!oom_killed(&pod_with(Some("Error")), "worker-0"));
    assert!(!oom_killed(&pod_with(Some("Completed")), "worker-0"));
    // No terminated lastState, or an unknown container, are likewise not OOM.
    assert!(!oom_killed(&pod_with(None), "worker-0"));
    assert!(!oom_killed(&pod_with(Some("OOMKilled")), "ghost"));
}
