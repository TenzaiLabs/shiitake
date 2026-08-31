#!/usr/bin/env bash
# Shared config + helpers for the e2e scripts (setup.sh, run.sh).
# shellcheck shell=bash
# Values are consumed by the scripts that source this file.
# shellcheck disable=SC2034

CLUSTER="${SHIITAKE_E2E_CLUSTER:-shiitake-e2e}"
CONTEXT="k3d-${CLUSTER}"
LOCAL_PORT="${SHIITAKE_E2E_PORT:-18080}"
SERVER_IMAGE="shiitake-server:e2e"
WORKER_IMAGE="shiitake-test-worker:e2e"
# Helm release name; the chart names the deployment after it. Deployed into its
# own namespace, never `default`.
RELEASE="${SHIITAKE_E2E_RELEASE:-shiitake-e2e}"
NAMESPACE="${SHIITAKE_E2E_NAMESPACE:-shiitake-e2e}"
# Must match authToken in the chart's values.
TOKEN="e2e-secret-token"
# Deployment topology, passed straight through to the chart's `topology` value:
#   single-pod — server + worker containers in one pod, dispatch on loopback
#   two-pod    — server and workers in pods of their own, dispatch through a
#                Service, workers confined by a NetworkPolicy
# Both must pass the same test_exec.py suite; two-pod adds test_two_pod.py.
TOPOLOGY="${SHIITAKE_E2E_TOPOLOGY:-single-pod}"
# The server Deployment is named after the release in both topologies, so
# port-forwards and log tails don't care which one is deployed. In two-pod the
# workers get their own Deployment alongside it.
WORKER_DEPLOY="${RELEASE}-workers"

log() { printf '\n=== %s ===\n' "$*"; }

# Everything worth knowing when a deploy or a test run fails. Kept here because
# both setup.sh and run.sh need it, and because the interesting failures are the
# ones where the cluster itself is unwell — a helm timeout tells you nothing on
# its own, so dump the node's k3s log too, not just the workload's.
diagnostics() {
  log "Diagnostics: nodes"
  kubectl --context "$CONTEXT" get nodes -o wide 2>&1 | head -20 || true
  kubectl --context "$CONTEXT" describe nodes 2>&1 | grep -A12 -iE "conditions|allocated resources" | head -60 || true

  log "Diagnostics: workload"
  kubectl --context "$CONTEXT" -n "$NAMESPACE" get all,networkpolicy -o wide 2>&1 | head -60 || true
  kubectl --context "$CONTEXT" -n "$NAMESPACE" get pods -o wide 2>&1 | head -40 || true

  log "Diagnostics: events"
  kubectl --context "$CONTEXT" -n "$NAMESPACE" get events \
    --sort-by=.lastTimestamp 2>&1 | tail -60 || true

  log "Diagnostics: not-running pods"
  kubectl --context "$CONTEXT" -n "$NAMESPACE" get pods \
    --field-selector=status.phase!=Running -o name 2>/dev/null | while read -r pod; do
    kubectl --context "$CONTEXT" -n "$NAMESPACE" describe "$pod" 2>&1 | tail -40 || true
  done

  log "Diagnostics: shiitake logs"
  kubectl --context "$CONTEXT" -n "$NAMESPACE" logs "deploy/$RELEASE" -c server --tail=60 2>&1 || true
  kubectl --context "$CONTEXT" -n "$NAMESPACE" logs "deploy/$WORKER_DEPLOY" --tail=60 2>&1 || true

  # The k3s server itself. When the API server flaps ("apiserver not ready",
  # EOF) this is the only place the reason shows up — a panic in an embedded
  # controller, or the node being out of memory.
  log "Diagnostics: k3s node containers"
  docker ps -a --filter "name=k3d-${CLUSTER}" --format '{{.Names}}\t{{.Status}}' 2>&1 || true
  for c in $(docker ps -a --filter "name=k3d-${CLUSTER}" --format '{{.Names}}' 2>/dev/null); do
    printf '\n--- docker logs %s (tail) ---\n' "$c"
    docker logs --tail 120 "$c" 2>&1 | tail -120 || true
  done

  log "Diagnostics: host memory"
  free -m 2>&1 || true
}
