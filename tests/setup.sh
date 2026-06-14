#!/usr/bin/env bash
# Stand up (or refresh) the e2e environment: create a k3d cluster, import the
# server and test-worker images, `helm upgrade --install` the deployment (one
# server + N workers), and roll it out onto the freshly-imported images. Leaves
# everything running for tests/run.sh.
#
# Build the images first with tests/build.sh (CI builds them in a separate,
# layer-cached step). Idempotent: re-run to roll out code changes — it reuses
# an existing cluster and restarts the deployment so re-imported images take
# effect.
#
# Deploy a different profile by passing extra helm args, e.g.
#   bash tests/setup.sh --set worker.count=2 --set worker.resources.limits.memory=64Mi
#
# Tear down manually when done: k3d cluster delete shiitake-e2e
#
# Requires: k3d, kubectl, helm, and the images already built (tests/build.sh).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck source=tests/lib.sh
. "$HERE/lib.sh"

if k3d cluster list "$CLUSTER" >/dev/null 2>&1; then
  log "Cluster '${CLUSTER}' already exists — reusing"
else
  # Per-container restartPolicy on a regular container (the worker's
  # SHIITAKE_RESTART_AFTER recycle path) is the ContainerRestartRules feature,
  # beta/on-by-default from k8s 1.35. k3d's default k3s is older and the API
  # server would reject the pod template, so pin a >=1.35 k3s image here.
  K3S_IMAGE="${K3S_IMAGE:-rancher/k3s:v1.35.5-k3s1}"
  log "Creating k3d cluster '${CLUSTER}' (image ${K3S_IMAGE})"
  k3d cluster create "$CLUSTER" --image "$K3S_IMAGE" --wait --timeout 180s
fi

log "Importing images into k3d"
k3d image import "$SERVER_IMAGE" "$WORKER_IMAGE" -c "$CLUSTER"

# On a re-run the same `:e2e` tag won't roll on its own, so trigger a restart to
# pick up the freshly-imported images (no-op on a first install). `helm --wait`
# then blocks until the deployment is ready; `--debug` streams what it's waiting on.
kubectl --context "$CONTEXT" -n "$NAMESPACE" rollout restart "deploy/$RELEASE" 2>/dev/null || true

log "Deploying release '${RELEASE}' into namespace '${NAMESPACE}'"
helm --kube-context "$CONTEXT" upgrade --install "$RELEASE" "$ROOT/tests/chart" \
  --namespace "$NAMESPACE" --create-namespace \
  --set server.image="$SERVER_IMAGE" \
  --set worker.image="$WORKER_IMAGE" \
  --set otel.enabled=true \
  --wait --timeout 300s --debug "$@"

log "Ready — run tests with tests/run.sh"
