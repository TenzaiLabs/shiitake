#!/usr/bin/env bash
# Run the HTTP-level checks (test_exec.py) and the shiitake-py client e2e
# against the cluster that tests/setup.sh stood up. Re-runnable; creates and
# deletes nothing but a port-forward.
#
# The whole suite must pass in either topology — the API contract doesn't change
# when the server and the workers stop sharing a pod. A two-pod deployment
# (SHIITAKE_E2E_TOPOLOGY=two-pod, matching what setup.sh deployed) additionally
# runs test_two_pod.py for what the split makes newly true.
#
# Requires: kubectl, python3, curl, uv. Run tests/setup.sh first.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck source=tests/lib.sh
. "$HERE/lib.sh"

PF_PID=""
MPF_PID=""
PF_LOG="$(mktemp)"
MPF_LOG="$(mktemp)"

port_forward_forever() (
  local resource="$1" local_port="$2" remote_port="$3" log_file="$4" child=""
  stop_forward() {
    trap - TERM INT
    if [ -n "$child" ]; then
      kill "$child" >/dev/null 2>&1 || true
      wait "$child" 2>/dev/null || true
    fi
    exit 0
  }
  trap stop_forward TERM INT
  while :; do
    kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward \
      "$resource" "${local_port}:${remote_port}" >>"$log_file" 2>&1 &
    child=$!
    wait "$child" || true
    child=""
    printf 'port-forward %s exited; restarting\n' "$resource" >&2
    sleep 0.2
  done
)

cleanup() {
  local status=$?
  trap - EXIT
  if [ -n "$PF_PID" ]; then kill "$PF_PID" >/dev/null 2>&1 || true; fi
  if [ -n "$MPF_PID" ]; then kill "$MPF_PID" >/dev/null 2>&1 || true; fi
  if [ "$status" -ne 0 ]; then
    log "API port-forward log"
    tail -60 "$PF_LOG" || true
    log "Metrics port-forward log"
    tail -60 "$MPF_LOG" || true
    diagnostics
  fi
  rm -f "$PF_LOG" "$MPF_LOG"
  exit "$status"
}
trap cleanup EXIT

log "Port-forwarding ${LOCAL_PORT} -> 8080"
port_forward_forever "deploy/$RELEASE" "$LOCAL_PORT" 8080 "$PF_LOG" &
PF_PID=$!
base="http://127.0.0.1:${LOCAL_PORT}"

# Wait for both test profiles, including the isolated workers, before the
# suite starts allocating sessions.
workers="${SHIITAKE_E2E_WORKERS:-8}"
isolated="${SHIITAKE_E2E_ISOLATED_WORKERS:-2}"
total=$((workers + isolated))
log "Waiting for all ${total} workers to connect"
idle=0
for _ in $(seq 1 90); do
  idle="$(curl -fsS "$base/api/v1/health" 2>/dev/null \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["workers_idle"])' 2>/dev/null || echo 0)"
  if [ "${idle:-0}" -ge "$total" ]; then break; fi
  sleep 2
done
if [ "${idle:-0}" -lt "$total" ]; then
  echo "only ${idle}/${total} workers connected to the dispatcher" >&2
  diagnostics
  exit 1
fi

# Topology-specific checks first: if the split isn't actually in effect, say
# so before the broad suite muddies the output.
if [ "$TOPOLOGY" = "two-pod" ]; then
  log "Running test_two_pod.py against ${base}"
  SHIITAKE_E2E_URL="$base" SHIITAKE_E2E_TOKEN="$TOKEN" SHIITAKE_E2E_RELEASE="$RELEASE" \
    python3 "$ROOT/tests/test_two_pod.py"
fi

log "Running test_exec.py against ${base}"
SHIITAKE_E2E_URL="$base" SHIITAKE_E2E_TOKEN="$TOKEN" SHIITAKE_E2E_WORKERS="$workers" \
  python3 "$ROOT/tests/test_exec.py"

# The workers run as root in-cluster, so the PTY named-user drop (writes
# /etc/passwd + setuids) can run here; SHIITAKE_E2E_ROOT_WORKERS enables it.
log "Running shiitake-py e2e against ${base}"
# An existing project .venv can have been created by an older Python. Use the
# same interpreter as the HTTP checks and mise pin for both uv invocations.
test_python="$(command -v python3)"
SHIITAKE_E2E_URL="$base" SHIITAKE_E2E_TOKEN="$TOKEN" SHIITAKE_E2E_ROOT_WORKERS=1 \
  uv run --python "$test_python" --locked --project "$ROOT/clients/shiitake-py" --group dev \
  pytest "$ROOT/clients/shiitake-py/tests/test_e2e.py" \
         "$ROOT/clients/shiitake-py/tests/test_pty_e2e.py" -q

# Scrape the server's emitted metrics from the OTel collector's Prometheus
# endpoint (deployed by setup.sh with otel.enabled=true). The tests ran real
# commands, so `shiitake_exec_*` must show up — poll until they export, and
# fail loudly (with diagnostics) if they never do.
log "Server metrics (shiitake_*)"
mport="${SHIITAKE_E2E_METRICS_PORT:-18889}"
if ! kubectl --context "$CONTEXT" -n "$NAMESPACE" get "svc/${RELEASE}-otel" >/dev/null 2>&1; then
  echo "otel collector not deployed — re-run tests/setup.sh (it enables otel)" >&2
  exit 1
fi

port_forward_forever "svc/${RELEASE}-otel" "$mport" 8889 "$MPF_LOG" &
MPF_PID=$!

metrics=""
for _ in $(seq 1 30); do
  metrics="$(curl -fsS "http://127.0.0.1:${mport}/metrics" 2>/dev/null | grep '^shiitake_' | sort || true)"
  [ -n "$metrics" ] && break
  sleep 2
done

if [ -z "$metrics" ]; then
  echo "no shiitake_ metrics exported after waiting — the OTLP pipeline is broken" >&2
  echo "--- raw /metrics (head) ---" >&2
  curl -fsS "http://127.0.0.1:${mport}/metrics" 2>/dev/null | head -40 >&2 || true
  echo "--- collector logs (tail) ---" >&2
  kubectl --context "$CONTEXT" -n "$NAMESPACE" logs "deploy/${RELEASE}-otel" --tail=40 >&2 || true
  echo "--- server logs (tail) ---" >&2
  kubectl --context "$CONTEXT" -n "$NAMESPACE" logs "deploy/$RELEASE" -c server --tail=40 >&2 || true
  exit 1
fi
printf '%s\n' "$metrics"

# Last: the pod-failure suite (self-contained — own port-forward + kubectl, since
# it kills the pods the shared forward above rides on). It includes the terminal
# worker-death case, so it runs under uv for the shiitake-py client. Two-pod only
# — a worker is its own pod there.
if [ "$TOPOLOGY" = "two-pod" ]; then
  log "Running test_pod_failures.py"
  SHIITAKE_E2E_TOKEN="$TOKEN" SHIITAKE_E2E_WORKERS="$workers" \
    SHIITAKE_E2E_CONTEXT="$CONTEXT" SHIITAKE_E2E_NAMESPACE="$NAMESPACE" \
    SHIITAKE_E2E_RELEASE="$RELEASE" \
    uv run --python "$test_python" --locked --project "$ROOT/clients/shiitake-py" --group dev \
    python "$ROOT/tests/test_pod_failures.py"
fi

log "PASS"
