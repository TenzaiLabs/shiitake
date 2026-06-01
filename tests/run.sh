#!/usr/bin/env bash
# Run the HTTP-level checks (test_exec.py) and the shiitake-py client e2e
# against the cluster that tests/setup.sh stood up. Re-runnable; creates and
# deletes nothing but a port-forward.
#
# Requires: kubectl, python3, curl, uv. Run tests/setup.sh first.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck source=tests/lib.sh
. "$HERE/lib.sh"

PF_PID=""
MPF_PID=""
cleanup() {
  if [ -n "$PF_PID" ]; then kill "$PF_PID" >/dev/null 2>&1 || true; fi
  if [ -n "$MPF_PID" ]; then kill "$MPF_PID" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

log "Port-forwarding ${LOCAL_PORT} -> 8080"
kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward "deploy/$RELEASE" "${LOCAL_PORT}:8080" >/dev/null 2>&1 &
PF_PID=$!
base="http://127.0.0.1:${LOCAL_PORT}"

# Wait for the whole pool to connect, not just one worker — the concurrency
# tests dispatch several commands at once and assert they run in parallel, so
# they fail (spuriously) if they start before enough workers are idle. Must
# match the chart's worker.count.
workers="${SHIITAKE_E2E_WORKERS:-8}"
log "Waiting for all ${workers} workers to connect"
idle=0
for _ in $(seq 1 90); do
  idle="$(curl -fsS "$base/api/v1/health" 2>/dev/null \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["workers_idle"])' 2>/dev/null || echo 0)"
  if [ "${idle:-0}" -ge "$workers" ]; then break; fi
  sleep 2
done
if [ "${idle:-0}" -lt "$workers" ]; then
  kubectl --context "$CONTEXT" -n "$NAMESPACE" describe "deploy/$RELEASE" || true
  kubectl --context "$CONTEXT" -n "$NAMESPACE" logs "deploy/$RELEASE" -c server || true
  echo "only ${idle}/${workers} workers connected to the dispatcher" >&2
  exit 1
fi

log "Running test_exec.py against ${base}"
SHIITAKE_E2E_URL="$base" SHIITAKE_E2E_TOKEN="$TOKEN" SHIITAKE_E2E_WORKERS="$workers" \
  python3 "$ROOT/tests/test_exec.py"

log "Running shiitake-py e2e against ${base}"
SHIITAKE_E2E_URL="$base" SHIITAKE_E2E_TOKEN="$TOKEN" \
  uv run --project "$ROOT/clients/shiitake-py" --group dev \
  pytest "$ROOT/clients/shiitake-py/tests/test_e2e.py" -q

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

kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward "svc/${RELEASE}-otel" "${mport}:8889" >/dev/null 2>&1 &
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

log "PASS"
