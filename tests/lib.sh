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

log() { printf '\n=== %s ===\n' "$*"; }
