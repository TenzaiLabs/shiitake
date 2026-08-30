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
