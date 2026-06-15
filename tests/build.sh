#!/usr/bin/env bash
# Build the e2e server + test-worker images locally, then run tests/setup.sh.
# (CI builds the same images in a separate, layer-cached workflow step.)
#
# Requires: docker.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck source=tests/lib.sh
. "$HERE/lib.sh"

log "Building images"
docker build -t "$SERVER_IMAGE" -f "$ROOT/Dockerfile.server" "$ROOT"
docker build -t "$WORKER_IMAGE" -f "$ROOT/tests/Dockerfile.worker" "$ROOT"
