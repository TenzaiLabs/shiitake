# End-to-end suite

Stands up a real Kubernetes cluster (k3d), builds the server image and a test
worker image, deploys **one server + N workers** (default 8) sharing a capture
volume with loopback dispatch, and runs HTTP-level checks against the live pool.

## Run

Build, setup, and test are separate steps; the cluster is left running between
them (the usual CI shape), so re-running the tests is cheap and there is no
teardown:

```bash
bash tests/build.sh   # build the server + test-worker images
bash tests/setup.sh   # create cluster, import images, deploy, roll out
bash tests/run.sh     # port-forward + run the checks (re-runnable)

k3d cluster delete shiitake-e2e   # tear down manually when done
```

`build.sh` needs `docker`; `setup.sh` needs `k3d`, `kubectl`, `helm`; `run.sh`
needs `kubectl`, `python3`, `curl`, `uv`. These come from `mise install` (pinned
in `mise.toml`) or your own PATH. CI builds the images in a separate,
layer-cached workflow step instead of `build.sh`.

The topology is a Helm chart (`tests/chart`). Deploy a different profile by
passing helm flags to `setup.sh`, or install a second release alongside the
first to run different server flavours in parallel:

```bash
bash tests/setup.sh --set worker.count=2 --set worker.resources.limits.memory=64Mi
SHIITAKE_E2E_RELEASE=tight SHIITAKE_E2E_PORT=18081 bash tests/setup.sh -f my-profile.yaml
```

## Coverage (`test_exec.py`)

- health + pool snapshot; bearer-auth required / rejected; unknown handle → 404
- `echo`, stderr capture + non-zero exit code, multi-line bash loops
- `python3` one-liners and a small stdlib program
- explicit `workdir` and `env` passthrough
- large output (5 MB) captured to the stream file, read back whole and via a
  suffix `Range` request (`206 Partial Content`)
- server-enforced timeout and `DELETE`-driven cancellation
- OOM: a command that exceeds the worker container's memory limit is killed and
  reported as `oom_container` (externally, via the kubelet probe) — or `signal`
  on hosts without cgroup `memory.oom.group`
- concurrent dispatch across the pool (overlapping commands report `inflight`)

## Output

After the assertions, the run prints two summaries:

- a **command summary** from `test_exec.py` — every command dispatched, its
  wall-clock time, terminal status / exit cause, and how many `429`s the pool
  returned (plus totals).
- the **server metrics** (`shiitake_*`) — `setup.sh` deploys an OpenTelemetry
  Collector (`otel.enabled=true`) that the server exports OTLP to; `run.sh`
  polls its Prometheus endpoint and prints the series. The export interval is
  shortened (`OTEL_METRIC_EXPORT_INTERVAL`) so metrics appear within a run. The
  tests run real commands, so the run **fails** (with collector/server log
  diagnostics) if no `shiitake_*` metrics show up within the wait window.

## Pieces

- `Dockerfile.worker` — builds the static musl worker binary and bakes it into
  `python:3-alpine` as the entrypoint, standing in for a real downstream
  toolchain image. Build context is the repo root.
- `chart/` — Helm chart for the topology: one server + `worker.count` workers in
  a pod sharing a capture `emptyDir`, plus a `pods get` Role for the server's
  container-OOM probe and (when `otel.enabled`) an OpenTelemetry Collector that
  receives the server's OTLP and re-exposes it on a Prometheus endpoint.
  Parametrised via `values.yaml` (image, worker count, per-container resources,
  auth token, otel).
- `build.sh` — build the server + test-worker images (local; CI uses a cached
  workflow step).
- `setup.sh` — create the cluster, import images, `helm install` the chart into
  its own namespace (otel enabled), and roll the deployment onto the new images.
  Extra args pass through to helm.
- `run.sh` — port-forward, run `test_exec.py` and the `shiitake-py` client e2e,
  then scrape and print the server's metrics.
- `lib.sh` — shared config + `log` helper sourced by the scripts.
- `test_exec.py` — standard-library HTTP client and assertions.

## Quick local check without k3d

The server and workers only need a shared network namespace and a shared capture
directory, so you can exercise the full path with Docker alone: run the server
container, then worker containers with `--network container:<server>` and a
shared `-v <vol>:/capture`, and point `test_exec.py` at the published port. The
k3d suite additionally covers RBAC, the `emptyDir` capture volume, and
kubelet-driven worker restarts.
