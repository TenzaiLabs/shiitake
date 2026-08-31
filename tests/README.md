# End-to-end suite

Stands up a real Kubernetes cluster (k3d), builds the server image and a test
worker image, deploys **one server + N workers** (default 8) sharing a capture
volume, and runs HTTP-level checks against the live pool.

## Topologies

The chart deploys either supported shape, selected by `SHIITAKE_E2E_TOPOLOGY`:

| | `single-pod` (default) | `two-pod` |
| --- | --- | --- |
| Layout | server + N worker containers in one pod | server pod, plus N worker pods (one container each) |
| Dispatch | `ws://127.0.0.1:8090/dispatch` | `ws://<release>-dispatch:8090/dispatch`, through a Service |
| Capture | shared `emptyDir` | shared `hostPath` (single-node k3d stands in for a ReadWriteMany volume) |
| Worker id | literal `worker-N` | the pod name, via the downward API |
| Worker recycle | per-container `restartPolicy` (needs k8s ≥ 1.35) | the ordinary pod `restartPolicy` |
| NetworkPolicy | not expressible — one policy would catch the server too | workers confined: no ingress, egress only to DNS and the dispatcher |
| Service account | mounted (the server needs it) | not mounted in the worker pods |

Both run the **same** `test_exec.py` suite: the API contract does not change
when the server and the workers stop sharing a pod. `two-pod` additionally runs
`test_two_pod.py` for what the split makes newly true. CI matrixes over both.

## Run

Build, setup, and test are separate steps; the cluster is left running between
them (the usual CI shape), so re-running the tests is cheap and there is no
teardown:

```bash
bash tests/build.sh   # build the server + test-worker images
bash tests/setup.sh   # create cluster, import images, deploy, roll out
bash tests/run.sh     # port-forward + run the checks (re-runnable)

# the split topology — setup.sh and run.sh must agree on it
SHIITAKE_E2E_TOPOLOGY=two-pod bash tests/setup.sh
SHIITAKE_E2E_TOPOLOGY=two-pod bash tests/run.sh

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

- health + pool snapshot; `/ready` reports ready with the pool up (both probes
  unauthenticated); bearer-auth required / rejected; unknown handle → 404
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

## Coverage (`test_two_pod.py`, two-pod only)

- commands land on worker-Deployment pods, so the Execute genuinely crossed a
  pod boundary through the dispatch Service
- output written by a worker in one pod reads back byte-for-byte through the
  server in another — the capture volume really does span both
- the worker NetworkPolicy is in effect: workers still reach the dispatcher (the
  positive control) but not the OTel collector and not the API server, the two
  egresses the server itself needs
- the worker pods carry no service-account token

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
- `chart/` — Helm chart for both topologies, selected by the `topology` value.
  `templates/_helpers.tpl` holds the server and worker container specs, which
  both topologies compose from, so a knob added for one is a knob the other
  gets — including readiness on `/api/v1/ready` (so a rollout only completes
  once workers have registered) and liveness on `/api/v1/health`. Also a `pods
  get` Role for the server's container-OOM probe and (when `otel.enabled`) an
  OpenTelemetry Collector that receives the server's OTLP and re-exposes it on a
  Prometheus endpoint. Parametrised via `values.yaml` (topology, image, worker
  count, per-container resources, tokens, otel, NetworkPolicy).
- `build.sh` — build the server + test-worker images (local; CI uses a cached
  workflow step).
- `setup.sh` — create the cluster, import images, `helm install` the chart into
  its own namespace (otel enabled), and roll the deployments onto the new
  images. Extra args pass through to helm.
- `run.sh` — port-forward, run the topology's checks, `test_exec.py` and the
  `shiitake-py` client e2e, then scrape and print the server's metrics.
- `lib.sh` — shared config + `log` helper sourced by the scripts.
- `test_exec.py` — standard-library HTTP client and assertions.
- `test_two_pod.py` — the split-topology checks; imports `test_exec.py`'s HTTP
  helpers rather than duplicating them.

## Notes on the two-pod deployment

- The capture volume is a `hostPath`, which works because k3d's default cluster
  is a single node, so both pods land on it. A real multi-node deployment needs
  a **ReadWriteMany** PersistentVolume — the server has to read back the very
  files the worker wrote.
- NetworkPolicy enforcement comes from k3s's built-in controller. Deploy with
  `--set networkPolicy.enabled=false` (and `SHIITAKE_E2E_NETPOL=0` when running)
  to A/B the pool with the policy out of the way.
- The per-container `restartPolicy: Always` (and its k8s ≥ 1.35 requirement) is
  a single-pod concern only; a worker pod recycles on the ordinary pod
  `restartPolicy`.

## Quick local check without k3d

In the single-pod shape the server and workers only need a shared network
namespace and a shared capture directory, so you can exercise the full path with
Docker alone: run the server container, then worker containers with `--network
container:<server>` and a shared `-v <vol>:/capture`, and point `test_exec.py` at
the published port. For the split shape, drop `--network container:<server>`,
publish the dispatch port, and give the workers a
`SHIITAKE_DISPATCH_URL=ws://<server-host>:8090/dispatch` — but note that Docker
alone cannot exercise NetworkPolicy. The k3d suite additionally covers RBAC, the
capture volume, kubelet-driven worker restarts, and the worker NetworkPolicy.
