# 🍄 Shiitake

A small-footprint command-dispatcher: one **server** accepts HTTP `/exec` calls
and hands each command to one of N **worker** processes over a WebSocket. Each
worker runs commands in its own resource-bounded container, resetting its
sandbox to a clean slate between them. The server is the only ingress; workers
never bind a port, they dial out to the dispatcher.

The server and the workers can share a pod or run as two, whichever the
deployment needs — see [Topologies](#topologies).

Shiitake is generic: it has no knowledge of any particular application. You
bring the toolchain image, drop in the worker binary as its entrypoint, and the
server fans commands out to the pool.

## Why

- **Isolation per command.** A worker resets its sandbox between commands (and
  can be recycled entirely every N), so there is no state bleed between
  commands, and a runaway command can only exhaust its own container — the
  server and the other workers are unaffected.
- **Separable server and workers.** Run them in one pod for the fewest moving
  parts, or in two so that pod-scoped controls — NetworkPolicy above all — can
  be tight on the containers running arbitrary commands while the server keeps
  the reach it needs. Same binaries, same wire protocol, one env var.
- **Zero-copy output capture.** The worker redirects the command's stdout/stderr
  straight into per-stream capture files via inherited fds — the kernel writes
  to disk, so neither the worker nor the server holds output in memory. The
  server reads it back with HTTP range support. Storage is unbounded (bounded by
  the volume), and output sizes are reported as metrics so a runaway command is
  observable rather than truncated.
- **Identity-agnostic privilege drop.** An `/exec` request may carry a `drop_to`
  directive (`uid`, `gid`, supplementary gids, umask); the worker applies it in
  the post-fork `pre_exec` hook before exec. Shiitake never decides identities —
  an embedding layer maps its own auth to `drop_to`.
- **OpenTelemetry built in.** The server emits traces (a span per command) and
  metrics (exit cause, duration, memory/CPU, output sizes, pool occupancy) over
  OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

## Crates

| Crate                   | Role                                                                                  |
| ----------------------- | ------------------------------------------------------------------------------------- |
| `shiitake-worker-api`   | Lib. The server↔worker contract: wire frames + the on-disk capture layout. The worker depends only on this. |
| `shiitake-server-api`   | Lib. The HTTP API request/response types — the contract between the server and any client. Pure types, no transport. |
| `shiitake-server`       | Lib + bin. axum HTTP API + WebSocket dispatcher + worker pool + Kubernetes OOM probe + OTel. Owns the capture-file layout and range reads. |
| `shiitake-worker`       | Bin. Dials the dispatcher by URL, runs each command in its own process group, redirects output to capture files, reports resource usage, and resets between commands. |
| `clients/shiitake-rs`   | Lib. Async `reqwest` client over the HTTP API. |
| `clients/shiitake-py`   | Python client over the HTTP API (`httpx`). |

## HTTP API

The HTTP API is versioned under `/api/v1`. The worker dispatch endpoint
(`/dispatch`) is a separate internal router on its own listener, with its own
bearer token.

| Method | Path                           | Purpose                                                                     |
| ------ | ------------------------------ | --------------------------------------------------------------------------- |
| GET    | `/api/v1/health`               | Liveness + pool snapshot (`workers_idle`, `workers_inflight`). No auth.      |
| GET    | `/api/v1/ready`                | Readiness: `200` once `SHIITAKE_MIN_READY_WORKERS` workers are registered, `503` otherwise. No auth. |
| POST   | `/api/v1/exec`                 | Spawn a command. Returns `{handle, started_at}` (202). 429 if the pool is full. |
| GET    | `/api/v1/exec/{handle}`        | Status: state, exit code/signal/cause, per-stream byte counters.             |
| GET    | `/api/v1/exec/{handle}/stdout` | Read stdout. Serves the capture file with HTTP `Range` support (`206`/`416`); tail with `Range: bytes=-N`. |
| GET    | `/api/v1/exec/{handle}/stderr` | Read stderr.                                                                |
| DELETE | `/api/v1/exec/{handle}`        | SIGTERM → SIGKILL the command. Idempotent on terminal handles.              |
| GET    | `/dispatch`                    | **Internal.** WebSocket workers connect to for dispatch. Its own listener, guarded by `SHIITAKE_DISPATCH_TOKEN` — never expose it outside the cluster. |

`POST /api/v1/exec` body:

```json
{
  "command": "python3 -c 'print(2 + 2)'",
  "workdir": "/tmp",
  "timeout": 300.0,
  "env": {"PATH": "/usr/bin:/bin"},
  "drop_to": {"uid": 1000, "gid": 1000, "supplementary_gids": [], "umask": 7}
}
```

`command` is a single string, run verbatim as `bash -c <command>` — use ordinary
shell syntax for pipes, redirects, and multi-statement scripts. The command runs
with **only** the `env` you pass (the worker clears its own environment first),
so include `PATH` for any command that calls an external binary.

### Liveness vs readiness

`/health` answers "is the process up" and always succeeds while the server is
serving — an empty pool is not a reason to restart it, so it is the
**liveness** probe. `/ready` answers "can this pod serve a command": its status
code is `200` only once at least `SHIITAKE_MIN_READY_WORKERS` workers are
registered, and `503` otherwise, so an orchestrator keeps traffic off a pod
whose workers haven't connected (or have all died) instead of letting commands
be accepted and then fail. Point the **readiness** probe at it:

```yaml
readinessProbe:
  httpGet: { path: /api/v1/ready, port: 8080 }
livenessProbe:
  httpGet: { path: /api/v1/health, port: 8080 }
```

A worker counts as registered whether it is idle or running a command, so a
fully-busy pool stays ready — gating on idle workers alone would pull a pod out
of rotation exactly when it is doing the most work. Operators running a large
pool can raise `SHIITAKE_MIN_READY_WORKERS` to stay unready below some fraction
of it rather than only at zero.

`exit_cause` on a finished handle is one of `normal`, `signal`, `oom_container`,
`timeout`, `worker_died`, `cancelled`. OOM is detected externally from the
kubelet's container status, never self-reported by the worker.

## Configuration

### Server

| Variable                  | Default                          | Purpose                                            |
| ------------------------- | -------------------------------- | -------------------------------------------------- |
| `SHIITAKE_HOST`           | `0.0.0.0`                        | HTTP API listen address.                           |
| `SHIITAKE_PORT`           | `8080`                           | HTTP API listen port.                              |
| `SHIITAKE_DISPATCH_HOST`  | `127.0.0.1`                      | Worker dispatch listen address. Loopback is all the single-pod topology needs; set `0.0.0.0` to let workers in another pod reach it. |
| `SHIITAKE_DISPATCH_PORT`  | `8090`                           | Worker dispatch listen port.                       |
| `SHIITAKE_DISPATCH_TOKEN` | (empty)                          | Bearer token workers present on the dispatch upgrade, **required** — the server refuses to start if unset. Distinct from `SHIITAKE_AUTH_TOKEN`. |
| `SHIITAKE_DEFAULT_WORKDIR`| `/`                              | Working directory when a request omits `workdir`.  |
| `SHIITAKE_AUTH_TOKEN`     | (empty)                          | Bearer token guarding `/exec`, **required** — the server refuses to start if unset. |
| `SHIITAKE_MAX_BODY_BYTES` | `268435456`                      | Maximum accepted request body size (256 MiB).      |
| `SHIITAKE_CAPTURE_ROOT`   | `/capture`                       | Root for the stdout/stderr capture files.          |
| `SHIITAKE_MIN_READY_WORKERS` | `1`                           | Registered workers (idle + in-flight) the pool needs before `/ready` reports ready. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (unset)                      | OTLP endpoint. When set, the server exports traces + metrics; otherwise logs to stdout only. |
| `OTEL_EXPORTER_OTLP_PROTOCOL`  | `http/protobuf`             | OTLP transport: `grpc`, `http/protobuf`, or `http/json` (all plaintext). |
| `POD_NAME` / `POD_NAMESPACE` | (downward API)                | The server's own pod, used by the container-OOM probe for workers that don't report a pod of their own. |

### Worker

| Variable                | Default                         | Purpose                                               |
| ----------------------- | ------------------------------- | ----------------------------------------------------- |
| `SHIITAKE_WORKER_ID`    | `worker-unknown`                | Identifier advertised to the dispatcher. Must be unique across the pool — use the pod name when each worker is its own pod. |
| `SHIITAKE_DISPATCH_URL` | `ws://127.0.0.1:8090/dispatch`  | Full URL of the server's dispatch endpoint. The default is the same-pod case; point it at a Service (`ws://shiitake-dispatch:8090/dispatch`) to run the workers in their own pods. |
| `SHIITAKE_DISPATCH_TOKEN` | (empty)                       | Bearer token presented on the dispatch upgrade, **required**. Must match the server's. |
| `SHIITAKE_CAPTURE_ROOT` | `/capture`                      | Must match the server's capture root (shared volume). |
| `POD_NAME` / `POD_NAMESPACE` | (downward API)             | This worker's own pod, reported to the server so its container-OOM probe queries the right one. Omit outside Kubernetes. |
| `SHIITAKE_CONTAINER_NAME` | (the worker id)               | This worker's container name within its pod, for the same probe. |

## Distribution & deployment

- The **server** ships as a container image (`ghcr.io/tenzailabs/shiitake-server`).
- The **worker** ships as a minimal container image holding just the
  statically-linked binary (`ghcr.io/tenzailabs/shiitake-worker`) — copy it
  straight into your own toolchain image at build time:

  ```dockerfile
  FROM your/toolchain:latest
  # Pin by immutable digest — GHCR tags (incl. release tags) are mutable, so a
  # `COPY --from` by tag can be silently moved. Get the digest for a release
  # from the package page or `docker buildx imagetools inspect <image>:<tag>`.
  COPY --from=ghcr.io/tenzailabs/shiitake-worker@sha256:<digest> \
       /usr/local/bin/shiitake-worker /usr/local/bin/shiitake-worker
  ENTRYPOINT ["/usr/local/bin/shiitake-worker"]
  ```

## Topologies

Two supported shapes. The binaries, the wire protocol and the HTTP API are
identical in both — what changes is where the containers sit and how the worker
addresses the dispatcher.

### Single pod

One Pod holding the server and N worker containers. They share the pod network
namespace, so workers reach the dispatcher on the default
`ws://127.0.0.1:8090/dispatch`, and they share an `emptyDir` capture volume
mounted at the same path everywhere. Fewest moving parts; the default.

### Two pods

The server in its own Pod, the workers in theirs. Three things change:

- **Dispatch is addressed by URL.** Bind the server's dispatch listener with
  `SHIITAKE_DISPATCH_HOST=0.0.0.0`, put a cluster-internal Service in front of
  it, and point the workers at it with
  `SHIITAKE_DISPATCH_URL=ws://<service>:8090/dispatch`.
- **Dispatch is authenticated.** `SHIITAKE_DISPATCH_TOKEN` is required on both
  sides and checked on the upgrade request — being loopback-bound is no longer
  what protects that path. It is a separate secret from `SHIITAKE_AUTH_TOKEN`; a
  worker never needs the API's token.
- **Capture must span both Pods.** The worker writes the capture files and the
  server reads them back, so `SHIITAKE_CAPTURE_ROOT` has to name the same
  storage in both — a **ReadWriteMany** volume. An `emptyDir` cannot do this.

Give each worker a unique `SHIITAKE_WORKER_ID` (the pod name, via the downward
API, is the natural choice for a Deployment of worker pods), and wire
`POD_NAME` / `POD_NAMESPACE` / `SHIITAKE_CONTAINER_NAME` into the worker so the
server's OOM probe queries the worker's own pod rather than its own.

**Why bother.** A NetworkPolicy selects a Pod, not a container. While the server
and the workers share one, every egress the server legitimately needs — the API
server to classify an OOM-killed worker, a collector to export telemetry to — is
necessarily also granted to the containers running arbitrary commands. Hardening
the workers and giving the server what it needs pull in opposite directions, and
you have to pick one. Split apart, the workers get a policy of their own (no
ingress, egress only to DNS and the dispatcher) while the server keeps its
reach. `tests/chart` deploys both topologies; see `tests/README.md`.

## Local quickstart

```bash
# Terminal 1 — server
SHIITAKE_AUTH_TOKEN=dev-token SHIITAKE_DISPATCH_TOKEN=dev-dispatch \
  SHIITAKE_CAPTURE_ROOT=/tmp/capture cargo run --bin shiitake-server

# Terminal 2 — one worker (the default dispatch URL is loopback)
SHIITAKE_WORKER_ID=worker-0 SHIITAKE_DISPATCH_TOKEN=dev-dispatch \
  SHIITAKE_CAPTURE_ROOT=/tmp/capture cargo run --bin shiitake-worker

# Terminal 3 — wait until a worker has registered, then drive it
until curl -sf localhost:8080/api/v1/ready >/dev/null; do sleep 1; done
curl -sX POST localhost:8080/api/v1/exec \
  -H "Authorization: Bearer dev-token" \
  -d '{"command": "echo hi"}'
```

`curl -f` fails on the `503` that `/ready` returns while the pool is empty, so
that one-liner is the same gate a readiness probe applies. Drop the `-f` to see
the body: `{"ready":false,"service":"shiitake","workers_idle":0,"workers_inflight":0,"workers_required":1}`.

(The worker stays resident, serving command after command and resetting its
sandbox between them.)

## Testing

```bash
cargo test --workspace                      # unit + in-process integration tests
bash tests/setup.sh && bash tests/run.sh    # full k3d cluster e2e (see tests/)

# the same suite against the split topology
SHIITAKE_E2E_TOPOLOGY=two-pod bash tests/setup.sh
SHIITAKE_E2E_TOPOLOGY=two-pod bash tests/run.sh
```

## How to contribute

Contributions are welcome! Feel free to open an issue or a pull request. By
contributing, you agree that your contributions are licensed under the same
[Apache License 2.0](LICENSE) that covers this repository.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
