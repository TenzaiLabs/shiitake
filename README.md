# 🍄 Shiitake

A small-footprint command-dispatcher: one **server** accepts HTTP `/exec` calls
and hands each command to one of N **worker** processes over an in-pod
WebSocket. Each worker runs a single command in its own resource-bounded
container and exits — the orchestrator restarts it for a clean slate per
command. The server is the only ingress; workers never bind a public port.

Shiitake is generic: it has no knowledge of any particular application. You
bring the toolchain image, drop in the worker binary as its entrypoint, and the
server fans commands out to the pool.

## Why

- **Isolation per command.** A worker handles exactly one command and exits, so
  there is no state bleed between commands and a runaway command can only
  exhaust its own container — the server and the other workers are unaffected.
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
| `shiitake-worker`       | Bin. Connects to the dispatcher, runs one command in its own process group, redirects output to capture files, reports resource usage, exits. |
| `clients/shiitake-rs`   | Lib. Async `reqwest` client over the HTTP API. |
| `clients/shiitake-py`   | Python client over the HTTP API (`httpx`). |

## HTTP API

The HTTP API is versioned under `/api/v1`. The worker dispatch endpoint
(`/dispatch`) is a separate internal router on the loopback dispatch port.

| Method | Path                           | Purpose                                                                     |
| ------ | ------------------------------ | --------------------------------------------------------------------------- |
| GET    | `/api/v1/health`               | Liveness + pool snapshot (`workers_idle`, `workers_inflight`). No auth.      |
| POST   | `/api/v1/exec`                 | Spawn a command. Returns `{handle, started_at}` (202). 429 if the pool is full. |
| GET    | `/api/v1/exec/{handle}`        | Status: state, exit code/signal/cause, per-stream byte counters.             |
| GET    | `/api/v1/exec/{handle}/stdout` | Read stdout. Serves the capture file with HTTP `Range` support (`206`/`416`); tail with `Range: bytes=-N`. |
| GET    | `/api/v1/exec/{handle}/stderr` | Read stderr.                                                                |
| DELETE | `/api/v1/exec/{handle}`        | SIGTERM → SIGKILL the command. Idempotent on terminal handles.              |
| GET    | `/dispatch`                    | **Internal, loopback-only.** WebSocket workers connect to for dispatch.     |

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

`exit_cause` on a finished handle is one of `normal`, `signal`, `oom_container`,
`timeout`, `worker_died`, `cancelled`. OOM is detected externally from the
kubelet's container status, never self-reported by the worker.

## Configuration

### Server

| Variable                  | Default                          | Purpose                                            |
| ------------------------- | -------------------------------- | -------------------------------------------------- |
| `SHIITAKE_HOST`           | `0.0.0.0`                        | HTTP API listen address.                           |
| `SHIITAKE_PORT`           | `8080`                           | HTTP API listen port.                              |
| `SHIITAKE_DISPATCH_HOST`  | `127.0.0.1`                      | Worker dispatch listen address (keep on loopback). |
| `SHIITAKE_DISPATCH_PORT`  | `8090`                           | Worker dispatch listen port.                       |
| `SHIITAKE_DEFAULT_WORKDIR`| `/`                              | Working directory when a request omits `workdir`.  |
| `SHIITAKE_AUTH_TOKEN`     | (empty)                          | Bearer token guarding `/exec`, **required** — the server refuses to start if unset. |
| `SHIITAKE_MAX_BODY_BYTES` | `268435456`                      | Maximum accepted request body size (256 MiB).      |
| `SHIITAKE_CAPTURE_ROOT`   | `/capture`                       | Root for the stdout/stderr capture files.          |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (unset)                      | OTLP endpoint. When set, the server exports traces + metrics; otherwise logs to stdout only. |
| `OTEL_EXPORTER_OTLP_PROTOCOL`  | `http/protobuf`             | OTLP transport: `grpc`, `http/protobuf`, or `http/json` (all plaintext). |
| `POD_NAME` / `POD_NAMESPACE` | (downward API)                | Used by the Kubernetes container-OOM probe.        |

### Worker

| Variable                | Default                         | Purpose                                               |
| ----------------------- | ------------------------------- | ----------------------------------------------------- |
| `SHIITAKE_WORKER_ID`    | `worker-unknown`                | Identifier advertised to the dispatcher.              |
| `SHIITAKE_DISPATCH_PORT`| `8090`                          | Dispatcher port. The host is always loopback (`127.0.0.1`) — server and workers share the pod network namespace. |
| `SHIITAKE_CAPTURE_ROOT` | `/capture`                      | Must match the server's capture root (shared volume). |

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

Run one server container and N worker containers in a single Pod that shares the
pod network namespace (so workers reach the dispatcher on `127.0.0.1`) and a
capture volume (mounted into the server and every worker at the same path; an
`emptyDir`, or a persistent volume if you want output to survive the pod). The
worker exits after each command; set the Pod `restartPolicy: Always` so the
kubelet restarts it. `tests/run.sh` deploys exactly this topology.

## Local quickstart

```bash
# Terminal 1 — server
SHIITAKE_AUTH_TOKEN=dev-token SHIITAKE_CAPTURE_ROOT=/tmp/capture cargo run --bin shiitake-server

# Terminal 2 — one worker
SHIITAKE_WORKER_ID=worker-0 SHIITAKE_CAPTURE_ROOT=/tmp/capture cargo run --bin shiitake-worker

# Terminal 3 — drive it
curl -sX POST localhost:8080/api/v1/exec \
  -H "Authorization: Bearer dev-token" \
  -d '{"command": "echo hi"}'
```

(The worker exits after one command; re-run it for the next.)

## Testing

```bash
cargo test --workspace                      # unit + in-process integration tests
bash tests/setup.sh && bash tests/run.sh    # full k3d cluster e2e (see tests/)
```

## How to contribute

Contributions are welcome! Feel free to open an issue or a pull request. By
contributing, you agree that your contributions are licensed under the same
[Apache License 2.0](LICENSE) that covers this repository.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
