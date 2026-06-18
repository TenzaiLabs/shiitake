# 🍄 Shiitake

**Sandboxing for untrusted commands on Kubernetes.** Shiitake is an HTTP server
that fans commands out to a pool of resident, resource-bounded **worker**
containers. Each command runs in its own worker, which **resets to a clean slate
between commands** — so there is no state bleed from one caller to the next. The
server is the only ingress; workers never bind a public port.

Shiitake is generic: it has no knowledge of any particular application. You bring
the toolchain image, drop in the worker binary as its entrypoint, and the server
dispatches commands to the pool.

> 📖 Full documentation and a Kubernetes quickstart live at
> **[tenzailabs.github.io/shiitake](https://tenzailabs.github.io/shiitake/)**.

## The problem Shiitake solves

Plenty of systems need to run **short-lived, untrusted commands** on behalf of
someone else — user-submitted code, AI-agent tool calls, CI steps, scraped build
scripts. Doing that safely is awkward:

- Running them **in your own process or container** gives no isolation: one
  command's leftover processes, temp files, or a fork bomb bleed into the next
  caller — or take the whole service down.
- Spawning a **Pod or Job per command** means handing your workload Kubernetes
  pod-create rights and eating per-task scheduling latency every single time.
- **Large output** buffered in memory turns a chatty command into an OOM of
  *your* service.

Shiitake puts a fixed pool of **resident, resource-bounded worker containers**
behind a single authenticated HTTP server. You `POST` a command and get a handle
back; the server hands it to an idle worker that runs it in isolation, streams
output straight to disk, and **resets to a clean slate before the next command**
(recycling into a fresh container if a reset can't be trusted). No pod-create
rights, no per-task scheduling latency, no state bleed — and a runaway command
can only take down its own worker.

## Highlights

- **Per-command isolation with a clean slate.** Each command runs in its own
  resource-bounded worker container — it can't see or touch another command's
  processes, files, or output. Between commands the worker resets (SIGKILLs
  leftover processes, empties the configured scratch paths, removes SysV IPC),
  and if a reset can't be trusted it recycles into a fresh container rather than
  serving dirty.
- **No pod-create rights, no per-task latency.** A fixed pool of workers is
  already running and waiting; you just `POST` a command and get a handle back.
  No RBAC to spawn a Job or Pod per task, no per-task scheduling delay.
- **OOM contained, never silent.** Memory and CPU are enforced at the container
  level, so an allocation bomb kills only its own worker — the server and every
  other worker keep running. The kill is detected from the kubelet's container
  status and surfaced as `oom_container`, never lost.
- **Zero-copy output capture.** The worker redirects the command's stdout/stderr
  straight into per-stream capture files via inherited fds — the kernel writes
  to disk, so neither the worker nor the server holds output in memory. The
  server reads it back with HTTP range support, and output sizes are reported as
  metrics so a runaway command is observable rather than truncated.
- **Identity-agnostic privilege drop.** An `/exec` request may carry a `drop_to`
  directive (`uid`, `gid`, supplementary gids, umask); the worker applies it in
  the post-fork `pre_exec` hook before exec. Shiitake never decides identities —
  an embedding layer maps its own auth to `drop_to`.
- **OpenTelemetry built in.** The server emits traces (a span per command) and
  metrics (exit cause, duration, memory/CPU, output sizes, pool occupancy) over
  OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

## How it works

One server process is the single entry point. It accepts `POST /api/v1/exec`,
picks an idle worker from the pool, and hands the command to it over an in-pod
WebSocket on the loopback dispatch port. The worker runs it as `bash -c` in its
own process group, streams stdout/stderr straight to capture files on the shared
volume, reports the result, then resets its sandbox before serving the next
command.

```
POST /api/v1/exec  →  Server (only ingress)  →  Worker pool  →  one command per worker
                                                  ↑               ↓ resets between commands
                                                  └───── re-advertised idle ─────┘
```

Tracing a single command there and back:

1. **You POST the command** with a bearer token to the server — the single,
   authenticated ingress. Workers never face the network.
2. **The server schedules it** onto an idle worker over the loopback dispatch
   WebSocket (`429` if every worker is busy — Shiitake never silently queues).
3. **You get a handle back immediately** (`202 Accepted`) — you don't hold the
   connection open while the command runs.
4. **The worker runs it** as `bash -c` in its own process group, inside a
   resource-bounded container it can't escape.
5. **Output streams to the shared volume** via inherited fds — the kernel writes
   the capture files, so nothing is buffered in memory.
6. **The worker reports and resets** — exit code, cause, and byte counts go back
   to the server, which marks the handle done; the worker then sweeps processes,
   clears scratch, and re-advertises idle (recycling if the reset can't be
   trusted).
7. **You read the result back** with `GET /api/v1/exec/{handle}` and
   `GET …/stdout` — served from the shared volume with HTTP `Range` support.

The full architecture, with a topology diagram and RBAC, is in the
[documentation](https://tenzailabs.github.io/shiitake/docs.html#architecture).

## Quickstart

Deploy a Shiitake Pod — one server, one worker — to any cluster, then dispatch a
command over HTTP. Built images only, no `cargo` required; works on a local
`kind` or `k3d` cluster.

**1. Build a runnable worker image.** The published `shiitake-worker` is just the
static binary (a `scratch` image), so pair it with a base that has `bash` — commands
run as `bash -c` — plus whatever tools your commands need:

```dockerfile
FROM alpine:3
RUN apk add --no-cache bash
COPY --from=ghcr.io/tenzailabs/shiitake-worker:latest \
     /usr/local/bin/shiitake-worker /usr/local/bin/shiitake-worker
ENTRYPOINT ["/usr/local/bin/shiitake-worker"]
```

```bash
docker build -t shiitake-worker-demo:latest .
kind load docker-image shiitake-worker-demo:latest   # k3d: k3d image import …
```

**2. Apply the smallest possible deployment** — one Pod, the server (the only
ingress) and one worker, sharing a capture volume:

```yaml
# shiitake.yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: shiitake }
spec:
  replicas: 1
  selector: { matchLabels: { app: shiitake } }
  template:
    metadata: { labels: { app: shiitake } }
    spec:
      containers:
        - name: server
          image: ghcr.io/tenzailabs/shiitake-server:latest
          env:
            - { name: SHIITAKE_AUTH_TOKEN, value: dev-secret-token }
            - { name: SHIITAKE_CAPTURE_ROOT, value: /capture }
          ports: [{ containerPort: 8080 }]
          volumeMounts: [{ name: capture, mountPath: /capture }]
        - name: worker
          image: shiitake-worker-demo:latest
          imagePullPolicy: IfNotPresent
          env:
            - { name: SHIITAKE_WORKER_ID, value: worker-0 }
            - { name: SHIITAKE_CAPTURE_ROOT, value: /capture }
          volumeMounts: [{ name: capture, mountPath: /capture }]
      # SHIITAKE_CAPTURE_ROOT is a shared volume mounted into both containers: the
      # worker writes each command's stdout/stderr here, the server reads it back.
      volumes: [{ name: capture, emptyDir: {} }]
```

```bash
kubectl apply -f shiitake.yaml
kubectl rollout status deploy/shiitake
```

**3. Reach the API and run a command.** The server is the only ingress:

```bash
kubectl port-forward deploy/shiitake 8080:8080 &

# POST a command — get a handle back immediately (202 Accepted)
curl -s -H "Authorization: Bearer dev-secret-token" \
  -X POST localhost:8080/api/v1/exec \
  -d '{"command": "echo hello from a sandboxed worker"}'
# → {"handle":"01J…","started_at":"…"}

# read the captured stdout for that handle
curl -s -H "Authorization: Bearer dev-secret-token" \
  localhost:8080/api/v1/exec/01J…/stdout
# → hello from a sandboxed worker
```

`echo` is a bash builtin, so it needs nothing else; a command that calls an
external binary must carry a `PATH` in the request's `env` (the worker runs with a
cleared environment). OOM detection (needs pod-read RBAC), per-worker recycling,
and scaling the pool are covered in the
[Kubernetes deployment guide](https://tenzailabs.github.io/shiitake/docs.html#kubernetes).

## Crates

| Crate                   | Role                                                                                  |
| ----------------------- | ------------------------------------------------------------------------------------- |
| `shiitake-worker-api`   | Lib. The server↔worker contract: wire frames + the on-disk capture layout. The worker depends only on this. |
| `shiitake-server-api`   | Lib. The HTTP API request/response types — the contract between the server and any client. Pure types, no transport. |
| `shiitake-server`       | Lib + bin. axum HTTP API + WebSocket dispatcher + worker pool + Kubernetes OOM probe + OTel. Owns the capture-file layout and range reads. |
| `shiitake-worker`       | Bin. Connects to the dispatcher, runs one command at a time in its own process group, redirects output to capture files, reports resource usage, and resets between commands. |
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
| `SHIITAKE_CAPTURE_ROOT`   | `/capture`                       | Root for the stdout/stderr capture files (a shared volume — see below). |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (unset)                      | OTLP endpoint. When set, the server exports traces + metrics; otherwise logs to stdout only. |
| `OTEL_EXPORTER_OTLP_PROTOCOL`  | `http/protobuf`             | OTLP transport: `grpc`, `http/protobuf`, or `http/json` (all plaintext). |
| `POD_NAME` / `POD_NAMESPACE` | (downward API)                | Used by the Kubernetes container-OOM probe.        |

### Worker

| Variable                 | Default            | Purpose                                               |
| ------------------------ | ------------------ | ----------------------------------------------------- |
| `SHIITAKE_WORKER_ID`     | `worker-unknown`   | Identifier advertised to the dispatcher.              |
| `SHIITAKE_DISPATCH_PORT` | `8090`             | Dispatcher port. The host is always loopback (`127.0.0.1`) — server and workers share the pod network namespace. |
| `SHIITAKE_CAPTURE_ROOT`  | `/capture`         | Must match the server's capture root (the shared volume). |
| `SHIITAKE_RESET_PATHS`   | (empty)            | Comma-separated scratch directories emptied between commands (e.g. `/tmp,/var/tmp,/dev/shm`). Empty means "clear nothing"; list only per-command scratch and omit anything that must persist. |
| `SHIITAKE_RESTART_AFTER` | `0`                | Exit (recycle the container) after this many commands. `0` = stay resident; `1` = a fresh container per command; `N` = a full teardown every `N`. Bounds anything the in-process reset can't scrub. |

`SHIITAKE_CAPTURE_ROOT` is a **shared volume**: it is mounted into the server and
every worker at the same path. The worker writes each command's stdout/stderr
into capture files there; the server reads them back over HTTP. Use an `emptyDir`
(lives and dies with the Pod) or a persistent volume if output must outlive it.

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

  The published worker image is a `scratch` image (just the binary), so pair it
  with a base that has `bash` plus whatever tools your commands need.

Run one server container and N worker containers in a single **Pod** that shares
the pod network namespace (so workers reach the dispatcher on `127.0.0.1`) and a
capture volume (`SHIITAKE_CAPTURE_ROOT`, mounted into the server and every worker
at the same path — an `emptyDir`, or a persistent volume if you want output to
survive the Pod). Workers stay **resident** and reset between commands; give each
a per-container `restartPolicy: Always` (Kubernetes ≥ 1.35) so a worker that
reaches its `SHIITAKE_RESTART_AFTER` quota or fails a reset recycles into a fresh
container on its own. The worker always exits `0`, so a recycle reads as
`Completed`, never `CrashLoopBackOff`. `tests/run.sh` deploys exactly this
topology; see the
[Kubernetes deployment guide](https://tenzailabs.github.io/shiitake/docs.html#kubernetes)
for a full spec and the pod-reader RBAC the OOM probe needs.

## Developing locally

For hacking on Shiitake itself, run the server and a worker from source with
Cargo, then dispatch a command. Outside Kubernetes the container-OOM probe and
per-command container recycle are inert — the worker simply resets in-process.

```bash
# Terminal 1 — server
SHIITAKE_AUTH_TOKEN=dev-token SHIITAKE_CAPTURE_ROOT=/tmp/capture cargo run --bin shiitake-server

# Terminal 2 — one worker; joins the pool and stays resident
SHIITAKE_WORKER_ID=worker-0 SHIITAKE_CAPTURE_ROOT=/tmp/capture cargo run --bin shiitake-worker

# Terminal 3 — dispatch a command, get a handle back
curl -sX POST localhost:8080/api/v1/exec \
  -H "Authorization: Bearer dev-token" \
  -d '{"command": "echo hi"}'
```

The full development environment (toolchain, `mise`-managed e2e tooling), the
test commands CI runs, and versioning are all in
[CONTRIBUTING.md](CONTRIBUTING.md).

## License

This project is licensed under the [Apache License 2.0](LICENSE).
