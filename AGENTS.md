# AGENTS.md — Shiitake

Guidance for coding agents in this repository. Update it when architecture,
the wire protocol, or build/test flow changes.

## Architecture

Shiitake is a generic command dispatcher. `shiitake-server` exposes an HTTP
session API and keeps worker pools by profile. `shiitake-worker` dials its
internal dispatch WebSocket and advertises a unique worker id, a profile, and
optional Kubernetes location. Creating a session reserves one worker and starts
a persistent Bash shell under one fixed `DropTo` identity. Releasing it ends
the shell, cancels jobs, resets the worker sandbox, and requeues the worker.
The server and workers can share a pod or run in separate pods; the binaries
and dispatch protocol are the same.

- `shiitake-worker-api`: server/worker frames, ids, `DropTo`, tagged binary
  output chunks, and local capture path helpers. The worker depends only on
  this API crate.
- `shiitake-server-api`: pure public HTTP request/response types.
- `shiitake-server`: HTTP routes, dispatch authorization, profile pool,
  session/handle registries, local output spool and range reads, OOM probe,
  OpenTelemetry.
- `shiitake-worker`: resident dispatch client, persistent PTY shell,
  stateful and parallel jobs, local temporary output tailing, sandbox reset,
  cgroup readings for legacy commands.
- `clients/shiitake-rs`: async reqwest session client.
- `clients/shiitake-py`: async and synchronous session clients; optional
  `[pty]` terminal support.
- `tests`: Helm/k3d end-to-end suite for single- and two-pod layouts, default
  and isolated profiles, HTTP, client, terminal, streaming, and pod failures.

## Public API and command semantics

All authenticated public routes are under `/api/v1`: `GET /profiles`,
`POST/GET/DELETE /sessions[/{id}]`,
`POST /sessions/{id}/commands`,
`GET/DELETE /sessions/{id}/commands/{handle}`,
`GET /sessions/{id}/commands/{handle}/{stdout,stderr}`, and
`GET /sessions/{id}/terminal` (WebSocket). `/health` and `/ready` are
unauthenticated. Legacy `/exec` and `/pty` public routes were removed in the
0.5.0 session API. The internal `/dispatch` listener has a separate bearer
credential from the public API.
Dispatch protocol version 2 streams command output: the worker sends
`Hello.protocol_version = 2`, waits for the server's `HelloAck`, and only then
accepts work. The server rejects older Hello frames before pool registration.
This deliberately fences mixed-version rollouts that would otherwise lose
output when one side expects a shared capture volume.

At creation a session fixes its profile, working directory, environment,
`DropTo`, and terminal size. Stateful commands source a script in the live
Bash process: shell state persists, and these jobs run in order. Parallel jobs
run in child Bash processes and may overlap; they inherit the last recorded
working directory and exported environment, but do not change parent state.
Stateful completion requires the shell's per-command PTY marker after `source`
returns and a valid cwd/environment/status snapshot; the capture status file
alone is writable by the session user and is never proof of completion.
A terminal attachment is detachable. It cannot overlap a stateful job, and at
most one attachment is active at a time. Command strings use shell syntax.
The worker clears the inherited environment; pass `PATH` for external tools.
The sweeper releases idle sessions after the running TTL (one hour in the
binary); session-status requests and command submissions/completions refresh that clock, while
running commands and attached terminals prevent expiry. Explicit release is
still the normal lifecycle. Clients do not retry non-idempotent session-create
or command-submit POSTs after ambiguous transport failures.

Workers send tagged binary chunks for stdout, stderr, and terminal output
through `/dispatch`; the server spools command output locally for HTTP range
reads. Worker capture files are temporary and local to the worker container.
The internal legacy `Execute` frame also sends its captured stdout/stderr over
dispatch before Result, then purges its local files.
No capture volume is shared between server and worker pods. The server purges
orphaned spool files on startup and old handles by TTL; storage is unbounded.
The server-side spool must therefore have enough capacity for expected output.

## Build and test

```bash
cargo +nightly fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
uv run --project clients/shiitake-py --group dev pytest clients/shiitake-py/tests
bash tests/build.sh && bash tests/setup.sh && bash tests/run.sh
SHIITAKE_E2E_TOPOLOGY=two-pod bash tests/setup.sh
SHIITAKE_E2E_TOPOLOGY=two-pod bash tests/run.sh
```

`mise install` provides k3d, kubectl, Helm, Python, and uv. Rust is pinned in
`rust-toolchain.toml`; nightly rustfmt is used by CI for unstable import
formatting options. `ci.yml` runs format, clippy, test, and version checks;
`test.yml` runs both topology variants. The Cargo workspace and Python
package versions must move together. The first PR after a release must bump
both above the latest release; a later PR in the same cycle inherits that
version unless its scope needs a larger bump.

## Operational gotchas

- Workers are resident. Reset sweeps every other PID when the worker is PID 1,
  clears configured scratch paths, and removes created account/IPC state.
  `SHIITAKE_RESTART_AFTER` counts completed sessions, not commands; `0` keeps
  the worker resident. A worker exits 0 on planned recycle.
- Do not clear folders that a caller intends to persist across sessions.
  `SHIITAKE_RESET_PATHS` is supplied by the embedding deployment.
- The pool selects idle workers in FIFO order within each profile. Keep this
  least-recently-used rotation when returning released workers.
- `/health` is liveness and stays healthy for an empty pool. `/ready` needs
  `SHIITAKE_MIN_READY_WORKERS` registered workers, including allocated ones.
  The two-pod dispatch Service uses `publishNotReadyAddresses` so workers can
  register before the server becomes ready.
- Worker ids must be unique. In a worker Deployment use pod name from the
  downward API; `Hello` location points the server OOM probe at the right pod.
- The server pings idle and allocated workers and evicts silent connections.
  A worker that loses its server mid-session kills work and exits for a fresh
  container; idle workers reconnect. The lease timeout catches partitions.
- In two-pod mode, each profile gets its own worker Deployment. A profile can
  omit application shared volumes. In single-pod mode profile containers
  still share the pod network namespace and service account boundary.
- A root command can read its worker's dispatch credential. Production uses
  `SHIITAKE_PROFILE_TOKENS` to bind additional profiles to distinct tokens;
  the server rejects a Hello that claims another profile. Keep that mapping
  aligned with each worker Deployment. Workers within one profile still share
  its trust domain.
- Per-command cgroup CPU/memory readings cannot be attributed accurately to
  parallel jobs sharing a worker container, so those fields are omitted for
  session commands. OOM is detected from Kubernetes container status when a
  worker disconnects.
- The server spools output on its own volume; a restart orphans files and
  invalidates in-memory handles. `reconcile_capture` clears old files once
  at startup. Range reads use the server's local spool.
- Both published binaries are static musl builds. Keep `native-tls` and
  OpenSSL out of the dependency graph; `telemetry::init` pins rustls's ring
  provider because aws-lc-rs is also compiled through reqwest.
- The server aborts on any panic, including background telemetry task panics.
