# 🍄 Shiitake

Shiitake runs shell sessions in a pool of resident worker containers. The HTTP
server allocates one worker to each session. A caller chooses a worker profile
and, optionally, a `drop_to` identity when creating the session; commands and
an interactive terminal then share that shell until the caller releases it.
Workers connect to the server over an internal WebSocket and never bind an
inbound port.

A session can run several commands at once. **Stateful** commands run in the
persistent Bash shell, so `cd`, `export`, and shell variables survive. They are
serialized. **Parallel** commands run concurrently as child Bash processes,
using the shell's latest working directory and exported environment snapshot.
They cannot change the parent shell's state. Terminal attachment is optional
and detachable; closing the terminal connection does not release the session.

Worker profiles let a deployment offer different container shapes. For example,
a `default` profile can mount `/shared`, while an `isolated` profile has no
application volume. In the two-pod topology, each profile gets its own worker
Deployment and pod-scoped network policy. A single-pod topology is available
for smaller deployments, but all its containers share a pod network namespace.
The caller decides permissions within any shared folder.

## API

The public API lives under `/api/v1`. All routes except `/health` and `/ready`
require `SHIITAKE_AUTH_TOKEN`. `/dispatch` is a separate worker-only listener
protected by `SHIITAKE_DISPATCH_TOKEN`.

| Method | Path | What it does |
| --- | --- | --- |
| `GET` | `/profiles` | Connected profiles and their idle/allocated worker counts. |
| `POST` | `/sessions` | Allocate a worker and start its shell. Returns `201`, or `400` for invalid input, `404` for an unknown profile, `429` when that profile is busy, and `502` if the worker fails during creation. |
| `GET` | `/sessions/{id}` | Session state and assigned worker. |
| `DELETE` | `/sessions/{id}` | Close the shell, cancel its jobs, reset the worker, and release it. |
| `POST` | `/sessions/{id}/commands` | Submit a stateful or parallel command; returns a handle (`202`). |
| `GET` | `/sessions/{id}/commands` | List retained commands in this session, oldest first, with status and output byte counts. |
| `GET` | `/sessions/{id}/commands/{handle}` | Command status, exit cause, and output byte counts. |
| `DELETE` | `/sessions/{id}/commands/{handle}` | Cancel a command. |
| `GET` | `/sessions/{id}/commands/{handle}/stdout` | Read output, including HTTP `Range` requests. |
| `GET` | `/sessions/{id}/commands/{handle}/stderr` | Read stderr by byte range. |
| `GET` | `/sessions/{id}/terminal` | Attach an interactive WebSocket terminal. Binary messages are raw terminal bytes; send `{"op":"resize","cols":80,"rows":24}` as text to resize. |
| `GET` | `/health`, `/ready` | Unauthenticated liveness and pool readiness. |

Create a session, wait for a command, read its output, and release the worker:

```bash
set -e
base=http://localhost:8080/api/v1
until curl -fsS "$base/ready" >/dev/null 2>&1; do sleep 1; done
sid=$(curl -fsS -H "Authorization: Bearer $SHIITAKE_AUTH_TOKEN" \
  -H 'Content-Type: application/json' -d '{"profile":"default","env":{"PATH":"/usr/bin:/bin"}}' \
  "$base/sessions" | python3 -c 'import json,sys; print(json.load(sys.stdin)["session_id"])')
handle=$(curl -fsS -H "Authorization: Bearer $SHIITAKE_AUTH_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"command":"echo hello","mode":"stateful"}' \
  "$base/sessions/$sid/commands" |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["handle"])')
while :; do
  state=$(curl -fsS -H "Authorization: Bearer $SHIITAKE_AUTH_TOKEN" \
    "$base/sessions/$sid/commands/$handle" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
  [ "$state" = running ] || break
  sleep 0.2
done
if [ "$state" = completed ]; then
  curl -fsS -H "Authorization: Bearer $SHIITAKE_AUTH_TOKEN" \
    "$base/sessions/$sid/commands/$handle/stdout"
else
  echo "command ended as $state" >&2
fi
curl -fsS -X DELETE -H "Authorization: Bearer $SHIITAKE_AUTH_TOKEN" \
  "$base/sessions/$sid"
```

Session creation accepts `profile` (default `default`), `workdir`, `env`,
`drop_to`, `cols`, and `rows`. `drop_to` accepts `uid`, `gid`,
`supplementary_gids`, `umask`, and optionally `name` and `create_home`.
The environment may contain at most 128 entries and 64 KiB of key/value data;
`workdir` may be at most 4096 bytes.
Session IDs have the form `s-` plus 16 lowercase letters or digits; command
handles use `c-` plus the same short alphabet. Both are opaque random IDs.
Commands accept a shell `command` string and `mode` (`stateful` by default or
`parallel`). There is no command deadline: poll the handle, read output while
it runs, or cancel it with `DELETE`. Identity is fixed for a
session; callers cannot change it on a command. The worker clears its own
environment before starting the shell, so include `PATH` for external programs.
Release sessions explicitly. As a backstop for lost responses and abandoned
clients, the server releases an idle session after one hour without a session-status
request, command submission or completion, or terminal attachment. Running commands and
attached terminals keep their session allocated until they finish or detach.

For a job whose wall time is unknown, keep the same session and handle. For
example, after creating `sid` as above:

```bash
handle=$(curl -fsS -H "Authorization: Bearer $SHIITAKE_AUTH_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"command":"echo started; sleep 3600; echo finished","mode":"parallel"}' \
  "$base/sessions/$sid/commands" |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["handle"])')
curl -fsS -H "Authorization: Bearer $SHIITAKE_AUTH_TOKEN" \
  "$base/sessions/$sid/commands"                         # discover handles
curl -fsS -H "Authorization: Bearer $SHIITAKE_AUTH_TOKEN" \
  "$base/sessions/$sid/commands/$handle"                 # status: running
curl -fsS -H "Authorization: Bearer $SHIITAKE_AUTH_TOKEN" \
  "$base/sessions/$sid/commands/$handle/stdout"          # output so far
curl -fsS -X DELETE -H "Authorization: Bearer $SHIITAKE_AUTH_TOKEN" \
  "$base/sessions/$sid/commands/$handle"                 # cancel this command
curl -fsS -H "Authorization: Bearer $SHIITAKE_AUTH_TOKEN" \
  "$base/sessions/$sid/commands/$handle"                 # poll for cancelled
curl -fsS -X DELETE -H "Authorization: Bearer $SHIITAKE_AUTH_TOKEN" \
  "$base/sessions/$sid"                                  # release the worker
```

Cancellation sends SIGTERM to a parallel command's process group, then
SIGKILL after a two-second grace period. A stateful command receives an
interrupt; if it does not stop, the worker closes that session. Status and
output remain available through the handle until the server's retention period.

Output is streamed from worker to server over the authenticated dispatch
WebSocket. The server spools stdout and stderr locally for range reads. Workers
use temporary files only inside their own container while a command runs; no
capture volume is shared between pods. The server clears orphaned output at
startup and purges old handles after its retention period. Output storage is
unbounded, so size the server's local volume for expected workloads.

## Deployment and configuration

The server and worker binaries use the same wire protocol in both supported
layouts:

- **Single pod:** server and worker containers share one pod. The dispatcher
  listens on loopback. The server has a local `emptyDir` output spool.
- **Two pods:** server and worker profiles run in separate Deployments. Workers
  reach `/dispatch` through a Service. Only the server mounts the output spool;
  a separate application volume may be mounted in selected worker profiles.
  The dispatch Service uses `publishNotReadyAddresses` to avoid a startup
  deadlock with the server's readiness probe. The server ServiceAccount needs
  `get` permission on pods to identify container OOM kills.

The Helm test chart in `tests/chart` demonstrates both layouts and two profiles.
The worker image is a static binary suitable for `COPY --from`; the test chart
puts it in a toolchain image. Worker ids must be unique across the pool.

Key server variables: `SHIITAKE_HOST`, `SHIITAKE_PORT`,
`SHIITAKE_DISPATCH_HOST`, `SHIITAKE_DISPATCH_PORT`,
`SHIITAKE_DISPATCH_TOKEN`, `SHIITAKE_AUTH_TOKEN`,
`SHIITAKE_PROFILE_TOKENS` (JSON map of additional profile names to distinct
dispatch tokens),
`SHIITAKE_CAPTURE_ROOT` (server-local spool, default `/capture`),
`SHIITAKE_DEFAULT_WORKDIR`, and `SHIITAKE_MIN_READY_WORKERS`.

Key worker variables: `SHIITAKE_WORKER_ID`, `SHIITAKE_PROFILE` (default
`default`), `SHIITAKE_DISPATCH_URL`, `SHIITAKE_DISPATCH_TOKEN`,
`SHIITAKE_CAPTURE_ROOT` (worker-local temporary output),
`SHIITAKE_RESET_PATHS`, `SHIITAKE_RESTART_AFTER`, and
`SHIITAKE_LEASE_TIMEOUT`. `SHIITAKE_RESTART_AFTER` counts completed sessions;
`0` keeps a worker resident. The worker resets its sandbox when a session ends.
The Kubernetes container restart policy can bound state that reset cannot
clean. Set scratch paths to exclude data that must persist between sessions.

`/health` stays healthy while the server serves requests. `/ready` becomes
ready once the configured number of workers have registered, whether idle or
allocated. The server probes Kubernetes for `OOMKilled` when a worker drops;
the worker cannot reliably report a container OOM itself. The server exports
OpenTelemetry traces and metrics when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
Per-command CPU and memory attribution is omitted for parallel jobs because
they share a container cgroup.

**Trust boundary:** separate worker pods can have separate mounts and network
policy. A root shell in a worker container can read that container's dispatch
credential. Bind each profile to a distinct token with
`SHIITAKE_PROFILE_TOKENS`; the server checks that `Hello.profile` matches the
credential used to connect. Commands in the same profile still share that
profile's trust domain.

## Clients and tests

`clients/shiitake-py` provides async and sync session clients, plus optional
terminal support via its `[pty]` extra. `clients/shiitake-rs` provides an async
HTTP session client. Both clients expose explicit session release.

```bash
cargo +nightly fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
uv run --locked --project clients/shiitake-py --group dev pytest clients/shiitake-py/tests
bash tests/build.sh && bash tests/setup.sh && bash tests/run.sh
SHIITAKE_E2E_TOPOLOGY=two-pod bash tests/setup.sh
SHIITAKE_E2E_TOPOLOGY=two-pod bash tests/run.sh
```

The cluster suite needs Docker, k3d, kubectl, Helm, Python, and uv. `mise
install` provides the local tools; `rust-toolchain.toml` pins Rust. Releases
keep the Cargo workspace and Python package versions in lockstep.
