# AGENTS.md — Shiitake

Guidance for coding agents working in this repository. Keep it current when
architecture, the wire protocol, or the build/test flow changes.

## What this is

A generic, open-source command dispatcher: `shiitake-server` (HTTP + WebSocket
dispatcher + worker pool) hands commands to `shiitake-worker` processes that
serve commands resident, resetting their sandbox between each. It has no
application-specific code — privilege and
identity are expressed only as the generic `drop_to` directive on the wire.

## Layout

Directory names match crate names (Rust crates and the Python package share
one tree).

Two symmetric `*-api` crates hold the contracts (pure types, no transport); the
server + worker implement them, and the clients under `clients/` consume them.

```
shiitake-worker-api/ shiitake-worker-api — lib. The server↔worker contract: wire
          frames + the on-disk capture layout (capture.rs). The worker depends only on this.
shiitake-server-api/ shiitake-server-api — lib. The HTTP API request/response
          types (ExecRequest, SpawnResponse, StatusResponse, HandleStatus, ExitCause).
shiitake-server/  shiitake-server — lib + bin. http/ (routes, auth, range reads,
          dispatch), pool/ (registry, k8s_status OOM probe), metrics.rs +
          telemetry.rs (OTel), serve.rs
shiitake-worker/  shiitake-worker — bin only. client.rs (connect/Hello/serve-loop),
          reset.rs (between-command sandbox reset: process sweep, scratch clear, IPC),
          exec.rs (bash -c, own process group, fd-redirect to capture files),
          cgroup.rs (memory.peak/cpu.stat/limit reads for resource metrics)
clients/shiitake-rs/  shiitake-rs — lib. Async reqwest client over the HTTP API.
clients/shiitake-py/  Python client for the HTTP API (httpx, policy-free).
tests/    k3d-based suite: a Helm chart (chart/) deploying server + N workers,
          driven by build.sh + setup.sh + run.sh; HTTP-level checks in
          test_exec.py and the shiitake-py client e2e in test_e2e.py
```

The server↔worker wire frames + capture layout live in `shiitake-worker-api`
(the worker depends only on it). The HTTP API types live in `shiitake-server-api`,
which the server and every client depend on — the server to implement the API,
the clients (`clients/shiitake-rs`, `clients/shiitake-py`) to call it. Clients
add only their transport on top of those shared types.

The public HTTP API is served under `/api/v1` (`build_api_router` nests the
routes); the worker dispatch endpoint (`/dispatch`) is a separate internal
router on the loopback dispatch port. Both binaries take their config via clap,
with `SHIITAKE_*` env fallbacks — no ad-hoc `env::var` reads.

## Build & test

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace               # unit + in-process integration tests
bash tests/build.sh && bash tests/setup.sh && bash tests/run.sh  # full cluster e2e (docker, k3d, kubectl, helm, python3, uv)
```

Local + CI e2e tooling (k3d, kubectl, python) is managed by `mise` (`mise.toml`)
— run `mise install`; the test workflow uses `jdx/mise-action`. The Rust
toolchain is the one exception, pinned in `rust-toolchain.toml`.

CI runs all of the above (`.github/workflows/{ci,test}.yml`). `ci.yml` runs
format, clippy, test, and version as four parallel jobs; clippy and test share a
`Swatinem/rust-cache` keyed on `rust-toolchain.toml` (a toolchain bump starts
from a clean cache). The toolchain is pinned in `rust-toolchain.toml` (edition
2024, Rust 1.96.0); the format check is its own job under nightly because
`rustfmt.toml` uses the unstable `imports_granularity` / `group_imports`
options. Each crate declares its own dependencies (no `[workspace.dependencies]`),
kept current with `cargo upgrade --incompatible`. Release Drafter maintains a draft release and autolabels PRs.
Publishing a release triggers `release.yml`, which stamps the workspace `version`
from the release tag (in-place, no commit) before building, then pushes the
server image to GHCR and the worker binary as release assets.

## Versioning

The version is managed in-code and kept in lockstep across the two published
artifacts:

- Cargo workspace — `Cargo.toml` `[workspace.package] version` (server image +
  worker binary).
- Python client — `clients/shiitake-py/pyproject.toml` `version`.

A PR is responsible for bumping the version (semver: major / minor / patch,
chosen by the scope of its own changes) **if the target branch has not already
been bumped since the last published release**. In other words: the first PR
landing after a release bumps both versions; later PRs in the same release cycle
inherit that already-bumped version and leave it alone — only raise it further
if their change warrants a larger bump than what's already pending. Bump both
files together so the Rust and Python artifacts stay on the same version. The
release tag must then match the in-code version.

The `version` job in `ci.yml` (`.github/scripts/check-version.sh`) enforces this
as a single invariant: the in-code version must be **strictly greater** than the
last published release (so the first PR of each cycle is forced to bump), and
`Cargo.toml` must equal `pyproject.toml` (lockstep). CI verifies *that* a bump
happened, not whether its size matches the change's scope — that stays a reviewer
call. Before the first release the bump check has no baseline and is skipped;
once a release exists it gates every PR. Wire it into branch protection as a
required check.

## Gotchas

- **The worker clears the command environment.** `exec.rs` runs `bash -c` with
  `env_clear()` then applies only the request's `env`. A command that calls an
  external binary must be given `PATH`; bash builtins (`echo`) work without it.
  The tests pass `PATH` explicitly — keep doing so.
- **`/exec` takes a `command` string**, run verbatim as `bash -c <command>`.
  Use ordinary shell syntax for pipes, redirects, and multi-statement scripts;
  the caller does its own quoting (there is no argv array — the worker always
  goes through a shell, so an argv form would just be re-joined).
- **Workers are resident.** A worker connects once and serves commands in a
  loop, resetting its sandbox between each instead of exiting (`reset.rs`: sweep
  every leftover process — the worker is its container's pid 1, so it SIGKILLs
  all other pids in the namespace; empty the configured `SHIITAKE_RESET_PATHS`
  scratch dirs; best-effort SysV IPC removal). Staying resident avoids the
  kubelet CrashLoopBackOff that per-command container exits incur. The
  clean-slate-per-command property now depends on `reset` being equivalent to a
  container teardown — if you change exec or the mounts, keep `reset` in sync.
  Shiitake never assumes a path layout: the embedding layer lists the scratch
  paths to clear and omits anything that must persist across commands.
- **`SHIITAKE_RESTART_AFTER` re-adds a full-teardown layer.** After N commands
  the worker exits (process ends → fresh container via the container
  `restartPolicy: Always`), bounding anything `reset` can't scrub. `0` = never
  (pure resident); `1` = exit after every command (a fresh container each time);
  `N` = every N. The worker **always exits 0** — quota hit or session error —
  so a recycle reads as `Completed`, never CrashLoopBackOff. The command's own
  exit status rides the Result frame, never the worker process exit code. The
  per-container `restartPolicy` needs k8s ≥ 1.35 (ContainerRestartRules).
- **Dispatch is loopback-only.** The worker takes only `SHIITAKE_DISPATCH_PORT`
  and always dials `ws://127.0.0.1:<port>/dispatch`; server and workers must
  share a network namespace.
- **Worker liveness has two layers.** Each worker's read loop runs for its whole
  connection (idle and in-flight), so a clean disconnect is detected *immediately*
  (the `select!` on `stream.next()` breaks → `handle_worker_drop`). On top of that,
  `run_keepalive` pings idle workers every 10s and evicts any silent for >30s — a
  hung worker that never sends a FIN won't be caught by the read loop, so the
  ping/pong (workers pong in their idle-wait loop) is the backstop. Eviction fires
  a per-worker `shutdown` `Notify` that ends the read loop. Sinks are
  `Arc<Mutex<WsSink>>` so the pinger never holds the pool lock across a send.
- **Everything is a static musl binary.** Both the server and the worker build
  for `*-unknown-linux-musl` with `musl-gcc`. The server image
  (`Dockerfile.server`) is Alpine; the worker image (`Dockerfile.worker`) is the
  bare static binary on `scratch` for downstream `COPY --from`; the e2e worker
  (`tests/Dockerfile.worker`) bakes that binary into `python:3-alpine`.
- **rustls on `ring` only — no aws-lc-rs / OpenSSL / native-tls.** `kube`
  (`rustls-tls`) and `tokio-tungstenite` (`rustls-tls-webpki-roots`) use rustls;
  `opentelemetry-otlp` enables all three transports (`http-proto`, `http-json`,
  `grpc-tonic`) but **plaintext only** (`reqwest-client`, no `reqwest-rustls*` /
  `tls-*` features) — those TLS features drag in `aws-lc-rs`, which breaks the
  static musl link *and* leaves rustls unable to auto-pick a provider. Because
  several crates pull rustls with differing provider features, `telemetry::init`
  pins the ring provider via `CryptoProvider::install_default()`. Keep the graph
  free of `aws-lc-rs`/`native-tls`/`openssl`/`security-framework`
  (`cargo tree -i aws-lc-rs -e no-dev` must be empty); don't add a TLS feature to
  the OTLP exporter or switch any crate to `aws-lc-rs` without revisiting this.
- **Capture is shared between server and workers.** The worker redirects the
  command's stdout/stderr fds straight into `SHIITAKE_CAPTURE_ROOT/<handle>/{stdout,stderr}`
  (plain files, no buffering in the worker), and the server reads them back with
  HTTP range support and `stat`s them for byte counts. Both must mount the same
  volume at the same path. Storage is unbounded (capped only by the volume);
  output size and capture-volume free space are exported as metrics rather than
  enforced as a cap.
- **Telemetry lives only in the server.** Workers report per-command resource
  usage (cgroup `memory.peak`/`cpu.stat`/limit) on the `ResultFrame`; the server
  turns that plus its own timing into OTel traces (one `shiitake.exec` span per
  command) and `shiitake_`-prefixed metrics, exported via OTLP when
  `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Keep the worker free of OTel deps —
  that's what keeps the drop-in binary lean.
- **The server aborts on any panic.** `main` installs a panic hook that calls
  `process::abort()`, so a panic on any thread — notably OpenTelemetry's
  background export task — takes the whole process down (non-zero exit, pod
  restarts) instead of leaving it running degraded. Don't rely on tokio
  isolating a panicking task here; a panicking handler crashes the server.
- **OOM is detected externally, never in the worker.** A command shares the
  worker container's cgroup, so a container OOM can kill the worker itself; the
  reliable signal is the kubelet's container `OOMKilled` status, read by the
  server's `k8s_status` probe when the worker connection drops (→ `oom_container`).
  Don't reintroduce in-worker OOM counters. Likewise there are no per-command
  rlimits: the container's k8s `resources.limits` bound it.
