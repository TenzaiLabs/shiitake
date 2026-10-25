#!/usr/bin/env python3
"""End-to-end checks against a live shiitake-server + worker pool.

Talks to the server over HTTP exactly the way any client would: create a
session, submit commands, poll their handles, then release the session.
Standard library only — no third-party packages.

Config via env:
  SHIITAKE_E2E_URL     base URL of the server (default http://127.0.0.1:18080)
  SHIITAKE_E2E_TOKEN   bearer token (default "e2e-secret-token"; empty = no auth)
  SHIITAKE_E2E_WORKERS worker-pool size, used by the concurrency check (default 8)

The worker runs each command as `bash -c <cmd>` with an empty environment, so
every command that needs an external binary (python3, seq, …) must pass PATH in
`env`. `echo` and other bash builtins work without it.
"""

import collections
import json
import os
import sys
import time
import unittest
import urllib.error
import urllib.request

BASE = os.environ.get("SHIITAKE_E2E_URL", "http://127.0.0.1:18080").rstrip("/")
TOKEN = os.environ.get("SHIITAKE_E2E_TOKEN", "e2e-secret-token")
POOL_SIZE = int(os.environ.get("SHIITAKE_E2E_WORKERS", "8"))

PATH_ENV = {"PATH": "/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"}


def _request(method, path, body=None, token=TOKEN):
    url = f"{BASE}/api/v1{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {}
    if data is not None:
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            parsed = json.loads(raw) if raw else None
            return resp.status, parsed
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = raw
        return exc.code, parsed


# Per-command stats, accumulated during the run and printed as a summary at the
# end: what we executed, how long it took, and how many 429s the pool returned.
_COMMANDS = {}  # handle -> dict(cmd, spawned_at, retries_429, status, exit_cause, seconds)
_TOTAL_429 = 0
_SESSIONS = {}  # handle -> session id
_RELEASED = set()


def spawn(command, env=None, workdir=None, busy_deadline=90.0,
          profile="default", mode="parallel"):
    """Reserve a worker, submit a command, and remember its session for release."""
    global _TOTAL_429
    body = {"profile": profile, "env": env or {}}
    if workdir is not None:
        body["workdir"] = workdir
    deadline = time.monotonic() + busy_deadline
    retries = 0
    while True:
        status, payload = _request("POST", "/sessions", body)
        if status == 201:
            sid = payload["session_id"]
            status, command_payload = _request(
                "POST", f"/sessions/{sid}/commands",
                {"command": command, "mode": mode},
            )
            if status != 202:
                _request("DELETE", f"/sessions/{sid}")
                raise AssertionError(f"submit command -> {status}: {command_payload}")
            handle = command_payload["handle"]
            _SESSIONS[handle] = sid
            _COMMANDS[handle] = {
                "cmd": command,
                "spawned_at": time.monotonic(),
                "retries_429": retries,
                "status": None,
                "exit_cause": None,
                "seconds": None,
            }
            return handle
        if status == 429 and time.monotonic() < deadline:
            retries += 1
            _TOTAL_429 += 1
            time.sleep(0.5)
            continue
        raise AssertionError(f"POST /sessions -> {status}: {payload}")


def command_path(handle):
    return f"/sessions/{_SESSIONS[handle]}/commands/{handle}"


def wait_for_idle(count, deadline=60.0):
    """Block until at least `count` workers are idle (connected and free).

    Each new session reserves a worker. Gate concurrent session allocation on
    idle capacity so the commands can overlap instead of retrying after 429."""
    end = time.monotonic() + deadline
    while True:
        _, health = _request("GET", "/health", token=None)
        idle = (health or {}).get("workers_idle", 0)
        if idle >= count:
            return idle
        if time.monotonic() > end:
            raise AssertionError(f"only {idle} idle workers after {deadline}s; need {count}")
        time.sleep(0.5)


def get_status(handle):
    status, payload = _request("GET", command_path(handle))
    assert status == 200, f"GET command {handle} -> {status}: {payload}"
    return payload


def wait(handle, deadline=90.0):
    end = time.monotonic() + deadline
    while True:
        snap = get_status(handle)
        if snap["status"] != "running":
            rec = _COMMANDS.get(handle)
            if rec is not None and rec["seconds"] is None:
                rec["seconds"] = time.monotonic() - rec["spawned_at"]
                rec["status"] = snap["status"]
                rec["exit_cause"] = snap.get("exit_cause")
            if handle not in _RELEASED:
                released, details = _request("DELETE", f"/sessions/{_SESSIONS[handle]}")
                assert released == 200, f"release session -> {released}: {details}"
                _RELEASED.add(handle)
            return snap
        if time.monotonic() > end:
            raise AssertionError(f"handle {handle} still running after {deadline}s: {snap}")
        time.sleep(0.2)


def read_stream(handle, stream, range_header=None):
    """Raw GET of a capture stream. Returns (status, bytes). The endpoint
    serves the file directly, honouring an optional HTTP Range header."""
    url = f"{BASE}/api/v1{command_path(handle)}/{stream}"
    headers = {}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    if range_header:
        headers["Range"] = range_header
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def read_all(handle, stream):
    """Read a whole stream. Callers read after the handle is terminal, so the
    capture file is complete."""
    status, body = read_stream(handle, stream)
    assert status in (200, 206), f"read {stream} -> {status}: {body!r}"
    return body.decode(errors="replace")


def run(command, env=None, workdir=None, wait_deadline=90.0,
        profile="default", mode="parallel"):
    handle = spawn(command, env=env, workdir=workdir,
                   profile=profile, mode=mode)
    snap = wait(handle, deadline=wait_deadline)
    return snap, read_all(handle, "stdout"), read_all(handle, "stderr")


class HealthAndAuth(unittest.TestCase):
    def test_01_health(self):
        status, payload = _request("GET", "/health", token=None)
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "shiitake")
        self.assertIn("workers_idle", payload)
        self.assertIn("workers_inflight", payload)

    def test_02_ready(self):
        """The deployment has workers, so readiness must succeed. The status
        code is the part an orchestrator probe sees, so assert it directly —
        and, like health, it must answer without (and despite a wrong) token."""
        status, payload = _request("GET", "/ready", token=None)
        self.assertEqual(status, 200, payload)
        self.assertTrue(payload["ready"], payload)
        self.assertEqual(payload["service"], "shiitake")
        self.assertGreaterEqual(payload["workers_required"], 1)
        self.assertGreaterEqual(
            payload["workers_idle"] + payload["workers_inflight"],
            payload["workers_required"],
        )
        # The probe endpoints sit outside the bearer-auth layer: an
        # orchestrator has no token, and a bogus one must not lock it out.
        status, _ = _request("GET", "/ready", token="wrong-token")
        self.assertEqual(status, 200)

    def test_03_ready_and_health_report_the_same_pool(self):
        """Both probes read one pool snapshot; only their verdict differs."""
        _, health = _request("GET", "/health", token=None)
        status, ready = _request("GET", "/ready", token=None)
        self.assertEqual(status, 200, ready)
        self.assertEqual(health["service"], ready["service"])
        for key in ("workers_idle", "workers_inflight"):
            self.assertIn(key, ready)
            self.assertIsInstance(ready[key], int)

    def test_04_auth_required(self):
        if not TOKEN:
            self.skipTest("auth disabled")
        status, _ = _request("GET", "/sessions/does-not-exist", token=None)
        self.assertEqual(status, 401)
        status, _ = _request("GET", "/sessions/does-not-exist", token="wrong-token")
        self.assertEqual(status, 401)

    def test_05_unknown_handle_404(self):
        status, _ = _request("GET", "/sessions/00000000000000000000000000000000")
        self.assertEqual(status, 404)


class CommandExecution(unittest.TestCase):
    def test_10_echo_stdout(self):
        snap, out, err = run('echo "hello world"')
        self.assertEqual(snap["status"], "completed")
        self.assertEqual(snap["exit_code"], 0)
        self.assertEqual(snap["exit_cause"], "normal")
        self.assertEqual(out, "hello world\n")
        self.assertEqual(err, "")

    def test_11_stderr_and_nonzero_exit(self):
        snap, out, err = run("echo oops >&2; exit 7")
        self.assertEqual(snap["exit_code"], 7)
        self.assertEqual(snap["status"], "completed")
        self.assertEqual(snap["exit_cause"], "normal")
        self.assertEqual(err, "oops\n")

    def test_12_bash_script_loop(self):
        snap, out, err = run('for i in 1 2 3 4 5; do echo "line $i"; done')
        self.assertEqual(snap["exit_code"], 0)
        self.assertEqual(out, "line 1\nline 2\nline 3\nline 4\nline 5\n")

    def test_13_python_oneliner(self):
        snap, out, err = run("python3 -c 'print(sum(range(1, 101)))'", env=PATH_ENV)
        self.assertEqual(snap["exit_code"], 0, msg=err)
        self.assertEqual(out, "5050\n")

    def test_14_python_program(self):
        # prog uses single quotes internally, so wrap the -c argument in double
        # quotes (no $ / backticks to escape).
        prog = "import json; print(json.dumps({'evens': [x for x in range(10) if x % 2 == 0]}))"
        snap, out, err = run(f'python3 -c "{prog}"', env=PATH_ENV)
        self.assertEqual(snap["exit_code"], 0, msg=err)
        self.assertEqual(json.loads(out), {"evens": [0, 2, 4, 6, 8]})

    def test_15_working_directory(self):
        snap, out, err = run("pwd", env=PATH_ENV, workdir="/")
        self.assertEqual(snap["exit_code"], 0, msg=err)
        self.assertEqual(out, "/\n")

    def test_16_env_passthrough(self):
        snap, out, err = run('echo "$GREETING"', env={**PATH_ENV, "GREETING": "konnichiwa"})
        self.assertEqual(out, "konnichiwa\n")


class OutputCapture(unittest.TestCase):
    def test_20_large_output_captured_and_ranged(self):
        # 5 MB of output, captured straight to the stream file and read back
        # whole, then a suffix Range request for the last 4 bytes.
        n = 5_000_000
        handle = spawn(f"yes X | head -c {n}", env=PATH_ENV)
        snap = wait(handle)
        self.assertEqual(snap["exit_code"], 0)
        self.assertEqual(snap["stdout_bytes_written"], n)
        out = read_all(handle, "stdout")
        self.assertEqual(len(out), n)
        # `yes X` emits "X\n" repeatedly; the capture must be byte-exact.
        self.assertEqual(out[:4], "X\nX\n")
        self.assertLessEqual(set(out), {"X", "\n"})
        self.assertEqual(out.count("X"), n // 2)

        # Range support: last 4 bytes via a suffix range -> 206 Partial Content.
        status, tail = read_stream(handle, "stdout", range_header="bytes=-4")
        self.assertEqual(status, 206)
        self.assertEqual(tail.decode(), "X\nX\n")


class Lifecycle(unittest.TestCase):
    def test_30_unpredictable_long_command_can_be_observed_and_cancelled(self):
        # The caller does not predict wall time. Read output and poll while it runs.
        handle = spawn("echo started; sleep 30; echo finished", env=PATH_ENV)
        end = time.monotonic() + 10
        while True:
            snap = get_status(handle)
            status, body = read_stream(handle, "stdout")
            if b"started\n" in body:
                break
            self.assertLess(time.monotonic(), end, "command did not start")
            time.sleep(0.1)
        self.assertEqual(status, 200)
        self.assertEqual(snap["status"], "running")
        self.assertNotIn("timed_out", snap)
        status, payload = _request("DELETE", command_path(handle))
        self.assertEqual(status, 200, msg=payload)
        snap = wait(handle, deadline=20.0)
        self.assertTrue(snap["cancelled"])
        self.assertEqual(snap["exit_cause"], "cancelled")
        self.assertNotIn("finished", read_all(handle, "stdout"))

    def test_31_cancel(self):
        handle = spawn("sleep 30", env=PATH_ENV)
        # Give the worker a moment to actually start the command.
        time.sleep(1.0)
        status, payload = _request("DELETE", command_path(handle))
        self.assertEqual(status, 200, msg=payload)
        snap = wait(handle, deadline=20.0)
        self.assertTrue(snap["cancelled"])
        self.assertEqual(snap["exit_cause"], "cancelled")

    def test_32_oom(self):
        # Allocate far past the worker container's memory limit. On cgroup v2
        # with `memory.oom.group` (modern k8s/containerd) the kernel kills the
        # whole container, the worker connection drops, and the server's k8s
        # probe reports `oom_container`. Without oom-group the offending
        # subprocess alone is killed and the worker reports `signal` instead —
        # accept either, but it must never look like a clean exit.
        handle = spawn("python3 -c 'bytearray(2_000_000_000)'", env=PATH_ENV)
        snap = wait(handle, deadline=60.0)
        self.assertNotEqual(snap["status"], "completed", msg=snap)
        self.assertIn(snap["exit_cause"], ("oom_container", "signal"), msg=snap)
        self.assertNotEqual(snap.get("exit_code"), 0, msg=snap)


class Concurrency(unittest.TestCase):
    def test_40_pool_runs_commands_in_parallel(self):
        # Fewer than the pool size so every command should find an idle worker
        # without waiting; they overlap, so the server reports them inflight.
        n = max(2, min(POOL_SIZE - 1, 6))
        # Wait for idle capacity so all n sessions allocate back-to-back and
        # their commands actually overlap.
        wait_for_idle(n)
        handles = [spawn("sleep 2", env=PATH_ENV) for _ in range(n)]
        # Sample health across the run window — the peak inflight is the
        # parallelism we're proving. (A single sample can land in the gap
        # between dispatches or after the first command has already finished.)
        peak = 0
        for _ in range(15):
            _, health = _request("GET", "/health", token=None)
            peak = max(peak, health["workers_inflight"])
            if peak >= n:
                break
            time.sleep(0.1)
        self.assertGreaterEqual(peak, 2, msg=f"peak workers_inflight was {peak}, expected >= 2")
        for h in handles:
            snap = wait(h, deadline=30.0)
            self.assertEqual(snap["exit_code"], 0)


class Readiness(unittest.TestCase):
    def test_50_stays_ready_while_every_worker_is_busy(self):
        """Readiness counts *registered* workers, idle or in-flight. Occupying
        the whole pool must therefore keep the pod ready — gating on idle
        workers would pull it out of rotation exactly under load, which is the
        failure this endpoint exists to avoid."""
        wait_for_idle(POOL_SIZE)
        status, profiles = _request("GET", "/profiles")
        self.assertEqual(status, 200)
        handles = []
        for profile in profiles:
            for _ in range(profile["idle"]):
                handles.append(spawn("sleep 3", env=PATH_ENV,
                                     profile=profile["name"]))
        try:
            # Poll across the busy window: every sample must be ready, and at
            # least one must catch the pool with no idle worker left.
            saw_no_idle = False
            for _ in range(40):
                status, payload = _request("GET", "/ready", token=None)
                self.assertEqual(status, 200, msg=payload)
                self.assertTrue(payload["ready"], msg=payload)
                if payload["workers_idle"] == 0 and payload["workers_inflight"] > 0:
                    saw_no_idle = True
                    break
                time.sleep(0.1)
            self.assertTrue(
                saw_no_idle,
                f"pool never ran out of idle workers after dispatching {len(handles)} commands; "
                "readiness was not exercised under load",
            )
        finally:
            for h in handles:
                wait(h, deadline=30.0)


class SessionsAndProfiles(unittest.TestCase):
    def test_60_profile_selection_and_mounts(self):
        status, profiles = _request("GET", "/profiles")
        self.assertEqual(status, 200)
        self.assertGreaterEqual({p["name"] for p in profiles}, {"default", "isolated"})
        _, shared, _ = run("test -d /shared && echo YES || echo NO", profile="default")
        _, isolated, _ = run("test -d /shared && echo YES || echo NO", profile="isolated")
        self.assertEqual(shared.strip(), "YES")
        self.assertEqual(isolated.strip(), "NO")

    def test_61_stateful_context_and_parallel_jobs(self):
        status, session = _request("POST", "/sessions", {
            "profile": "default", "env": PATH_ENV, "workdir": "/tmp",
        })
        self.assertEqual(status, 201, session)
        sid = session["session_id"]
        try:
            status, job = _request("POST", f"/sessions/{sid}/commands", {
                "command": "cd /; export SESSION_MARKER=kept", "mode": "stateful",
            })
            self.assertEqual(status, 202, job)
            setup = job["handle"]
            while True:
                _, snap = _request("GET", f"/sessions/{sid}/commands/{setup}")
                if snap["status"] != "running":
                    break
                time.sleep(0.1)
            self.assertEqual(snap["exit_code"], 0, snap)
            handles = []
            for command in ("sleep 1; echo $SESSION_MARKER", "sleep 1; pwd"):
                status, job = _request("POST", f"/sessions/{sid}/commands", {
                    "command": command, "mode": "parallel",
                })
                self.assertEqual(status, 202, job)
                handles.append(job["handle"])
            status, listed = _request("GET", f"/sessions/{sid}/commands")
            self.assertEqual(status, 200, listed)
            self.assertEqual(listed[0]["handle"], setup)
            self.assertEqual({item["handle"] for item in listed}, {setup, *handles})
            self.assertTrue(all(item["session_id"] == sid for item in listed))
            for handle, expected in zip(handles, ("kept\n", "/\n")):
                while True:
                    _, snap = _request("GET", f"/sessions/{sid}/commands/{handle}")
                    if snap["status"] != "running":
                        break
                    time.sleep(0.1)
                self.assertEqual(snap["exit_code"], 0, snap)
                url = f"{BASE}/api/v1/sessions/{sid}/commands/{handle}/stdout"
                req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
                with urllib.request.urlopen(req) as resp:
                    self.assertEqual(resp.read().decode(), expected)
        finally:
            status, payload = _request("DELETE", f"/sessions/{sid}")
            self.assertEqual(status, 200, payload)

    def test_62_unknown_profile(self):
        status, _ = _request("POST", "/sessions", {"profile": "missing"})
        self.assertEqual(status, 404)


def print_summary():
    print("\n===== command summary =====")
    rows = sorted(_COMMANDS.values(), key=lambda r: r["spawned_at"])
    if not rows:
        print("(no commands dispatched)")
        return
    print(f"{'command':<46} {'status':<10} {'cause':<12} {'secs':>7} {'429':>4}")
    print("-" * 84)
    total_secs = 0.0
    for r in rows:
        secs = r["seconds"] or 0.0
        total_secs += secs
        print(
            f"{r['cmd'][:46]:<46} {str(r['status']):<10} "
            f"{str(r['exit_cause']):<12} {secs:>7.2f} {r['retries_429']:>4}"
        )
    causes = collections.Counter(r["exit_cause"] for r in rows)
    print("-" * 84)
    print(
        f"commands: {len(rows)}   429 retries: {_TOTAL_429}   "
        f"total: {total_secs:.2f}s   mean: {total_secs / len(rows):.2f}s"
    )
    print("exit causes: " + ", ".join(f"{k}={v}" for k, v in sorted(causes.items(), key=lambda x: str(x[0]))))


if __name__ == "__main__":
    runner = unittest.main(exit=False, verbosity=2)
    print_summary()
    sys.exit(0 if runner.result.wasSuccessful() else 1)
