#!/usr/bin/env python3
"""Pod-failure combinations in the two-pod topology.

Co-located, these cases did not exist: killing the server pod killed the worker
containers with it, so there was no independent failure to reconcile. Split
apart, each side dies on its own, and the contract is:

  * a worker lost mid-command   -> its handle is reconciled, never left Running
  * a PTY worker lost mid-session -> the client is closed 1011 with a reason,
                                   not left hanging on output that never comes
  * the server lost while idle  -> workers reconnect; the pool refills itself
  * the server lost mid-command -> the command is killed and that worker
                                   recycles, rather than reconnecting onto a
                                   sandbox holding a half-run command's debris
  * both lost together          -> the pool comes back with no manual step

Every case ends the same way, and the assertions are deliberately passive: the
point is that recovery needs no operator step, so these only ever poll.

This module manages its **own** port-forward. Deleting the server pod kills the
one run.sh holds, and `kubectl port-forward` does not re-establish itself, so a
shared forward would leave every later call failing against a healthy cluster.
For the same reason run.sh runs this suite last.

Config via env:
  SHIITAKE_E2E_CONTEXT     kubectl context (default k3d-shiitake-e2e)
  SHIITAKE_E2E_NAMESPACE   namespace (default shiitake-e2e)
  SHIITAKE_E2E_RELEASE     helm release (default shiitake-e2e)
  SHIITAKE_E2E_TOKEN       bearer token
  SHIITAKE_E2E_WORKERS     expected pool size (default 8)
  SHIITAKE_E2E_FAILOVER_PORT  local port for this suite's forward (default 18090)
"""

import json
import os
import subprocess
import time
import unittest
import urllib.error
import urllib.request

CONTEXT = os.environ.get("SHIITAKE_E2E_CONTEXT", "k3d-shiitake-e2e")
NAMESPACE = os.environ.get("SHIITAKE_E2E_NAMESPACE", "shiitake-e2e")
RELEASE = os.environ.get("SHIITAKE_E2E_RELEASE", "shiitake-e2e")
TOKEN = os.environ.get("SHIITAKE_E2E_TOKEN", "e2e-secret-token")
POOL_SIZE = int(os.environ.get("SHIITAKE_E2E_WORKERS", "8"))
PORT = int(os.environ.get("SHIITAKE_E2E_FAILOVER_PORT", "18090"))

PATH_ENV = {"PATH": "/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"}


def kubectl(*args, check=True):
    return subprocess.run(
        ["kubectl", "--context", CONTEXT, "-n", NAMESPACE, *args],
        capture_output=True,
        text=True,
        check=check,
    )


def worker_pods():
    out = kubectl("get", "pods", "-l", "app=shiitake-worker", "-o", "json").stdout
    return {p["metadata"]["name"]: p for p in json.loads(out)["items"]}


def restart_count(pod, container="worker"):
    """How many times `container` has been restarted in `pod`. A worker that
    recycles (exits 0 for a fresh container) increments this in place, which is
    how a recycle is told apart from a worker that simply carried on."""
    statuses = worker_pods().get(pod, {}).get("status", {}).get("containerStatuses") or []
    for cs in statuses:
        if cs["name"] == container:
            return cs.get("restartCount", 0)
    return None


def delete_pods(selector):
    """Delete every pod matching `selector` without waiting — as abrupt as the
    crash it stands in for."""
    kubectl(
        "delete", "pod", "-l", selector,
        "--wait=false", "--grace-period=0", "--force",
        check=False,
    )


class PortForward:
    """A `kubectl port-forward` that is re-established whenever it dies.

    The server pod going away is the subject of these tests, so its forward
    dying is expected rather than exceptional."""

    def __init__(self, port):
        self.port = port
        self.proc = None

    def ensure(self):
        if self.proc is not None and self.proc.poll() is None:
            return
        self.stop()
        self.proc = subprocess.Popen(
            ["kubectl", "--context", CONTEXT, "-n", NAMESPACE, "port-forward",
             f"deploy/{RELEASE}", f"{self.port}:8080"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(1.5)  # let it bind before the first call

    def stop(self):
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            self.proc = None


PF = PortForward(PORT)


def api(path, method="GET", body=None, token=TOKEN, timeout=10, raw=False):
    """One HTTP call, tolerant of the server being mid-restart.

    Returns `(status, payload)`; `status` is None when the server (or the
    forward) is unreachable, which these tests treat as a state to wait out
    rather than a failure."""
    PF.ensure()
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}/api/v1{path}", data=data, headers=headers, method=method
    )

    def decode(body_bytes):
        text = body_bytes.decode(errors="replace")
        if raw:
            return text
        try:
            return json.loads(text) if text else None
        except json.JSONDecodeError:
            return text

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, decode(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, decode(exc.read())
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        PF.stop()  # forward died with the pod; next call rebuilds it
        return None, str(exc)


def spawn(command, timeout=300.0, deadline=120.0):
    """POST /exec, waiting out a server that is still coming back."""
    end = time.monotonic() + deadline
    body = {"command": command, "timeout": timeout, "env": PATH_ENV}
    while time.monotonic() < end:
        status, payload = api("/exec", method="POST", body=body)
        if status == 202:
            return payload["handle"]
        time.sleep(2.0)
    raise AssertionError(f"could not spawn {command!r} within {deadline}s")


def wait_for_pool(workers=POOL_SIZE, deadline=300.0):
    """Block until at least `workers` workers are registered."""
    end = time.monotonic() + deadline
    last = None
    while time.monotonic() < end:
        status, payload = api("/health", token=None)
        if status == 200:
            last = payload
            if payload["workers_idle"] + payload["workers_inflight"] >= workers:
                return payload
        time.sleep(2.0)
    raise AssertionError(f"pool never returned to {workers} workers; last health={last}")


def wait_for_terminal(handle, deadline=180.0):
    """Poll a handle until it leaves `running`. Returns None if the handle is
    unknown — a restarted server has an empty registry, which is a legitimate
    outcome here, not a failure."""
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        status, snap = api(f"/exec/{handle}")
        if status == 404:
            return None
        if status == 200 and snap["status"] != "running":
            return snap
        time.sleep(1.0)
    raise AssertionError(f"handle {handle} still running after {deadline}s")


def running_handle(command="sleep 120"):
    """Spawn `command` and return `(handle, worker_pod)` once it is really
    running on a worker — so a delete lands mid-command, not in the gap."""
    handle = spawn(command, timeout=600.0)
    end = time.monotonic() + 60.0
    while time.monotonic() < end:
        status, snap = api(f"/exec/{handle}")
        if status == 200 and snap["status"] == "running" and snap.get("worker_id"):
            return handle, snap["worker_id"]
        time.sleep(0.5)
    raise AssertionError(f"handle {handle} never reached a worker")


class PodFailures(unittest.TestCase):
    """Ordered: each test leaves the pool healthy for the next."""

    @classmethod
    def setUpClass(cls):
        if not worker_pods():
            raise unittest.SkipTest("not a two-pod deployment (no worker pods)")

    @classmethod
    def tearDownClass(cls):
        PF.stop()

    def setUp(self):
        wait_for_pool()

    def test_10_worker_pod_deleted_mid_command(self):
        handle, victim = running_handle()
        self.assertIn(victim, worker_pods(), "a worker id should be its own pod name")

        kubectl("delete", "pod", victim, "--wait=false", "--grace-period=0", "--force",
                check=False)

        # Reconciled, never left Running. Which cause depends on how the kubelet
        # reports the termination it never got to observe cleanly.
        snap = wait_for_terminal(handle)
        self.assertIsNotNone(snap, "the server is up, so it must still know this handle")
        self.assertNotEqual(snap["status"], "completed", msg=snap)
        self.assertIn(snap["exit_cause"], ("worker_died", "oom_container"), msg=snap)

        wait_for_pool()  # the Deployment replaces it, unattended

    def test_20_server_pod_deleted_while_idle(self):
        delete_pods("app=shiitake")

        # Workers reconnect to the replacement through the dispatch Service —
        # which is exactly why that Service publishes not-ready addresses: the
        # new server is not ready until they arrive, and they arrive through it.
        health = wait_for_pool()
        self.assertGreaterEqual(health["workers_idle"], POOL_SIZE, msg=health)

        snap = wait_for_terminal(spawn("echo back"))
        self.assertEqual(snap["status"], "completed", msg=snap)

    def test_30_server_pod_deleted_mid_command(self):
        handle, victim = running_handle()
        before = restart_count(victim)
        self.assertIsNotNone(before, f"no container status for {victim}")

        delete_pods("app=shiitake")

        # The handle dies with the server: the registry is in memory, so the
        # replacement has never heard of it. 404 is the correct answer, not a
        # fabricated terminal state.
        self.assertIsNone(
            wait_for_terminal(handle),
            "a restarted server must not claim to know a handle it never issued",
        )

        # The fencing property. The worker that was mid-command had its command
        # SIGKILLed and its sandbox left un-reset, so it must recycle rather than
        # reconnect — visible as its container restarting in place.
        end = time.monotonic() + 180.0
        while time.monotonic() < end:
            after = restart_count(victim)
            # `None` means the pod itself went away, which is a recycle too.
            if after is None or after > before:
                break
            time.sleep(2.0)
        else:
            self.fail(
                f"worker {victim} never recycled after losing its server "
                f"mid-command (restartCount stuck at {before}) — it would have "
                f"served the next command on a dirty sandbox"
            )

        wait_for_pool()
        snap = wait_for_terminal(spawn("echo alive"))
        self.assertEqual(snap["status"], "completed", msg=snap)

    def test_40_both_deleted_together(self):
        running_handle()
        delete_pods("app=shiitake")
        delete_pods("app=shiitake-worker")

        # Neither side coordinates the other; both just come back.
        wait_for_pool()
        snap = wait_for_terminal(spawn("echo recovered"))
        self.assertEqual(snap["status"], "completed", msg=snap)

    def test_50_idle_workers_survive_a_server_restart_without_recycling(self):
        """The other half of the asymmetry: an idle worker has run nothing, so
        its sandbox is clean and it reconnects rather than burning a container."""
        wait_for_pool()
        before = {pod: restart_count(pod) for pod in worker_pods()}

        delete_pods("app=shiitake")
        wait_for_pool()

        # Give any recycle that was going to happen time to show up.
        time.sleep(15)
        after = {pod: restart_count(pod) for pod in worker_pods()}
        for pod, count in before.items():
            if pod in after and after[pod] is not None:
                self.assertEqual(
                    after[pod], count,
                    msg=f"idle worker {pod} recycled ({count} -> {after[pod]}) when "
                        f"it only needed to reconnect",
                )

    def test_60_pty_worker_death_closes_the_client_with_a_reason(self):
        """The /pty analogue of test_10: a session whose worker dies mid-stream
        must close the client 1011 with a reason, not leave it waiting on output
        that will never come. Uses the shiitake-py WebSocket client (this suite
        runs under uv for it); imported lazily so the rest still runs on python3."""
        import asyncio

        from shiitake.client import AsyncShiitakeClient

        PF.ensure()

        async def drive():
            async with AsyncShiitakeClient(f"http://127.0.0.1:{PORT}", auth_token=TOKEN) as c:
                session = await c.attach_pty(
                    working_dir="/tmp", command=["bash", "-c", "echo READY; sleep 60"]
                )
                buf = bytearray()

                async def until(needle):
                    async for chunk in session.output():
                        buf.extend(chunk)
                        if needle in buf:
                            return

                # Shell up + worker pinned, then kill the workers under it.
                await asyncio.wait_for(until(b"READY"), 20)
                delete_pods("app=shiitake-worker")

                # Drain to the close rather than hang on output that won't arrive.
                async def drain():
                    async for _ in session.output():
                        pass

                await asyncio.wait_for(drain(), 30)
                return session.close_code, session.close_reason

        code, reason = asyncio.run(drive())
        self.assertEqual(code, 1011, f"abnormal worker loss closes 1011; got {code} / {reason!r}")
        self.assertIn("worker disconnected", reason, f"reason should name the cause; got {reason!r}")
        wait_for_pool()  # the Deployment replaces the workers, unattended


if __name__ == "__main__":
    unittest.main(verbosity=2)
