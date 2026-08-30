#!/usr/bin/env python3
"""End-to-end checks specific to the two-pod topology.

test_exec.py must pass in either topology — the contract doesn't change when the
server and the workers stop sharing a pod. Checked here is what the split makes
newly true: commands running in worker pods of their own, capture spanning both
pods, and a NetworkPolicy confining the workers but not the server.

Run against a cluster deployed with `topology=two-pod` (tests/setup.sh with
SHIITAKE_E2E_TOPOLOGY=two-pod). Config via env, on top of test_exec.py's:

  SHIITAKE_E2E_RELEASE   helm release name (default shiitake-e2e), which names
                         the worker Deployment and the dispatch/otel Services
  SHIITAKE_E2E_NETPOL    "0" to skip the NetworkPolicy checks (deployed with
                         networkPolicy.enabled=false)
"""

import os
import shlex
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_exec import PATH_ENV, run, spawn, wait, wait_for_idle  # noqa: E402

RELEASE = os.environ.get("SHIITAKE_E2E_RELEASE", "shiitake-e2e")
DISPATCH_PORT = int(os.environ.get("SHIITAKE_E2E_DISPATCH_PORT", "8090"))
NETPOL = os.environ.get("SHIITAKE_E2E_NETPOL", "1") != "0"

# Pods of the worker Deployment are named "<release>-workers-<replicaset>-<id>",
# and each worker takes its id from its own pod name.
WORKER_POD_PREFIX = f"{RELEASE}-workers-"


def probe_tcp(host, port, connect_timeout=4.0):
    """Open a TCP connection *from inside a worker*. Returns "OPEN"/"BLOCKED".

    A NetworkPolicy drop is silent — the connect hangs until the socket timeout
    — so the timeout is the signal, and any other error counts as blocked too.
    """
    script = (
        "import socket, sys\n"
        "s = socket.socket()\n"
        f"s.settimeout({connect_timeout})\n"
        "try:\n"
        f"    rc = s.connect_ex(({host!r}, {port}))\n"
        "except OSError:\n"
        "    rc = 1\n"
        "print('OPEN' if rc == 0 else 'BLOCKED')\n"
    )
    snap, out, err = run(
        f"python3 -c {shlex.quote(script)}",
        env=PATH_ENV,
        timeout=30.0,
        wait_deadline=60.0,
    )
    assert snap["exit_code"] == 0, f"probe failed to run: {snap} {err!r}"
    verdict = out.strip()
    assert verdict in ("OPEN", "BLOCKED"), f"unexpected probe output {out!r} / {err!r}"
    return verdict


class Topology(unittest.TestCase):
    def test_01_commands_run_in_separate_worker_pods(self):
        # Each worker id is its own pod's name, so a worker_id from the worker
        # Deployment proves the Execute crossed a pod boundary. Dispatched
        # together, not serially: a resident worker returns straight to idle, so
        # sequential commands could both land on the same one.
        wait_for_idle(2)
        handles = [spawn("sleep 2", env=PATH_ENV, timeout=30.0) for _ in range(2)]
        snaps = [wait(h, deadline=60.0) for h in handles]
        for snap in snaps:
            self.assertTrue(
                snap["worker_id"].startswith(WORKER_POD_PREFIX),
                msg=f"worker_id {snap['worker_id']!r} is not a {WORKER_POD_PREFIX}* pod",
            )
        self.assertEqual(len({s["worker_id"] for s in snaps}), 2, msg=snaps)

    def test_02_capture_volume_spans_both_pods(self):
        # The worker writes in its pod, the server serves from its own. Anything
        # less than a shared volume reads back empty or short.
        size = 256 * 1024
        snap, out, _ = run(
            f"python3 -c 'print(\"x\" * {size})'",
            env=PATH_ENV,
            wait_deadline=60.0,
        )
        self.assertEqual(snap["status"], "completed", msg=snap)
        self.assertEqual(len(out), size + 1)  # + the trailing newline
        self.assertEqual(snap["stdout_bytes_written"], size + 1, msg=snap)


class WorkerConfinement(unittest.TestCase):
    """Pod-scoped controls that apply to the workers alone — none of which could
    hold co-located, where one policy would have caught the server too.
    """

    def setUp(self):
        if not NETPOL:
            self.skipTest("deployed with networkPolicy.enabled=false")

    def test_10_worker_can_still_reach_the_dispatcher(self):
        # The positive control: without it, every "BLOCKED" below could just as
        # well mean the probe itself is broken.
        self.assertEqual(
            probe_tcp(f"{RELEASE}-dispatch", DISPATCH_PORT),
            "OPEN",
            msg="the worker must still reach the dispatcher it is connected to",
        )

    def test_11_worker_cannot_reach_the_otel_collector(self):
        # Ordinary pod-to-pod traffic, and an egress the server itself needs —
        # until the split, shared with every worker container.
        self.assertEqual(
            probe_tcp(f"{RELEASE}-otel", 4318),
            "BLOCKED",
            msg="a worker reached the collector — the NetworkPolicy is not in effect",
        )

    def test_12_worker_cannot_reach_the_api_server(self):
        # The other egress the server needs: container status, read to classify
        # an OOM-killed worker.
        self.assertEqual(
            probe_tcp("kubernetes.default.svc.cluster.local", 443),
            "BLOCKED",
            msg="a worker reached the API server — the NetworkPolicy is not in effect",
        )

    def test_13_worker_has_no_service_account_token(self):
        # Worked before the split too; kept so a worker pod-spec regression is
        # caught alongside the network ones.
        snap, out, _ = run(
            "if [ -e /var/run/secrets/kubernetes.io/serviceaccount/token ]; "
            "then echo PRESENT; else echo ABSENT; fi"
        )
        self.assertEqual(out.strip(), "ABSENT", msg=snap)


if __name__ == "__main__":
    unittest.main(verbosity=2)
