"""End-to-end tests: drive a real shiitake server through shiitake-py.

Skipped unless ``SHIITAKE_E2E_URL`` points at a running server (the k3d suite's
``tests/run.sh`` sets it after standing the cluster up + port-forwarding). This
exercises the full path — client → HTTP → server → worker → bash — with no
mocks, so a wire-format regression in either shiitake-py or the server surfaces
here.
"""

from __future__ import annotations

import asyncio
import os

import pytest
from shiitake.client import AsyncShiitakeClient

_BASE = os.environ.get("SHIITAKE_E2E_URL")
_TOKEN = os.environ.get("SHIITAKE_E2E_TOKEN") or None

pytestmark = pytest.mark.skipif(not _BASE, reason="SHIITAKE_E2E_URL not set")


async def test_health_reports_pool() -> None:
    async with AsyncShiitakeClient(_BASE, auth_token=_TOKEN) as c:
        health = await c.health()
        assert health.status == "ok"
        assert health.service == "shiitake"


async def test_ready_reflects_the_pool() -> None:
    async with AsyncShiitakeClient(_BASE, auth_token=_TOKEN) as c:
        ready = await c.ready()
        # The deployment under test runs workers, so it must report ready.
        assert ready.ready is True, ready
        assert ready.service == "shiitake"
        assert ready.workers_required >= 1
        assert ready.workers_idle + ready.workers_inflight >= ready.workers_required


async def test_ready_holds_while_a_command_occupies_a_worker() -> None:
    """Registered, not idle, is what readiness counts — a worker running a
    command still keeps its pod in rotation."""
    async with AsyncShiitakeClient(_BASE, auth_token=_TOKEN) as c:
        handle = await c.spawn("sleep 2", env={"PATH": "/bin:/usr/bin"}, wait_for_worker=True)
        try:
            ready = await c.ready()
            assert ready.ready is True, ready
            assert ready.workers_inflight >= 1, ready
        finally:
            await handle.wait()


async def test_run_echo_against_real_server() -> None:
    async with AsyncShiitakeClient(_BASE, auth_token=_TOKEN) as c:
        # `echo` is a bash builtin, so it runs without a PATH in the cleared env.
        result = await c.run("echo shiitake-e2e", wait_for_worker=True)
        assert result.status == "completed", result
        assert result.exit_code == 0
        assert result.stdout == "shiitake-e2e\n"
        assert result.exit_cause == "normal"


async def test_spawn_wait_and_range_read() -> None:
    async with AsyncShiitakeClient(_BASE, auth_token=_TOKEN) as c:
        handle = await c.spawn("printf abcdef", wait_for_worker=True)
        snap = await handle.wait()
        assert snap.status == "completed"
        assert snap.stdout_bytes_written == 6
        # Suffix-free byte range over the real capture file.
        head = await c.read(handle.handle, "stdout", from_=0, max_=3)
        assert head.content == "abc"
        assert head.bytes_written == 6


async def test_nonzero_exit_is_completed_with_code() -> None:
    async with AsyncShiitakeClient(_BASE, auth_token=_TOKEN) as c:
        result = await c.run("echo oops >&2; exit 7", wait_for_worker=True)
        assert result.status == "completed"
        assert result.exit_code == 7
        assert result.stderr == "oops\n"


async def test_cancelling_run_frees_the_worker() -> None:
    """The point of killing the handle is the slot, so assert the slot.

    A mocked transport can only show that the DELETE goes out; only a real
    server shows the worker leaving inflight and returning to idle.
    """
    async with AsyncShiitakeClient(_BASE, auth_token=_TOKEN) as c:
        idle_before = (await c.health()).workers_idle

        task = asyncio.create_task(c.run("sleep 600", wait_for_worker=True))
        # Wait until the command is actually occupying a worker.
        async with asyncio.timeout(30):
            while (await c.health()).workers_inflight == 0:
                await asyncio.sleep(0.2)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # The kill is issued as the caller unwinds; the worker resets before it
        # reports idle again, so poll rather than sampling once.
        async with asyncio.timeout(30):
            while (await c.health()).workers_idle < idle_before:
                await asyncio.sleep(0.2)
        assert (await c.health()).workers_inflight == 0
