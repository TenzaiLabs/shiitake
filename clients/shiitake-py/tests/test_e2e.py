"""Client → HTTP → server → worker tests against the k3d deployment."""

from __future__ import annotations

import asyncio

import pytest
from shiitake.client import AsyncShiitakeClient


async def test_health_and_profiles(shiitake_client: AsyncShiitakeClient) -> None:
    health = await shiitake_client.health()
    assert health.status == "ok"
    ready = await shiitake_client.ready()
    assert ready.ready is True
    assert ready.workers_idle + ready.workers_inflight >= ready.workers_required
    profiles = await shiitake_client.profiles()
    assert {p.name for p in profiles} >= {"default", "isolated"}


async def test_session_keeps_worker_and_context(shiitake_client: AsyncShiitakeClient) -> None:
    async with await shiitake_client.create_session(
        env={"PATH": "/bin:/usr/bin"}, wait_for_worker=True
    ) as session:
        first = await session.run("cd /; export SHIITAKE_VALUE=kept")
        assert first.exit_code == 0, first
        second = await session.run("echo $SHIITAKE_VALUE; pwd", mode="parallel")
        assert second.stdout == "kept\n/\n"
        status = await session.status()
        assert status.state == "active"


async def test_range_read_on_isolated_profile(shiitake_client: AsyncShiitakeClient) -> None:
    async with await shiitake_client.create_session(
        profile="isolated", wait_for_worker=True
    ) as session:
        handle = await session.spawn("printf abcdef", mode="parallel")
        snap = await handle.wait()
        assert snap.status == "completed"
        assert snap.stdout_bytes_written == 6
        head = await handle.read("stdout", from_=0, max_=3)
        assert head.content == "abc"
        assert head.bytes_written == 6


async def test_nonzero_exit_and_stderr(shiitake_client: AsyncShiitakeClient) -> None:
    async with await shiitake_client.create_session(wait_for_worker=True) as session:
        result = await session.run("echo oops >&2; exit 7", mode="parallel")
        assert result.status == "completed"
        assert result.exit_code == 7
        assert result.stderr == "oops\n"


async def test_cancelling_command_keeps_session_until_release(
    shiitake_client: AsyncShiitakeClient,
) -> None:
    idle_before = (await shiitake_client.health()).workers_idle
    async with await shiitake_client.create_session(
        env={"PATH": "/bin:/usr/bin"}, wait_for_worker=True
    ) as session:
        task = asyncio.create_task(session.run("sleep 600", mode="parallel"))
        async with asyncio.timeout(30):
            while (await shiitake_client.health()).workers_sessions == 0:
                await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (await session.status()).state == "active"
        result = await session.run("echo still-here")
        assert result.stdout == "still-here\n"
    async with asyncio.timeout(30):
        while (await shiitake_client.health()).workers_idle < idle_before:
            await asyncio.sleep(0.2)
