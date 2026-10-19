"""Client contract tests against an HTTP mock transport."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable

import httpx
import pytest
from shiitake.client import (
    AsyncShiitakeClient, DropTo, NoIdleWorkerError, SessionResponse, ShiitakeClient,
    ShiitakeError,
)


def session_payload() -> dict[str, object]:
    return {
        "session_id": "s", "worker_id": "w", "profile": "isolated",
        "state": "active", "created_at": 1.0,
    }


async def with_mock(handler: Callable[[httpx.Request], httpx.Response], **kw: object) -> AsyncShiitakeClient:
    client = AsyncShiitakeClient("http://srv", **kw)  # type: ignore[arg-type]
    await client.aclose()
    client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=5.0)
    return client


@pytest.mark.asyncio
async def test_create_and_release_session_with_fixed_identity() -> None:
    seen: list[tuple[str, str, dict[str, object] | None]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        assert req.headers["authorization"] == "Bearer tok"
        body = json.loads(req.content) if req.content else None
        seen.append((req.method, req.url.path, body))
        if req.method == "POST":
            return httpx.Response(201, json=session_payload())
        return httpx.Response(200, json={**session_payload(), "state": "closed"})

    client = await with_mock(handler, auth_token="tok")
    async with await client.create_session(
        profile="isolated", workdir="/tmp", env={"PATH": "/bin"},
        drop_to=DropTo(uid=10, gid=20, umask=0o007, name="alice", create_home=True),
    ) as session:
        assert session.session_id == "s"
    assert seen[0][0:2] == ("POST", "/api/v1/sessions")
    assert seen[0][2]["drop_to"] == {
        "uid": 10, "gid": 20, "supplementary_gids": [], "umask": 0o007,
        "name": "alice", "create_home": True,
    }
    assert seen[1][0:2] == ("DELETE", "/api/v1/sessions/s")
    await client.aclose()


@pytest.mark.asyncio
async def test_profile_busy_raises() -> None:
    client = await with_mock(lambda req: httpx.Response(429, text="busy"), max_retries=1)
    with pytest.raises(NoIdleWorkerError):
        await client.create_session(profile="isolated")
    await client.aclose()


@pytest.mark.asyncio
async def test_create_session_retries_only_when_capacity_is_unavailable() -> None:
    attempts = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, text="busy")
        return httpx.Response(201, json=session_payload())

    client = await with_mock(handler, max_retries=3, retry_initial_delay=0)
    session = await client.create_session(wait_for_worker=True)
    assert session.session_id == "s"
    assert attempts == 2
    await client.aclose()


@pytest.mark.asyncio
async def test_create_session_does_not_repeat_ambiguous_failure() -> None:
    for failure in (httpx.ReadTimeout("response lost"), httpx.Response(502, text="failed")):
        attempts = 0

        def handler(req: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if isinstance(failure, Exception):
                raise failure
            return failure

        client = await with_mock(handler, max_retries=3, retry_initial_delay=0)
        with pytest.raises((httpx.ReadTimeout, ShiitakeError)):
            await client.create_session(wait_for_worker=True)
        assert attempts == 1
        await client.aclose()


@pytest.mark.asyncio
async def test_session_command_modes_and_range_output() -> None:
    seen: list[tuple[str, str, dict[str, object] | None]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content) if req.content else None
        seen.append((req.method, req.url.path, body))
        if req.url.path == "/api/v1/sessions":
            return httpx.Response(201, json=session_payload())
        if req.method == "POST":
            return httpx.Response(202, json={"handle": "h", "started_at": 2.0})
        if req.url.path.endswith("/h"):
            return httpx.Response(200, json={
                "handle": "h", "session_id": "s", "worker_id": "w", "status": "completed",
                "started_at": 2.0, "exit_code": 0, "exit_cause": "normal",
                "stdout_bytes_written": 3,
            })
        if req.url.path.endswith("/stdout"):
            assert req.headers["range"] == "bytes=0-9"
            return httpx.Response(206, content=b"hi\n", headers={"content-range": "bytes 0-2/3"})
        return httpx.Response(416, headers={"content-range": "bytes */0"})

    client = await with_mock(handler)
    session = await client.create_session(profile="isolated")
    handle = await session.spawn("echo hi", mode="parallel")
    assert seen[-1][2] == {"command": "echo hi", "timeout": 300.0, "mode": "parallel"}
    status = await handle.wait()
    assert status.session_id == "s"
    chunk = await handle.read("stdout", max_=10)
    assert chunk.content == "hi\n"
    assert chunk.bytes_written == 3
    empty = await handle.read("stderr", max_=10)
    assert empty.content == ""
    await client.aclose()


@pytest.mark.asyncio
async def test_run_cancels_only_its_command() -> None:
    seen: list[tuple[str, str]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append((req.method, req.url.path))
        if req.url.path == "/api/v1/sessions":
            return httpx.Response(201, json=session_payload())
        if req.method == "POST":
            return httpx.Response(202, json={"handle": "h", "started_at": 1.0})
        if req.method == "DELETE":
            return httpx.Response(200, json={"handle": "h", "status": "error"})
        return httpx.Response(200, json={
            "handle": "h", "session_id": "s", "worker_id": "w", "status": "running",
            "started_at": 1.0,
        })

    client = await with_mock(handler)
    session = await client.create_session()
    task = asyncio.create_task(session.run("sleep 600"))
    while ("GET", "/api/v1/sessions/s/commands/h") not in seen:
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert ("DELETE", "/api/v1/sessions/s/commands/h") in seen
    assert ("DELETE", "/api/v1/sessions/s") not in seen
    await client.aclose()


@pytest.mark.asyncio
async def test_profiles_readiness_and_error() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path.endswith("/profiles"):
            return httpx.Response(200, json=[{"name": "isolated", "idle": 2, "allocated": 1}])
        if req.url.path.endswith("/ready"):
            return httpx.Response(503, json={
                "ready": False, "service": "shiitake", "workers_idle": 0,
                "workers_inflight": 0, "workers_required": 1,
            })
        return httpx.Response(500, text="boom")

    client = await with_mock(handler, max_retries=1)
    assert (await client.profiles())[0].allocated == 1
    assert (await client.ready()).ready is False
    with pytest.raises(ShiitakeError) as exc:
        await client.session_status("s")
    assert exc.value.status == 500
    await client.aclose()


def test_sync_client_requires_session() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/api/v1/sessions" and req.method == "POST":
            return httpx.Response(201, json=session_payload())
        if req.url.path.endswith("/commands"):
            return httpx.Response(202, json={"handle": "h", "started_at": 1.0})
        if req.url.path.endswith("/h"):
            return httpx.Response(200, json={
                "handle": "h", "session_id": "s", "worker_id": "w", "status": "completed",
                "started_at": 1.0, "exit_code": 0,
            })
        if req.url.path.endswith("/stdout"):
            return httpx.Response(206, content=b"ok\n", headers={"content-range": "bytes 0-2/3"})
        if req.url.path.endswith("/stderr"):
            return httpx.Response(416, headers={"content-range": "bytes */0"})
        return httpx.Response(200, json={**session_payload(), "state": "closed"})

    client = ShiitakeClient("http://srv")
    client.close()
    client._http = httpx.Client(transport=httpx.MockTransport(handler))
    with client.create_session(profile="isolated") as session:
        result = session.run("echo ok")
        assert result.stdout == "ok\n"
        assert result.exit_code == 0
    client.close()
