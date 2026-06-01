"""Round-trip tests for shiitake-py against an httpx MockTransport."""

from __future__ import annotations

import json
from collections.abc import Callable

import httpx
import pytest
from shiitake.client import AsyncShiitakeClient, DropTo, NoIdleWorkerError, ShiitakeError


async def _with_mock(handler: Callable[[httpx.Request], httpx.Response], **kw: object) -> AsyncShiitakeClient:
    client = AsyncShiitakeClient("http://srv", **kw)  # type: ignore[arg-type]
    await client.aclose()
    client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=5.0)
    return client


@pytest.mark.asyncio
async def test_spawn_sends_command_and_drop_to() -> None:
    captured: dict[str, object] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        captured["path"] = req.url.path
        captured["auth"] = req.headers.get("authorization")
        captured["body"] = json.loads(req.content)
        return httpx.Response(202, json={"handle": "h", "started_at": 1.0})

    client = await _with_mock(handler, auth_token="tok")
    h = await client.spawn("echo hi", env={"PATH": "/bin"}, drop_to=DropTo(uid=10, gid=20, umask=0o007))
    assert h.handle == "h"
    assert captured["path"] == "/api/v1/exec"
    assert captured["auth"] == "Bearer tok"
    body = captured["body"]
    assert body["command"] == "echo hi"
    assert body["env"] == {"PATH": "/bin"}
    assert body["drop_to"] == {"uid": 10, "gid": 20, "supplementary_gids": [], "umask": 0o007}
    await client.aclose()


@pytest.mark.asyncio
async def test_spawn_429_raises() -> None:
    client = await _with_mock(lambda req: httpx.Response(429, text="busy"), max_retries=1)
    with pytest.raises(NoIdleWorkerError):
        await client.spawn("echo")
    await client.aclose()


@pytest.mark.asyncio
async def test_run_collapses_spawn_wait_read() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        if req.method == "POST":
            return httpx.Response(202, json={"handle": "h", "started_at": 0.0})
        if req.url.path == "/api/v1/exec/h":
            return httpx.Response(
                200,
                json={
                    "handle": "h",
                    "worker_id": "w",
                    "status": "completed",
                    "started_at": 0.0,
                    "exit_code": 0,
                    "exit_cause": "normal",
                    "stdout_bytes_written": 3,
                },
            )
        if req.url.path == "/api/v1/exec/h/stdout":
            return httpx.Response(206, content=b"hi\n", headers={"content-range": "bytes 0-2/3"})
        return httpx.Response(416, headers={"content-range": "bytes */0"})

    client = await _with_mock(handler)
    result = await client.run("echo hi")
    assert result.status == "completed"
    assert result.exit_code == 0
    assert result.stdout == "hi\n"
    assert result.exit_cause == "normal"
    await client.aclose()


@pytest.mark.asyncio
async def test_read_range_and_past_eof() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        if req.headers.get("range") == "bytes=0-1023":
            return httpx.Response(206, content=b"hello", headers={"content-range": "bytes 0-4/5"})
        return httpx.Response(416, headers={"content-range": "bytes */5"})

    client = await _with_mock(handler)
    r = await client.read("h", "stdout", from_=0, max_=1024)
    assert r.content == "hello"
    assert r.bytes_written == 5
    assert r.eof is True
    past = await client.read("h", "stdout", from_=999)
    assert past.content == ""
    assert past.eof is True
    await client.aclose()


@pytest.mark.asyncio
async def test_http_error_raises() -> None:
    client = await _with_mock(lambda req: httpx.Response(500, text="boom"), max_retries=1)
    with pytest.raises(ShiitakeError) as exc:
        await client.status("h")
    assert exc.value.status == 500
    await client.aclose()


@pytest.mark.asyncio
async def test_health() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.url.path == "/api/v1/health"
        return httpx.Response(200, json={"status": "ok", "service": "shiitake", "workers_idle": 7, "workers_inflight": 1})

    client = await _with_mock(handler)
    h = await client.health()
    assert h.service == "shiitake"
    assert h.workers_idle == 7
    await client.aclose()
