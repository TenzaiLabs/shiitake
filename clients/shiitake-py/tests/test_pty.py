"""Terminal attachment uses the session URL and preserves binary bytes."""

from __future__ import annotations

import asyncio
import json

import pytest
import websockets
from shiitake.client import AsyncShiitakeClient


@pytest.fixture
async def pty_server():
    holder: dict[str, object] = {}

    async def handler(ws):
        holder["path"] = ws.request.path
        holder["auth"] = ws.request.headers.get("authorization")
        await ws.send(b"READY")
        async for msg in ws:
            if isinstance(msg, (bytes, bytearray)):
                await ws.send(b"echo:" + bytes(msg))
            else:
                holder["control"] = json.loads(msg)

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        yield f"http://127.0.0.1:{port}", holder


async def test_attach_terminal_streams_and_resizes(pty_server) -> None:
    base_url, holder = pty_server
    async with AsyncShiitakeClient(base_url, auth_token="t") as client:
        async with await client.attach_terminal("session-1") as terminal:
            it = terminal.output()
            assert await anext(it) == b"READY"
            await terminal.send(b"ls\n")
            assert await anext(it) == b"echo:ls\n"
            await terminal.resize(80, 24)
            await asyncio.sleep(0.05)
    assert holder["path"] == "/api/v1/sessions/session-1/terminal"
    assert holder["auth"] == "Bearer t"
    assert holder["control"] == {"op": "resize", "cols": 80, "rows": 24}


def test_terminal_url_derives_ws_scheme_from_http_base() -> None:
    http = AsyncShiitakeClient("http://sandbox:8080")._url("/sessions/s/terminal")
    https = AsyncShiitakeClient("https://sandbox:8080")._url("/sessions/s/terminal")
    assert http.replace("http://", "ws://", 1) == "ws://sandbox:8080/api/v1/sessions/s/terminal"
    assert https.replace("https://", "wss://", 1) == "wss://sandbox:8080/api/v1/sessions/s/terminal"
