"""`attach_pty` against a stand-in WebSocket server (no real shiitake server).

Proves the client speaks the wire contract: the first frame is the JSON ``open``
(carrying command/cwd/env/size/drop_to), binary frames are the byte stream both
ways, and ``resize`` is a JSON control frame."""

from __future__ import annotations

import asyncio
import json

import pytest
import websockets
from shiitake.client import AsyncShiitakeClient, DropTo


async def _echo_pty(ws) -> None:
    """A fake worker: record the open frame, then echo stdin as output, and
    stash any resize control frame for the test to inspect."""
    opened = json.loads(await ws.recv())
    ws.seen_open = opened  # type: ignore[attr-defined]
    await ws.send(b"READY")  # server -> client: pty output
    async for msg in ws:
        if isinstance(msg, (bytes, bytearray)):
            await ws.send(b"echo:" + bytes(msg))  # echo stdin back as output
        else:
            ws.seen_control = json.loads(msg)  # type: ignore[attr-defined]


@pytest.fixture
async def pty_server():
    holder: dict[str, object] = {}

    async def handler(ws):
        holder["ws"] = ws
        await _echo_pty(ws)

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        yield f"http://127.0.0.1:{port}", holder


async def test_attach_pty_sends_open_frame_and_streams(pty_server) -> None:
    base_url, holder = pty_server
    async with AsyncShiitakeClient(base_url, auth_token="t") as client:
        session = await client.attach_pty(
            working_dir="/workspace",
            command=["bash", "-i"],
            env={"PATH": "/usr/bin"},
            cols=120,
            rows=40,
            drop_to=DropTo(uid=2000123, gid=10009, supplementary_gids=[20001]),
        )

        outputs: list[bytes] = []
        async with session:
            it = session.output()
            outputs.append(await anext(it))  # "READY"
            await session.send(b"ls\n")
            outputs.append(await anext(it))  # "echo:ls\n"
            await session.resize(80, 24)
            # Give the server a tick to record the control frame.
            await asyncio.sleep(0.05)

    assert outputs[0] == b"READY"
    assert outputs[1] == b"echo:ls\n"

    server_ws = holder["ws"]
    opened = server_ws.seen_open  # type: ignore[attr-defined]
    assert opened["op"] == "open"
    assert opened["command"] == ["bash", "-i"]
    assert opened["working_dir"] == "/workspace"
    assert opened["cols"] == 120 and opened["rows"] == 40
    assert opened["drop_to"]["uid"] == 2000123
    assert opened["drop_to"]["gid"] == 10009

    control = server_ws.seen_control  # type: ignore[attr-defined]
    assert control == {"op": "resize", "cols": 80, "rows": 24}


def test_pty_url_derives_ws_scheme_from_http_base() -> None:
    # The ws(s) endpoint is the /exec origin+prefix, scheme swapped.
    http = AsyncShiitakeClient("http://sandbox:8080")._url("/pty")
    https = AsyncShiitakeClient("https://sandbox:8080")._url("/pty")
    assert http.replace("http://", "ws://", 1) == "ws://sandbox:8080/api/v1/pty"
    assert https.replace("https://", "wss://", 1) == "wss://sandbox:8080/api/v1/pty"
