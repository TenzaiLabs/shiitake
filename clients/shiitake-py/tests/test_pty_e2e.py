"""Real terminal attachment tests against the k3d session API."""

from __future__ import annotations

import asyncio
import contextlib
import os

import pytest
from shiitake.client import AsyncPtySession, AsyncSession, AsyncShiitakeClient

_ROOT_WORKERS = os.environ.get("SHIITAKE_E2E_ROOT_WORKERS") == "1"
_PATH_ENV = {"PATH": "/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"}


class _Term:
    def __init__(self, terminal: AsyncPtySession) -> None:
        self.terminal = terminal
        self.buf = bytearray()
        self.closed = False
        self.task = asyncio.create_task(self._drain())

    async def _drain(self) -> None:
        async for chunk in self.terminal.output():
            self.buf.extend(chunk)
        self.closed = True

    async def wait_for(self, needle: bytes, deadline: float = 20.0) -> None:
        async def poll() -> None:
            while needle not in self.buf and not self.closed:
                await asyncio.sleep(0.05)

        await asyncio.wait_for(poll(), deadline)
        assert needle in self.buf, f"{needle!r} never arrived; got {bytes(self.buf)!r}"

    async def send(self, data: bytes) -> None:
        await self.terminal.send(data)

    async def close(self) -> None:
        await self.terminal.aclose()
        self.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.task


async def _session(c: AsyncShiitakeClient, **kwargs) -> AsyncSession:
    return await c.create_session(env=_PATH_ENV, wait_for_worker=True, **kwargs)


async def test_terminal_detach_keeps_shell_and_worker(shiitake_client: AsyncShiitakeClient) -> None:
    async with await _session(shiitake_client) as session:
        first = _Term(await session.attach_terminal())
        await first.send(b"export TERM_MARKER=kept; echo SET\n")
        await first.wait_for(b"SET")
        await first.close()
        assert (await session.status()).state == "active"
        second = _Term(await session.attach_terminal())
        await second.send(b"echo VALUE=$TERM_MARKER\n")
        await second.wait_for(b"VALUE=kept")
        await second.close()


async def _assert_control_byte(c: AsyncShiitakeClient, script: str, ctrl: int, marker: bytes) -> None:
    async with await _session(c) as session:
        term = _Term(await session.attach_terminal())
        try:
            await term.send(f"bash -c '{script}'\n".encode())
            await term.wait_for(b"READY")
            await term.send(bytes([ctrl]))
            await term.wait_for(marker)
        finally:
            await term.close()


async def test_ctrl_c_delivers_sigint(shiitake_client: AsyncShiitakeClient) -> None:
    await _assert_control_byte(
        shiitake_client, 'trap "echo CAUGHT_INT; exit 0" INT; echo READY; read _',
        0x03, b"CAUGHT_INT",
    )


async def test_ctrl_backslash_delivers_sigquit(shiitake_client: AsyncShiitakeClient) -> None:
    await _assert_control_byte(
        shiitake_client, 'trap "echo CAUGHT_QUIT; exit 0" QUIT; echo READY; read _',
        0x1C, b"CAUGHT_QUIT",
    )


async def test_ctrl_z_delivers_sigtstp(shiitake_client: AsyncShiitakeClient) -> None:
    await _assert_control_byte(
        shiitake_client, 'trap "echo CAUGHT_TSTP; exit 0" TSTP; echo READY; read _',
        0x1A, b"CAUGHT_TSTP",
    )


async def test_ctrl_d_signals_eof(shiitake_client: AsyncShiitakeClient) -> None:
    await _assert_control_byte(
        shiitake_client, 'echo READY; if read _; then echo GOTLINE; else echo GOTEOF; fi',
        0x04, b"GOTEOF",
    )


async def test_resize_reaches_tty(shiitake_client: AsyncShiitakeClient) -> None:
    async with await _session(shiitake_client) as session:
        term = _Term(await session.attach_terminal())
        try:
            await term.terminal.resize(100, 40)
            await term.send(b"stty size\n")
            await term.wait_for(b"40 100")
        finally:
            await term.close()


@pytest.mark.parametrize("frame", ["not json", '{"op":"unknown"}', '{"op":"resize","cols":0,"rows":24}'])
async def test_invalid_terminal_control_closes_attachment(
    shiitake_client: AsyncShiitakeClient, frame: str,
) -> None:
    async with await _session(shiitake_client) as session:
        terminal = await session.attach_terminal()
        await terminal._ws.send(frame)
        await asyncio.wait_for(terminal._ws.wait_closed(), 5)
        assert terminal.close_code == 1007
        assert terminal.close_reason == "invalid terminal control"
        assert (await session.status()).state == "active"


async def test_output_is_byte_transparent(shiitake_client: AsyncShiitakeClient) -> None:
    async with await _session(shiitake_client) as session:
        term = _Term(await session.attach_terminal())
        try:
            await term.send(b"stty raw -echo; printf 'A\\033[31m\\377\\376\\300\\200\\000Z'; stty sane\n")
            needle = b"A\x1b[31m\xff\xfe\xc0\x80\x00Z"
            await term.wait_for(needle)
            assert needle in term.buf
        finally:
            await term.close()


async def test_large_output_survives_backpressure(shiitake_client: AsyncShiitakeClient) -> None:
    async with await _session(shiitake_client) as session:
        terminal = await session.attach_terminal()
        await terminal.send(b"stty -echo\n")
        await asyncio.sleep(0.1)
        await terminal.send(b"head -c 5000000 /dev/zero | tr '\\0' X; echo DONE\n")
        buf = bytearray()
        paused = False
        async with asyncio.timeout(45):
            async for chunk in terminal.output():
                buf.extend(chunk)
                if not paused and len(buf) >= 100_000:
                    await asyncio.sleep(0.5)
                    paused = True
                if b"DONE" in buf:
                    break
        await terminal.aclose()
        assert bytes(buf).count(b"X") == 5_000_000


async def test_quiet_session_outlives_worker_lease(shiitake_client: AsyncShiitakeClient) -> None:
    async with await _session(shiitake_client) as session:
        term = _Term(await session.attach_terminal())
        try:
            await term.send(b"echo READY; sleep 18; echo SURVIVED\n")
            await term.wait_for(b"READY")
            await term.wait_for(b"SURVIVED", deadline=30)
        finally:
            await term.close()


async def test_two_sessions_reserve_two_workers(shiitake_client: AsyncShiitakeClient) -> None:
    async with await _session(shiitake_client) as a:
        async with await _session(shiitake_client) as b:
            assert a.worker_id != b.worker_id
            assert (await shiitake_client.health()).workers_sessions >= 2
            ta = _Term(await a.attach_terminal())
            tb = _Term(await b.attach_terminal())
            try:
                await ta.send(b"echo START_A\n")
                await tb.send(b"echo START_B\n")
                await ta.wait_for(b"START_A")
                await tb.wait_for(b"START_B")
            finally:
                await ta.close()
                await tb.close()


@pytest.mark.skipif(not _ROOT_WORKERS, reason="named-user drop needs root workers")
async def test_named_user_shell_has_home(shiitake_client: AsyncShiitakeClient) -> None:
    async with await _session(shiitake_client, drop_to={
        "uid": 2_000_123, "gid": 2_000_123, "supplementary_gids": [],
        "umask": 0o022, "name": "alice", "create_home": True,
    }) as session:
        term = _Term(await session.attach_terminal())
        try:
            await term.send(b"echo whoami=$(whoami); echo home=$HOME\n")
            await term.wait_for(b"whoami=alice")
            await term.wait_for(b"home=/var/lib/shiitake-homes/alice")
        finally:
            await term.close()
