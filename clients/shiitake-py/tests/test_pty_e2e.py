"""End-to-end tests for the interactive terminal (``/api/v1/pty``).

The ``shiitake_client`` fixture (conftest.py) skips these unless
``SHIITAKE_E2E_URL`` points at a running server. Drives the terminal over a real
WebSocket: control bytes reach the shell as signals, output is byte-transparent,
resize reaches the tty, backpressure is lossless, a quiet session outlives the
worker lease, and concurrent sessions each pin their own worker. The
``websockets`` dep behind ``attach_pty`` is in the e2e dev group.
"""

from __future__ import annotations

import asyncio
import contextlib
import os

import pytest
from shiitake.client import AsyncPtySession, AsyncShiitakeClient

# The named-user drop writes /etc/passwd and setuids, so it needs a root worker.
# The cluster's workers are root; run.sh sets this. Skipped otherwise.
_ROOT_WORKERS = os.environ.get("SHIITAKE_E2E_ROOT_WORKERS") == "1"
# `/tmp`, `stty`, `tr`, `head` need a PATH — the worker runs with a cleared env.
_PATH_ENV = {"PATH": "/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"}


class _Term:
    """A PTY session with a background reader draining output into one buffer, so
    a test can wait for a marker, send input, and keep reading with no gap — a
    second reader over the same socket would race the first."""

    def __init__(self, session: AsyncPtySession) -> None:
        self._session = session
        self._buf = bytearray()
        self._closed = False
        self._task = asyncio.create_task(self._drain())

    async def _drain(self) -> None:
        async for chunk in self._session.output():
            self._buf.extend(chunk)
        self._closed = True

    async def wait_for(self, needle: bytes, deadline: float = 15.0) -> None:
        async def poll() -> None:
            while needle not in self._buf and not self._closed:
                await asyncio.sleep(0.05)

        await asyncio.wait_for(poll(), deadline)
        assert needle in self._buf, f"{needle!r} never arrived; got {bytes(self._buf)!r}"

    async def wait_closed(self, deadline: float = 20.0) -> None:
        await asyncio.wait_for(asyncio.shield(self._task), deadline)

    async def send(self, data: bytes) -> None:
        await self._session.send(data)

    async def resize(self, cols: int, rows: int) -> None:
        await self._session.resize(cols, rows)

    @property
    def output(self) -> bytes:
        return bytes(self._buf)

    async def aclose(self) -> None:
        await self._session.aclose()
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


async def _open_term(c: AsyncShiitakeClient, **kwargs) -> _Term:
    """Open a PTY session (cwd `/tmp`) and wrap it in a background-reading `_Term`."""
    return _Term(await c.attach_pty(working_dir="/tmp", **kwargs))


async def _wait_idle(c: AsyncShiitakeClient, want: int, deadline: float = 30.0) -> None:
    async def poll() -> None:
        while (await c.health()).workers_idle < want:
            await asyncio.sleep(0.2)

    await asyncio.wait_for(poll(), deadline)


async def _wait_pinned(c: AsyncShiitakeClient, want: int, deadline: float = 20.0) -> None:
    async def poll() -> None:
        while (await c.health()).workers_pinned != want:
            await asyncio.sleep(0.1)

    await asyncio.wait_for(poll(), deadline)


async def test_pty_streams_a_shell_and_releases_the_worker(shiitake_client: AsyncShiitakeClient) -> None:
    await _wait_idle(shiitake_client, 1)
    term = await _open_term(shiitake_client, command=["bash", "-c", "echo HELLO_FROM_PTY; exit 7"])
    await term.wait_closed()
    await term.aclose()
    assert b"HELLO_FROM_PTY" in term.output, term.output
    # The worker unpins and resets after the shell exits.
    await _wait_pinned(shiitake_client, 0)


async def _assert_control_byte(
    c: AsyncShiitakeClient, script: str, ctrl: int, marker: bytes
) -> None:
    """Arm `script` (prints READY once armed), send one control byte, and assert
    `marker` — printed only if that byte had its terminal effect — comes back."""
    await _wait_idle(c, 1)
    term = await _open_term(c, command=["bash", "-c", script])
    try:
        await term.wait_for(b"READY")
        await term.send(bytes([ctrl]))
        await term.wait_for(marker)
    finally:
        await term.aclose()


async def test_pty_ctrl_c_delivers_sigint(shiitake_client: AsyncShiitakeClient) -> None:
    # 0x03 = VINTR -> SIGINT to the foreground process group.
    await _assert_control_byte(
        shiitake_client, "trap 'echo CAUGHT_INT; exit 0' INT; echo READY; read _", 0x03, b"CAUGHT_INT"
    )


async def test_pty_ctrl_backslash_delivers_sigquit(shiitake_client: AsyncShiitakeClient) -> None:
    # 0x1c = VQUIT -> SIGQUIT.
    await _assert_control_byte(
        shiitake_client, "trap 'echo CAUGHT_QUIT; exit 0' QUIT; echo READY; read _", 0x1C, b"CAUGHT_QUIT"
    )


async def test_pty_ctrl_z_delivers_sigtstp(shiitake_client: AsyncShiitakeClient) -> None:
    # 0x1a = VSUSP -> SIGTSTP (job-control suspend); trappable, unlike SIGSTOP.
    await _assert_control_byte(
        shiitake_client, "trap 'echo CAUGHT_TSTP; exit 0' TSTP; echo READY; read _", 0x1A, b"CAUGHT_TSTP"
    )


async def test_pty_ctrl_d_signals_eof(shiitake_client: AsyncShiitakeClient) -> None:
    # 0x04 = VEOF -> end-of-file on the read, not a byte delivered to it.
    await _assert_control_byte(
        shiitake_client, "echo READY; if read _; then echo GOTLINE; else echo GOTEOF; fi", 0x04, b"GOTEOF"
    )


async def test_pty_resize_reaches_the_tty(shiitake_client: AsyncShiitakeClient) -> None:
    """Resize to 100x40, then read the size back with `stty size` (prints "rows
    cols") — proving `op:resize` reached the pty as a TIOCSWINSZ. The newline
    unblocks `read` so `stty` samples the already-applied size, which avoids
    depending on when a bash version runs a SIGWINCH trap during a blocking read.
    """
    await _wait_idle(shiitake_client, 1)
    term = await _open_term(
        shiitake_client, command=["bash", "-c", "echo READY; read _; stty size"], env=_PATH_ENV
    )
    try:
        await term.wait_for(b"READY")
        await term.resize(100, 40)
        await term.send(b"\n")
        await term.wait_for(b"40 100")
    finally:
        await term.aclose()


async def test_pty_output_is_byte_transparent(shiitake_client: AsyncShiitakeClient) -> None:
    """`stty raw` drops output post-processing, then printf emits a CSI sequence,
    0xFF/0xFE, an overlong lead + lone continuation, and a NUL — none valid as a
    whole UTF-8 string. Output rides binary frames, so every byte survives."""
    await _wait_idle(shiitake_client, 1)
    cmd = "stty raw -echo; printf 'A\\033[31m\\377\\376\\300\\200\\000Z'; exit 0"
    term = await _open_term(shiitake_client, command=["bash", "-c", cmd], env=_PATH_ENV)
    await term.wait_closed()
    await term.aclose()
    needle = b"A\x1b[31m\xff\xfe\xc0\x80\x00Z"
    assert needle in term.output, f"arbitrary bytes must survive verbatim; got {term.output!r}"


async def test_pty_large_output_survives_backpressure(shiitake_client: AsyncShiitakeClient) -> None:
    await _wait_idle(shiitake_client, 1)
    session = await shiitake_client.attach_pty(
        working_dir="/tmp",
        command=["bash", "-c", "head -c 5000000 /dev/zero | tr '\\0' X"],
        env=_PATH_ENV,
    )
    # Pause mid-stream so the server's bounded backlog fills and back-pressures
    # the worker; then drain the rest. Every byte must still arrive.
    buf = bytearray()

    async def pump() -> None:
        paused = False
        async for chunk in session.output():
            buf.extend(chunk)
            if not paused and len(buf) >= 100_000:
                await asyncio.sleep(0.5)
                paused = True

    try:
        await asyncio.wait_for(pump(), 45.0)
    except asyncio.TimeoutError:
        pass
    await session.aclose()
    xs = bytes(buf).count(b"X")
    assert xs == 5_000_000, f"every byte must arrive under backpressure; got {xs} of 5000000"
    await _wait_pinned(shiitake_client, 0)


async def test_pty_quiet_session_outlives_the_worker_lease(shiitake_client: AsyncShiitakeClient) -> None:
    """A session that emits output but takes NO input for longer than the worker's
    lease must not be reaped: the /pty handler pings the pinned worker (which the
    pool's own keepalive skips) to keep its lease fresh. The chart sets a short
    lease + a shorter handler keepalive so this runs without a long wait."""
    await _wait_idle(shiitake_client, 1)
    # Quiet for ~18s — past the 15s lease — then prove the session is alive.
    term = await _open_term(
        shiitake_client, command=["bash", "-c", "echo READY; sleep 18; echo SURVIVED"], env=_PATH_ENV
    )
    try:
        await term.wait_for(b"READY")
        await term.wait_for(b"SURVIVED", deadline=30.0)
    finally:
        await term.aclose()


async def test_pty_two_sessions_each_pin_a_worker(shiitake_client: AsyncShiitakeClient) -> None:
    await _wait_idle(shiitake_client, 2)
    a = await _open_term(shiitake_client, command=["bash", "-c", "echo START_A; sleep 3"])
    b = await _open_term(shiitake_client, command=["bash", "-c", "echo START_B; sleep 3"])
    try:
        await a.wait_for(b"START_A")
        await b.wait_for(b"START_B")
        # Both sessions hold a pinned worker at the same time.
        await _wait_pinned(shiitake_client, 2)
    finally:
        await a.aclose()
        await b.aclose()
    await _wait_pinned(shiitake_client, 0)


@pytest.mark.skipif(not _ROOT_WORKERS, reason="named-user drop needs root workers")
async def test_pty_names_the_session_user_under_the_home_root(
    shiitake_client: AsyncShiitakeClient,
) -> None:
    """`drop_to.name` makes the shell run as a real login name — the worker writes
    a matching /etc/passwd entry + home before the setuid drop (root only). No
    HOME is sent, so the shell's `$HOME` also proves the created home lands under
    the configured `SHIITAKE_HOME_ROOT` (worker.homeRoot), off the default /home."""
    await _wait_idle(shiitake_client, 1)
    term = await _open_term(
        shiitake_client,
        command=["bash", "-c", "echo whoami=$(whoami); echo home=$HOME"],
        env=_PATH_ENV,
        drop_to={
            "uid": 2_000_123,
            "gid": 2_000_123,
            "supplementary_gids": [],
            "umask": 0o022,
            "name": "alice",
            "create_home": True,
        },
    )
    await term.wait_closed()
    await term.aclose()
    text = term.output.decode(errors="replace")
    assert "whoami=alice" in text, text
    # No HOME was sent, so $HOME is the home shiitake created: it must be the
    # named user's home AND relocated off the default /home by SHIITAKE_HOME_ROOT.
    home = next((ln.split("=", 1)[1] for ln in text.splitlines() if ln.startswith("home=")), "")
    assert home.endswith("/alice"), f"home should be the created home; got {home!r}"
    assert not home.startswith("/home/"), f"home should be relocated off /home; got {home!r}"
