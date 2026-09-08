"""Async + sync HTTP clients for the shiitake API (``/api/v1``).

A fire-and-forget ``POST /exec`` returns a handle; poll ``GET /exec/{handle}``
and read output by HTTP ``Range`` from ``/exec/{handle}/{stdout,stderr}``. This
client is policy-free: the caller supplies ``env`` and an optional ``drop_to``
directly. Embedding layers add their own auth/identity/env policy on top.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import random
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx

StreamName = Literal["stdout", "stderr"]

_RETRYABLE_EXC: tuple[type[Exception], ...] = (
    httpx.RemoteProtocolError,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.WriteError,
    httpx.ReadTimeout,
)
_RETRYABLE_STATUS: tuple[int, ...] = (429, 502, 503, 504)
_API_PREFIX = "/api/v1"


class ShiitakeError(RuntimeError):
    """A non-2xx response from the server."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"shiitake {status}: {message}")
        self.status = status
        self.message = message


class NoIdleWorkerError(ShiitakeError):
    """`POST /exec` returned 429 — the worker pool is exhausted."""

    def __init__(self) -> None:
        super().__init__(429, "all workers busy")


@dataclass
class DropTo:
    """Privilege-drop directive: the worker does setgid → setgroups → setuid →
    umask before exec. uid/gid are numeric; identity mapping is the caller's
    concern."""

    uid: int
    gid: int
    supplementary_gids: list[int] = field(default_factory=list)
    umask: int | None = None

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "uid": self.uid,
            "gid": self.gid,
            "supplementary_gids": list(self.supplementary_gids),
        }
        if self.umask is not None:
            out["umask"] = self.umask
        return out


@dataclass
class SpawnResponse:
    handle: str
    started_at: float


@dataclass
class StatusResponse:
    handle: str
    worker_id: str
    status: str
    started_at: float
    finished_at: float | None = None
    exit_code: int | None = None
    exit_cause: str | None = None
    exit_signal: int | None = None
    timed_out: bool = False
    cancelled: bool = False
    stdout_bytes_written: int = 0
    stderr_bytes_written: int = 0

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> StatusResponse:
        known = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in known})


@dataclass
class HealthResponse:
    status: str
    service: str
    workers_idle: int = 0
    workers_inflight: int = 0
    workers_pinned: int = 0


@dataclass
class ReadyResponse:
    """Readiness verdict. ``ready`` mirrors the HTTP status (200 / 503):
    the pool has at least ``workers_required`` registered workers, counting
    both idle and in-flight ones."""

    ready: bool
    service: str
    workers_idle: int = 0
    workers_inflight: int = 0
    workers_required: int = 0


@dataclass
class ReadChunk:
    content: str
    next_offset: int
    bytes_written: int
    eof: bool


@dataclass
class RunResult:
    stdout: str
    stderr: str
    exit_code: int
    status: str
    exit_cause: str | None = None
    exit_signal: int | None = None
    stdout_bytes_written: int = 0
    stderr_bytes_written: int = 0


def _total_from_content_range(value: str | None) -> int:
    if not value:
        return 0
    tail = value.rsplit("/", 1)[-1].strip()
    return int(tail) if tail.isdigit() else 0


class _Base:
    def __init__(
        self,
        base_url: str,
        *,
        auth_token: str | None = None,
        timeout: float = 300.0,
        max_retries: int = 5,
        retry_initial_delay: float = 0.5,
        retry_max_delay: float = 10.0,
        api_prefix: str = _API_PREFIX,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_prefix = api_prefix
        self._auth_token = auth_token or ""
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_initial = retry_initial_delay
        self._retry_max = retry_max_delay

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._auth_token}"} if self._auth_token else {}

    def _url(self, path: str) -> str:
        return f"{self._base_url}{self._api_prefix}{path}"

    def _backoff(self, attempt: int) -> float:
        base = min(self._retry_initial * (2**attempt), self._retry_max)
        return base * (0.5 + random.random())


class AsyncHandle:
    """Reference to a spawned command; every call goes through the parent client."""

    def __init__(self, client: AsyncShiitakeClient, handle: str, started_at: float) -> None:
        self._client = client
        self.handle = handle
        self.started_at = started_at

    async def poll(self) -> StatusResponse:
        return await self._client.status(self.handle)

    async def wait(self, poll_interval: float = 0.2) -> StatusResponse:
        """Poll until the handle leaves Running."""
        attempt = 0
        while True:
            snap = await self.poll()
            if snap.status != "running":
                return snap
            await asyncio.sleep(min(poll_interval * (1.5**attempt), 2.0))
            attempt += 1

    async def kill(self) -> None:
        await self._client.kill(self.handle)

    async def read(self, stream: StreamName, *, from_: int = 0, max_: int | None = None) -> ReadChunk:
        return await self._client.read(self.handle, stream, from_=from_, max_=max_)

    async def slurp(self, stream: StreamName, *, max_bytes: int = 64 * 1024) -> tuple[str, bool]:
        """Read up to ``max_bytes`` from offset 0; returns (content, truncated)."""
        r = await self.read(stream, from_=0, max_=max_bytes)
        return r.content, r.bytes_written > len(r.content.encode("utf-8", errors="replace"))


class AsyncPtySession:
    """A live interactive terminal over one ``/api/v1/pty`` WebSocket.

    Binary frames are the raw byte stream — server→client is pty **output**,
    client→server is **stdin**; JSON text frames are control (only ``resize``
    from the client). Use as an async context manager; ``output()`` yields until
    the shell exits or the socket closes."""

    def __init__(self, ws: Any) -> None:
        self._ws = ws

    async def output(self) -> AsyncIterator[bytes]:
        """Yield pty output chunks until the session ends. Text frames (a close
        reason, say) are not terminal bytes and are skipped."""
        with contextlib.suppress(Exception):
            async for message in self._ws:
                if isinstance(message, (bytes, bytearray)):
                    yield bytes(message)

    async def send(self, data: bytes) -> None:
        """Write keystrokes to the pty (stdin)."""
        await self._ws.send(data)

    async def resize(self, cols: int, rows: int) -> None:
        """Reflow the tty so full-screen programs repaint."""
        await self._ws.send(json.dumps({"op": "resize", "cols": cols, "rows": rows}))

    async def aclose(self) -> None:
        with contextlib.suppress(Exception):
            await self._ws.close()

    async def __aenter__(self) -> AsyncPtySession:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()


class AsyncShiitakeClient(_Base):
    """Async client. Reuses a single ``httpx.AsyncClient`` for connection pooling."""

    def __init__(self, base_url: str, **kwargs: Any) -> None:
        super().__init__(base_url, **kwargs)
        self._http = httpx.AsyncClient(timeout=self._timeout)

    async def __aenter__(self) -> AsyncShiitakeClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _request(
        self,
        method: str,
        url: str,
        *,
        json: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        retry_on_429: bool = False,
    ) -> httpx.Response:
        headers = self._headers()
        if extra_headers:
            headers.update(extra_headers)
        last_exc: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                resp = await self._http.request(method, url, headers=headers, json=json)
            except _RETRYABLE_EXC as exc:
                last_exc = exc
                if attempt + 1 >= self._max_retries:
                    break
                await asyncio.sleep(self._backoff(attempt))
                continue
            if retry_on_429 and resp.status_code in _RETRYABLE_STATUS:
                if attempt + 1 >= self._max_retries:
                    return resp
                await asyncio.sleep(self._backoff(attempt))
                continue
            return resp
        assert last_exc is not None
        raise last_exc

    async def health(self) -> HealthResponse:
        resp = await self._request("GET", self._url("/health"))
        _raise_for_status(resp)
        d = resp.json()
        return HealthResponse(
            status=d["status"],
            service=d["service"],
            workers_idle=d.get("workers_idle", 0),
            workers_inflight=d.get("workers_inflight", 0),
            workers_pinned=d.get("workers_pinned", 0),
        )

    async def ready(self) -> ReadyResponse:
        """Readiness, as opposed to ``health``'s liveness. A ``503`` means
        "not ready", not a failed call — the verdict is on ``.ready``."""
        resp = await self._request("GET", self._url("/ready"))
        if resp.status_code != httpx.codes.SERVICE_UNAVAILABLE:
            _raise_for_status(resp)
        d = resp.json()
        return ReadyResponse(
            ready=d["ready"],
            service=d["service"],
            workers_idle=d.get("workers_idle", 0),
            workers_inflight=d.get("workers_inflight", 0),
            workers_required=d.get("workers_required", 0),
        )

    async def spawn(
        self,
        command: str,
        *,
        workdir: str | None = None,
        timeout: float = 300.0,
        env: dict[str, str] | None = None,
        drop_to: DropTo | Mapping[str, Any] | None = None,
        wait_for_worker: bool = False,
    ) -> AsyncHandle:
        """Run ``command`` as ``bash -c <command>``. Returns a handle once a
        worker accepts it (or raises ``NoIdleWorkerError`` on 429; pass
        ``wait_for_worker`` to retry instead)."""
        body: dict[str, Any] = {"command": command, "timeout": timeout, "env": env or {}}
        if workdir is not None:
            body["workdir"] = workdir
        if drop_to is not None:
            body["drop_to"] = drop_to.to_json() if isinstance(drop_to, DropTo) else dict(drop_to)
        resp = await self._request("POST", self._url("/exec"), json=body, retry_on_429=wait_for_worker)
        if resp.status_code == 429:
            raise NoIdleWorkerError
        _raise_for_status(resp)
        d = resp.json()
        return AsyncHandle(self, d["handle"], d["started_at"])

    async def status(self, handle: str) -> StatusResponse:
        resp = await self._request("GET", self._url(f"/exec/{handle}"))
        _raise_for_status(resp)
        return StatusResponse.from_dict(resp.json())

    async def kill(self, handle: str) -> None:
        resp = await self._request("DELETE", self._url(f"/exec/{handle}"))
        _raise_for_status(resp)

    async def attach_pty(
        self,
        *,
        working_dir: str,
        command: list[str] | None = None,
        env: dict[str, str] | None = None,
        cols: int = 80,
        rows: int = 24,
        drop_to: DropTo | Mapping[str, Any] | None = None,
    ) -> AsyncPtySession:
        """Open an interactive PTY. Pins one worker for the session's life;
        raises if none is idle. ``command`` empty means the worker's default
        shell. Requires the ``pty`` extra (``shiitake-py[pty]``)."""
        try:
            from websockets.asyncio.client import connect as ws_connect
        except ImportError as exc:  # pragma: no cover - import-guard
            raise RuntimeError("attach_pty needs the 'pty' extra: pip install shiitake-py[pty]") from exc

        # `/pty` is the same origin + prefix as `/exec`, over ws(s) not http(s).
        ws_url = self._url("/pty").replace("https://", "wss://", 1).replace("http://", "ws://", 1)
        open_frame: dict[str, Any] = {
            "op": "open",
            "command": command or [],
            "working_dir": working_dir,
            "env": env or {},
            "cols": cols,
            "rows": rows,
        }
        if drop_to is not None:
            open_frame["drop_to"] = drop_to.to_json() if isinstance(drop_to, DropTo) else dict(drop_to)
        ws = await ws_connect(ws_url, additional_headers=self._headers())
        try:
            await ws.send(json.dumps(open_frame))
        except Exception:
            with contextlib.suppress(Exception):
                await ws.close()
            raise
        return AsyncPtySession(ws)

    async def read(
        self,
        handle: str,
        stream: StreamName,
        *,
        from_: int = 0,
        max_: int | None = None,
    ) -> ReadChunk:
        """Read a slice of a captured stream over HTTP ``Range``. Reading past
        EOF yields an empty, ``eof=True`` chunk rather than an error."""
        extra: dict[str, str] = {}
        if from_ or max_ is not None:
            end = "" if max_ is None else str(from_ + max_ - 1)
            extra["Range"] = f"bytes={from_}-{end}"
        resp = await self._request("GET", self._url(f"/exec/{handle}/{stream}"), extra_headers=extra)
        if resp.status_code == 416:
            total = _total_from_content_range(resp.headers.get("content-range"))
            return ReadChunk(content="", next_offset=from_, bytes_written=total, eof=True)
        _raise_for_status(resp)
        body = resp.content
        cr = resp.headers.get("content-range")
        total = _total_from_content_range(cr) if (resp.status_code == 206 and cr) else from_ + len(body)
        next_offset = from_ + len(body)
        return ReadChunk(
            content=body.decode("utf-8", errors="replace"),
            next_offset=next_offset,
            bytes_written=total,
            eof=next_offset >= total,
        )

    async def run(
        self,
        command: str,
        *,
        workdir: str | None = None,
        timeout: float = 300.0,
        env: dict[str, str] | None = None,
        drop_to: DropTo | Mapping[str, Any] | None = None,
        max_inline_bytes: int = 64 * 1024,
        wait_for_worker: bool = True,
    ) -> RunResult:
        """Spawn + wait + slurp small output into one result. Output beyond
        ``max_inline_bytes`` stays on the server (the ``*_bytes_written`` fields
        reflect the full size)."""
        handle = await self.spawn(
            command,
            workdir=workdir,
            timeout=timeout,
            env=env,
            drop_to=drop_to,
            wait_for_worker=wait_for_worker,
        )
        try:
            status = await handle.wait()
            stdout, _ = await handle.slurp("stdout", max_bytes=max_inline_bytes)
            stderr, _ = await handle.slurp("stderr", max_bytes=max_inline_bytes)
        except asyncio.CancelledError:
            # Cancelling the await only stops the polling here; the command keeps
            # running on its worker and holds that slot until it exits or the
            # server times it out. `run` owns this handle, so it kills it.
            with contextlib.suppress(Exception):
                await handle.kill()
            raise
        return RunResult(
            stdout=stdout,
            stderr=stderr,
            exit_code=status.exit_code if status.exit_code is not None else -1,
            status=status.status,
            exit_cause=status.exit_cause,
            exit_signal=status.exit_signal,
            stdout_bytes_written=status.stdout_bytes_written,
            stderr_bytes_written=status.stderr_bytes_written,
        )


class ShiitakeClient(_Base):
    """Sync wrapper around a one-shot ``AsyncShiitakeClient.run`` — for scripts."""

    def __init__(self, base_url: str, **kwargs: Any) -> None:
        super().__init__(base_url, **kwargs)
        self._kwargs = kwargs

    def run(
        self,
        command: str,
        *,
        workdir: str | None = None,
        timeout: float = 300.0,
        env: dict[str, str] | None = None,
        drop_to: DropTo | Mapping[str, Any] | None = None,
        max_inline_bytes: int = 64 * 1024,
    ) -> RunResult:
        async def _go() -> RunResult:
            async with AsyncShiitakeClient(self._base_url, **self._kwargs) as c:
                return await c.run(
                    command,
                    workdir=workdir,
                    timeout=timeout,
                    env=env,
                    drop_to=drop_to,
                    max_inline_bytes=max_inline_bytes,
                )

        return asyncio.run(_go())


def _raise_for_status(resp: httpx.Response) -> None:
    if 200 <= resp.status_code < 300:
        return
    raise ShiitakeError(resp.status_code, resp.text)
