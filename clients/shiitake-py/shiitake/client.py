"""Async + sync HTTP clients for the shiitake API (``/api/v1``).

A fire-and-forget ``POST /exec`` returns a handle; poll ``GET /exec/{handle}``
and read output by HTTP ``Range`` from ``/exec/{handle}/{stdout,stderr}``. This
client is policy-free: the caller supplies ``env`` and an optional ``drop_to``
directly. Embedding layers add their own auth/identity/env policy on top.
"""

from __future__ import annotations

import asyncio
import dataclasses
import random
from collections.abc import Mapping
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
        status = await handle.wait()
        stdout, _ = await handle.slurp("stdout", max_bytes=max_inline_bytes)
        stderr, _ = await handle.slurp("stderr", max_bytes=max_inline_bytes)
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
