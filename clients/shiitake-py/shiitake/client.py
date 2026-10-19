"""Async and sync clients for explicit shiitake sessions and their commands."""

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
CommandMode = Literal["stateful", "parallel"]

_RETRYABLE_EXC: tuple[type[Exception], ...] = (
    httpx.RemoteProtocolError,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.WriteError,
    httpx.ReadTimeout,
)
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
    name: str | None = None
    create_home: bool = False

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "uid": self.uid,
            "gid": self.gid,
            "supplementary_gids": list(self.supplementary_gids),
        }
        if self.umask is not None:
            out["umask"] = self.umask
        if self.name is not None:
            out["name"] = self.name
        if self.create_home:
            out["create_home"] = True
        return out


@dataclass
class ProfileResponse:
    name: str
    idle: int
    allocated: int


@dataclass
class SessionResponse:
    session_id: str
    worker_id: str
    profile: str
    state: str
    created_at: float


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
    session_id: str | None = None
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
    workers_interactive: int = 0
    workers_sessions: int = 0


@dataclass
class ReadyResponse:
    """Readiness verdict. ``ready`` mirrors the HTTP status (200 / 503):
    the pool has at least ``workers_required`` registered workers, counting
    both idle and in-flight ones."""

    ready: bool
    service: str
    workers_idle: int = 0
    workers_inflight: int = 0
    workers_sessions: int = 0
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

    def __init__(self, client: AsyncShiitakeClient, session_id: str, handle: str, started_at: float) -> None:
        self._client = client
        self.session_id = session_id
        self.handle = handle
        self.started_at = started_at

    async def poll(self) -> StatusResponse:
        return await self._client.command_status(self.session_id, self.handle)

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
        await self._client.kill_command(self.session_id, self.handle)

    async def read(self, stream: StreamName, *, from_: int = 0, max_: int | None = None) -> ReadChunk:
        return await self._client.read_command(self.session_id, self.handle, stream, from_=from_, max_=max_)

    async def slurp(self, stream: StreamName, *, max_bytes: int = 64 * 1024) -> tuple[str, bool]:
        """Read up to ``max_bytes`` from offset 0; returns (content, truncated)."""
        r = await self.read(stream, from_=0, max_=max_bytes)
        return r.content, r.bytes_written > len(r.content.encode("utf-8", errors="replace"))


class AsyncPtySession:
    """A detachable terminal attachment to a persistent session.

    Binary frames are the raw byte stream — server→client is pty **output**,
    client→server is **stdin**; JSON text frames carry resize. Closing this
    attachment leaves the session and shell alive."""

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

    @property
    def close_code(self) -> int | None:
        """WebSocket close code once the session ends (``None`` while open). 1000
        is a clean shell exit; 1011 an abnormal end such as the worker dropping."""
        return self._ws.close_code

    @property
    def close_reason(self) -> str:
        """Human-readable close reason once the session ends (empty while open)."""
        return self._ws.close_reason or ""

    async def aclose(self) -> None:
        with contextlib.suppress(Exception):
            await self._ws.close()

    async def __aenter__(self) -> AsyncPtySession:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()


class AsyncSession:
    """A reserved worker and shell. Release explicitly or use an async context manager."""

    def __init__(self, client: AsyncShiitakeClient, details: SessionResponse) -> None:
        self._client = client
        self.session_id = details.session_id
        self.worker_id = details.worker_id
        self.profile = details.profile
        self.created_at = details.created_at

    async def status(self) -> SessionResponse:
        return await self._client.session_status(self.session_id)

    async def spawn(
        self, command: str, *, timeout: float = 300.0,
        mode: CommandMode = "stateful",
    ) -> AsyncHandle:
        body = {"command": command, "timeout": timeout, "mode": mode}
        resp = await self._client._request(
            "POST", self._client._url(f"/sessions/{self.session_id}/commands"),
            json=body, retry_transport=False,
        )
        _raise_for_status(resp)
        d = resp.json()
        return AsyncHandle(self._client, self.session_id, d["handle"], d["started_at"])

    async def run(
        self, command: str, *, timeout: float = 300.0,
        mode: CommandMode = "stateful", max_inline_bytes: int = 64 * 1024
    ) -> RunResult:
        handle = await self.spawn(command, timeout=timeout, mode=mode)
        try:
            status = await handle.wait()
            stdout, _ = await handle.slurp("stdout", max_bytes=max_inline_bytes)
            stderr, _ = await handle.slurp("stderr", max_bytes=max_inline_bytes)
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await handle.kill()
            raise
        return RunResult(
            stdout=stdout, stderr=stderr,
            exit_code=status.exit_code if status.exit_code is not None else -1,
            status=status.status, exit_cause=status.exit_cause,
            exit_signal=status.exit_signal,
            stdout_bytes_written=status.stdout_bytes_written,
            stderr_bytes_written=status.stderr_bytes_written,
        )

    async def attach_terminal(self) -> AsyncPtySession:
        return await self._client.attach_terminal(self.session_id)

    async def release(self) -> SessionResponse:
        return await self._client.release_session(self.session_id)

    async def __aenter__(self) -> AsyncSession:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.release()


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
        retry_transport: bool = True,
    ) -> httpx.Response:
        headers = self._headers()
        if extra_headers:
            headers.update(extra_headers)
        last_exc: Exception | None = None
        attempts = self._max_retries if (retry_transport or retry_on_429) else 1
        for attempt in range(attempts):
            try:
                resp = await self._http.request(method, url, headers=headers, json=json)
            except _RETRYABLE_EXC as exc:
                last_exc = exc
                if not retry_transport or attempt + 1 >= attempts:
                    break
                await asyncio.sleep(self._backoff(attempt))
                continue
            if retry_on_429 and resp.status_code == 429:
                if attempt + 1 >= attempts:
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
            workers_interactive=d.get("workers_interactive", 0),
            workers_sessions=d.get("workers_sessions", 0),
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
            workers_sessions=d.get("workers_sessions", 0),
            workers_required=d.get("workers_required", 0),
        )

    async def profiles(self) -> list[ProfileResponse]:
        resp = await self._request("GET", self._url("/profiles"))
        _raise_for_status(resp)
        return [ProfileResponse(**p) for p in resp.json()]

    async def create_session(
        self, *, profile: str = "default", workdir: str | None = None,
        env: dict[str, str] | None = None,
        drop_to: DropTo | Mapping[str, Any] | None = None,
        cols: int = 80, rows: int = 24, wait_for_worker: bool = False,
    ) -> AsyncSession:
        body: dict[str, Any] = {
            "profile": profile, "env": env or {}, "cols": cols, "rows": rows,
        }
        if workdir is not None:
            body["workdir"] = workdir
        if drop_to is not None:
            body["drop_to"] = drop_to.to_json() if isinstance(drop_to, DropTo) else dict(drop_to)
        resp = await self._request(
            "POST", self._url("/sessions"), json=body,
            retry_on_429=wait_for_worker, retry_transport=False,
        )
        if resp.status_code == 429:
            raise NoIdleWorkerError
        _raise_for_status(resp)
        return AsyncSession(self, SessionResponse(**resp.json()))

    async def session_status(self, session_id: str) -> SessionResponse:
        resp = await self._request("GET", self._url(f"/sessions/{session_id}"))
        _raise_for_status(resp)
        return SessionResponse(**resp.json())

    async def release_session(self, session_id: str) -> SessionResponse:
        resp = await self._request("DELETE", self._url(f"/sessions/{session_id}"))
        _raise_for_status(resp)
        return SessionResponse(**resp.json())

    async def command_status(self, session_id: str, handle: str) -> StatusResponse:
        resp = await self._request(
            "GET", self._url(f"/sessions/{session_id}/commands/{handle}")
        )
        _raise_for_status(resp)
        return StatusResponse.from_dict(resp.json())

    async def kill_command(self, session_id: str, handle: str) -> None:
        resp = await self._request(
            "DELETE", self._url(f"/sessions/{session_id}/commands/{handle}")
        )
        _raise_for_status(resp)

    async def read_command(
        self, session_id: str, handle: str, stream: StreamName,
        *, from_: int = 0, max_: int | None = None,
    ) -> ReadChunk:
        extra: dict[str, str] = {}
        if from_ or max_ is not None:
            end = "" if max_ is None else str(from_ + max_ - 1)
            extra["Range"] = f"bytes={from_}-{end}"
        resp = await self._request(
            "GET", self._url(f"/sessions/{session_id}/commands/{handle}/{stream}"),
            extra_headers=extra,
        )
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
            next_offset=next_offset, bytes_written=total, eof=next_offset >= total,
        )

    async def attach_terminal(self, session_id: str) -> AsyncPtySession:
        try:
            from websockets.asyncio.client import connect as ws_connect
        except ImportError as exc:  # pragma: no cover - import-guard
            raise RuntimeError("terminal needs the 'pty' extra: pip install shiitake-py[pty]") from exc
        ws_url = self._url(f"/sessions/{session_id}/terminal")
        ws_url = ws_url.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
        ws = await ws_connect(ws_url, additional_headers=self._headers())
        return AsyncPtySession(ws)



class ShiitakeClient(_Base):
    """Synchronous client. Commands run through an explicitly created session."""

    def __init__(self, base_url: str, **kwargs: Any) -> None:
        super().__init__(base_url, **kwargs)
        self._http = httpx.Client(timeout=self._timeout)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> ShiitakeClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def create_session(
        self, *, profile: str = "default", workdir: str | None = None,
        env: dict[str, str] | None = None,
        drop_to: DropTo | Mapping[str, Any] | None = None,
        cols: int = 80, rows: int = 24,
    ) -> Session:
        body: dict[str, Any] = {"profile": profile, "env": env or {}, "cols": cols, "rows": rows}
        if workdir is not None:
            body["workdir"] = workdir
        if drop_to is not None:
            body["drop_to"] = drop_to.to_json() if isinstance(drop_to, DropTo) else dict(drop_to)
        resp = self._http.post(self._url("/sessions"), headers=self._headers(), json=body)
        if resp.status_code == 429:
            raise NoIdleWorkerError
        _raise_for_status(resp)
        return Session(self, SessionResponse(**resp.json()))


class Session:
    """Synchronous session with explicit release or a context manager."""

    def __init__(self, client: ShiitakeClient, details: SessionResponse) -> None:
        self._client = client
        self.session_id = details.session_id
        self.worker_id = details.worker_id
        self.profile = details.profile

    def release(self) -> SessionResponse:
        resp = self._client._http.delete(
            self._client._url(f"/sessions/{self.session_id}"),
            headers=self._client._headers(),
        )
        _raise_for_status(resp)
        return SessionResponse(**resp.json())

    def run(
        self, command: str, *, timeout: float = 300.0,
        mode: CommandMode = "stateful", max_inline_bytes: int = 64 * 1024
    ) -> RunResult:
        base = self._client._url(f"/sessions/{self.session_id}/commands")
        resp = self._client._http.post(
            base, headers=self._client._headers(),
            json={"command": command, "timeout": timeout, "mode": mode}
        )
        _raise_for_status(resp)
        handle = resp.json()["handle"]
        status_url = f"{base}/{handle}"
        delay = 0.2
        while True:
            resp = self._client._http.get(status_url, headers=self._client._headers())
            _raise_for_status(resp)
            status = StatusResponse.from_dict(resp.json())
            if status.status != "running":
                break
            import time

            time.sleep(delay)
            delay = min(delay * 1.5, 2.0)
        streams: dict[str, str] = {}
        for stream in ("stdout", "stderr"):
            resp = self._client._http.get(
                f"{status_url}/{stream}",
                headers={**self._client._headers(), "Range": f"bytes=0-{max_inline_bytes - 1}"},
            )
            if resp.status_code == 416:
                streams[stream] = ""
            else:
                _raise_for_status(resp)
                streams[stream] = resp.content.decode("utf-8", errors="replace")
        return RunResult(
            stdout=streams["stdout"], stderr=streams["stderr"],
            exit_code=status.exit_code if status.exit_code is not None else -1,
            status=status.status, exit_cause=status.exit_cause, exit_signal=status.exit_signal,
            stdout_bytes_written=status.stdout_bytes_written,
            stderr_bytes_written=status.stderr_bytes_written,
        )

    def __enter__(self) -> Session:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()


def _raise_for_status(resp: httpx.Response) -> None:
    if 200 <= resp.status_code < 300:
        return
    raise ShiitakeError(resp.status_code, resp.text)
