# shiitake-py

Python client for the [shiitake](../../) command-dispatch HTTP API.

Fire-and-forget: `spawn` returns a handle the moment a worker accepts the
command; poll the handle and read output back by HTTP range. The client is
policy-free — pass `env` and an optional `drop_to` (uid/gid) directly; auth,
identity, and env policy are the embedding layer's concern.

```python
import asyncio
from shiitake.client import AsyncShiitakeClient, DropTo

async def main():
    async with AsyncShiitakeClient("http://localhost:8080", auth_token="…") as c:
        # Spawn-and-wait: RunResult with stdout/stderr slurped inline.
        result = await c.run("echo hi")
        print(result.stdout, result.exit_cause)

        # Handle API: spawn returns immediately; poll or range-read as needed.
        h = await c.spawn("sleep 1", drop_to=DropTo(uid=10001, gid=10001))
        snap = await h.wait()
        print(snap.status, snap.exit_code)

asyncio.run(main())
```

`command` runs as `bash -c <command>` — use ordinary shell syntax for pipes,
redirects, and multi-statement scripts. Output is read by byte range, so large
output never has to be buffered in memory:

```python
chunk = await h.read("stdout", from_=0, max_=64 * 1024)
print(chunk.content, chunk.eof, chunk.bytes_written)
```
