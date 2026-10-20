# shiitake-py

Python client for the [Shiitake](../../README.md) session API. A session
reserves one worker in a named profile and fixes its shell identity, initial
environment, and working directory. Release it explicitly or use a context
manager.

```python
import asyncio
from shiitake.client import AsyncShiitakeClient, DropTo

async def main():
    async with AsyncShiitakeClient("http://localhost:8080", auth_token="…") as client:
        print(await client.profiles())
        async with await client.create_session(
            profile="default",
            env={"PATH": "/usr/bin:/bin"},
            drop_to=DropTo(uid=10001, gid=10001),
        ) as session:
            await session.run("cd /tmp; export MESSAGE=hello")
            first = await session.spawn("echo $MESSAGE", mode="parallel")
            second = await session.spawn("pwd", mode="parallel")
            print((await first.wait()).exit_code, (await second.wait()).exit_code)
            print((await first.read("stdout")).content)

asyncio.run(main())
```

`stateful` is the default command mode: it executes in the live Bash shell,
retaining `cd`, exports, and shell variables. These commands run in order.
`parallel` commands run concurrently in child shells with the current directory
and exported environment. The caller manages permissions on any shared folder.
The worker clears its environment at session creation, so supply `PATH` for
external programs.

`session.attach_terminal()` opens a detachable binary WebSocket terminal;
closing it leaves the session and shell running. Install `shiitake-py[pty]`
for terminal support. `session.run()` collects a bounded amount of output;
use a handle's `read("stdout", from_=offset, max_=size)` for large streams.
For a short command, use `await session.run("echo hello")`. For a command
whose wall time is unknown, retain its handle and decide when to cancel:

```python
job = await session.spawn("echo started; sleep 3600; echo finished", mode="parallel")
print([item.handle for item in await session.commands()])  # rediscover handles
print((await job.poll()).status)        # running
print((await job.read("stdout")).content)  # output so far
await job.kill()                         # cancel this command
print((await job.wait()).exit_cause)     # cancelled
```

Commands have no wall-time deadline. `AsyncHandle.wait()` polls until a
terminal status. Use `session.get_command(handle_id)` to recover a handle from
a listing and then poll, read, or cancel it. The synchronous `ShiitakeClient` has the same `Session.run()`,
`Session.spawn()`, and `Handle.poll()`, `read()`, `kill()`, and `wait()` methods.
Release the session after the command ends, or use a context manager.
