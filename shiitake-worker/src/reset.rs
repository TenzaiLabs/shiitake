//! Between-command sandbox reset.
//!
//! The worker is pid 1 of its own container's PID namespace and runs every
//! command as a child process. Resetting in-process lets one worker handle many
//! commands without a container restart — avoiding the kubelet CrashLoopBackOff
//! that per-command container exits incur — while still giving each command a
//! clean slate equivalent to a fresh container:
//!
//! 1. every process left over from the previous command is SIGKILLed and reaped,
//! 2. the configured writable scratch paths are emptied, and
//! 3. SysV IPC objects are removed and the removal is verified.
//!
//! Shiitake is generic: it never assumes any particular path layout. The caller
//! supplies the list of writable paths to clear, so an embedding layer decides
//! what is per-command scratch (cleared) versus persistent state it wants kept.

use anyhow::{Context, Error, Result, bail};
use itertools::process_results;
use nix::{
    errno::Errno,
    sys::{
        signal::{Signal, kill},
        wait::{WaitPidFlag, WaitStatus, waitpid},
    },
    unistd::{Pid, getpid},
};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tracing::{error, info, warn};

/// How long to keep reaping after the SIGKILL sweep before giving up and
/// logging the stragglers. Processes die promptly under SIGKILL; this only
/// bounds pathological cases (e.g. a task stuck in uninterruptible sleep).
const REAP_DEADLINE: Duration = Duration::from_secs(5);

/// Perform a full between-command reset, returning `Err` if the sandbox can no
/// longer be trusted as clean. Every applicable step still runs (so the logs
/// surface all the damage at once), but if any of them — the process sweep, a
/// scratch clear, or SysV IPC removal — fails, the error is propagated: a worker
/// is only safe to reuse once it has been reset successfully, so the caller must
/// recycle it rather than serve another command on a dirty sandbox.
pub fn reset(clear_paths: &[PathBuf]) -> Result<()> {
    let mut first_err: Option<Error> = None;
    // The process sweep and SysV IPC removal are namespace-wide operations that
    // are only valid — and only safe — as pid 1, the container's PID/IPC
    // namespace init (which the worker always is in production). Anywhere else —
    // a dev box, a unit test, a pod with no isolated namespace — sweeping
    // "everything but me" or clearing all IPC could destroy unrelated host
    // state, so we skip both rather than treat the refusal as a reset failure.
    // Once we *are* pid 1, a sweep or IPC removal that can't finish is fatal.
    let self_pid = getpid();
    if self_pid == Pid::from_raw(1) {
        match kill_all_except_self(Path::new("/proc"), self_pid) {
            Ok(killed) => info!(killed, "process sweep complete"),
            Err(e) => {
                warn!("process sweep failed: {e:#}");
                first_err.get_or_insert(e.context("process sweep"));
            }
        }
        if let Err(e) = remove_sysv_ipc() {
            warn!("SysV IPC removal failed: {e:#}");
            first_err.get_or_insert(e.context("SysV IPC removal"));
        }
    } else {
        // In production the worker is its container's pid 1, so this should
        // never happen — flag it loudly. (It is expected only off-cluster: dev
        // runs, tests, or a pod with a shared PID namespace.)
        error!(
            self_pid = self_pid.as_raw(),
            "not pid 1; skipping process sweep and IPC removal"
        );
    }
    // Scratch clearing is path-scoped (only the configured dirs), so it is safe
    // to run anywhere and always does.
    for path in clear_paths {
        if let Err(e) = clear_dir_contents(path) {
            warn!(path = %path.display(), "scratch clear failed: {e:#}");
            first_err.get_or_insert(e.context(format!("clear {}", path.display())));
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// SIGKILL every process in our PID namespace except ourselves, then reap them.
///
/// Refuses unless we are pid 1: only a PID-namespace init may safely sweep
/// "everything but me". Running this anywhere else (dev box, unit test, a
/// misconfigured pod without an isolated PID namespace) could kill unrelated
/// host processes, so we hard-refuse instead. Returns the number of pids
/// signalled.
fn kill_all_except_self(proc_root: &Path, self_pid: Pid) -> Result<usize> {
    if self_pid != Pid::from_raw(1) {
        bail!(
            "worker is pid {}, not pid 1 — not a PID-namespace init; refusing to sweep",
            self_pid.as_raw()
        );
    }

    // We are pid 1, so every process we kill (and every orphan it leaves behind)
    // reparents to us. Reap, list, SIGKILL the survivors, repeat — until the
    // namespace holds only us or the deadline passes. The first sweep's count is
    // what we report as "signalled".
    let deadline = Instant::now() + REAP_DEADLINE;
    let mut signalled: Option<usize> = None;
    loop {
        reap_zombies();
        let remaining = sibling_pids(proc_root, self_pid)?;
        let count = *signalled.get_or_insert(remaining.len());
        if remaining.is_empty() {
            return Ok(count);
        }
        if Instant::now() >= deadline {
            // A process that outlives SIGKILL + the reap deadline (e.g. stuck in
            // uninterruptible sleep) leaves the sandbox dirty — fail so the
            // worker is recycled into a fresh container. Log each survivor on its
            // own line with its state (a `D` is the usual culprit: unkillable
            // until its I/O completes), then fail with the count.
            for pid in &remaining {
                let (comm, state) = proc_comm_state(proc_root, *pid);
                warn!(pid = pid.as_raw(), comm = %comm, state = %state,
                      "process still alive after reap deadline");
            }
            bail!(
                "{} process(es) still alive after reap deadline",
                remaining.len()
            );
        }
        for pid in &remaining {
            // ESRCH (already gone) is fine; anything else just retries next loop.
            let _ = kill(*pid, Signal::SIGKILL);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Numeric pids under `proc_root` other than `self_pid`. `/proc` lists one
/// directory per process (thread groups), so this enumerates every process in
/// the PID namespace; threads of other processes are not top-level entries and
/// die with their group leader.
fn sibling_pids(proc_root: &Path, self_pid: Pid) -> Result<Vec<Pid>> {
    let entries = std::fs::read_dir(proc_root)
        .context("read /proc")?
        .map(|entry| entry.context("read /proc entry"));
    // Stream the entries, short-circuiting on the first read error, and keep the
    // numeric pids that aren't us.
    process_results(entries, |entries| {
        entries
            .filter_map(|e| e.file_name().to_str().and_then(|n| n.parse().ok()))
            .map(Pid::from_raw)
            .filter(|&pid| pid != self_pid)
            .collect()
    })
}

/// Best-effort `(comm, state)` for a pid from `/proc/<pid>/status`; either field
/// renders as `?` if the pid is gone or the file is unreadable. `state` is the
/// single-letter code (e.g. `D` for uninterruptible sleep).
fn proc_comm_state(proc_root: &Path, pid: Pid) -> (String, String) {
    let (mut comm, mut state) = ("?".to_string(), "?".to_string());
    if let Ok(status) =
        std::fs::read_to_string(proc_root.join(pid.as_raw().to_string()).join("status"))
    {
        for line in status.lines() {
            if let Some(v) = line.strip_prefix("Name:") {
                comm = v.trim().to_string();
            } else if let Some(v) = line.strip_prefix("State:") {
                // "State:\tD (disk sleep)" → "D"
                state = v.split_whitespace().next().unwrap_or("?").to_string();
            }
        }
    }
    (comm, state)
}

/// Non-blocking reap of any zombies that have reparented to us.
fn reap_zombies() {
    loop {
        match waitpid(Pid::from_raw(-1), Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) => break,
            Ok(_) => continue,
            // ECHILD: no children left. Anything else: stop reaping this round.
            Err(Errno::ECHILD) | Err(_) => break,
        }
    }
}

/// Empty a directory's contents while keeping the directory itself (it is
/// usually a mountpoint). Symlinks are unlinked, never followed, so a symlink
/// planted by a command can't redirect deletion outside the scratch dir. A
/// missing directory is a no-op.
fn clear_dir_contents(dir: &Path) -> Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("read_dir {}", dir.display())),
    };
    let mut first_err: Option<Error> = None;
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                first_err.get_or_insert_with(|| e.into());
                continue;
            }
        };
        let path = entry.path();
        // DirEntry::file_type does not follow symlinks, so a symlink reports as
        // a symlink (not a dir) and is unlinked below rather than traversed.
        let res = match entry.file_type() {
            Ok(ft) if ft.is_dir() => std::fs::remove_dir_all(&path),
            _ => std::fs::remove_file(&path),
        };
        if let Err(e) = res {
            first_err
                .get_or_insert_with(|| Error::new(e).context(format!("remove {}", path.display())));
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Remove every SysV IPC object (shared memory, semaphores, message queues) in
/// the worker's IPC namespace, then verify the tables are empty. The namespace
/// persists across commands — a fresh container would discard it — so a leaked
/// object would be visible to the next command. By the time this runs every
/// command process has been swept and the worker is pid 1 and privileged (it
/// must be, to `drop_to`), so `IPC_RMID` succeeds regardless of owner and the
/// tables are static; anything still listed afterwards is a removal that did not
/// take, which we propagate so `reset` recycles the worker rather than reuse a
/// dirty namespace.
#[cfg(target_os = "linux")]
fn remove_sysv_ipc() -> Result<()> {
    // The id is column 1 (0-based) in every /proc/sysvipc table, after the key.
    for id in ipc_ids("/proc/sysvipc/shm") {
        unsafe { libc::shmctl(id, libc::IPC_RMID, std::ptr::null_mut()) };
    }
    for id in ipc_ids("/proc/sysvipc/sem") {
        // semctl is variadic; IPC_RMID ignores semnum and the union arg.
        unsafe { libc::semctl(id, 0, libc::IPC_RMID) };
    }
    for id in ipc_ids("/proc/sysvipc/msg") {
        unsafe { libc::msgctl(id, libc::IPC_RMID, std::ptr::null_mut()) };
    }
    // Re-read to confirm removal: with every command process swept the tables
    // don't change under us, so a non-empty table means an RMID that didn't take.
    let residual: Vec<String> = [
        ("shm", "/proc/sysvipc/shm"),
        ("sem", "/proc/sysvipc/sem"),
        ("msg", "/proc/sysvipc/msg"),
    ]
    .into_iter()
    .filter_map(|(kind, path)| {
        let n = ipc_ids(path).len();
        (n > 0).then(|| format!("{n} {kind}"))
    })
    .collect();
    if residual.is_empty() {
        Ok(())
    } else {
        bail!("SysV IPC objects survived removal: {}", residual.join(", "));
    }
}

#[cfg(not(target_os = "linux"))]
fn remove_sysv_ipc() -> Result<()> {
    Ok(())
}

// Used by remove_sysv_ipc on Linux and by the parse test everywhere; on a
// non-Linux build (dev macOS) neither consumer is compiled.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn ipc_ids(path: &str) -> Vec<i32> {
    match std::fs::read_to_string(path) {
        Ok(content) => parse_ipc_ids(&content),
        Err(_) => Vec::new(),
    }
}

/// Parse the id column out of a `/proc/sysvipc/*` table. The first line is a
/// header; every data row is `key <id> perms …`, so the id is the second field.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_ipc_ids(content: &str) -> Vec<i32> {
    content
        .lines()
        .skip(1)
        .filter_map(|line| line.split_whitespace().nth(1)?.parse::<i32>().ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn sweep_refuses_when_not_pid_one() {
        // The critical safety property: never sweep unless we are the PID-ns
        // init, so a dev/test/misconfigured context can't kill host processes.
        let err = kill_all_except_self(Path::new("/proc"), Pid::from_raw(4242)).unwrap_err();
        assert!(err.to_string().contains("not pid 1"), "got: {err}");
    }

    #[test]
    fn sibling_pids_excludes_self_and_non_numeric() {
        let tmp = tempfile::TempDir::new().unwrap();
        for name in ["1", "7", "42", "self", "cpuinfo", "1234"] {
            fs::create_dir(tmp.path().join(name)).unwrap();
        }
        let mut pids = sibling_pids(tmp.path(), Pid::from_raw(7)).unwrap();
        pids.sort_unstable_by_key(|p| p.as_raw());
        // numeric, minus self (7)
        assert_eq!(
            pids,
            vec![Pid::from_raw(1), Pid::from_raw(42), Pid::from_raw(1234)]
        );
    }

    #[test]
    fn proc_comm_state_reads_name_and_state_else_placeholder() {
        let tmp = tempfile::TempDir::new().unwrap();
        let p = tmp.path().join("4242");
        fs::create_dir(&p).unwrap();
        fs::write(
            p.join("status"),
            "Name:\tstuck-proc\nState:\tD (disk sleep)\nPid:\t4242\n",
        )
        .unwrap();

        assert_eq!(
            proc_comm_state(tmp.path(), Pid::from_raw(4242)),
            ("stuck-proc".to_string(), "D".to_string())
        );
        // No status file for pid 9 → both fields fall back to "?".
        assert_eq!(
            proc_comm_state(tmp.path(), Pid::from_raw(9)),
            ("?".to_string(), "?".to_string())
        );
    }

    #[test]
    fn clear_dir_contents_empties_but_keeps_dir() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().join("scratch");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("a.txt"), b"x").unwrap();
        fs::create_dir(root.join("sub")).unwrap();
        fs::write(root.join("sub/b.txt"), b"y").unwrap();

        clear_dir_contents(&root).unwrap();

        assert!(root.is_dir(), "scratch dir must survive");
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0, "contents removed");
    }

    #[test]
    fn clear_dir_contents_unlinks_symlink_without_following_it() {
        let tmp = tempfile::TempDir::new().unwrap();
        let outside = tmp.path().join("precious");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("keep.txt"), b"keep").unwrap();

        let scratch = tmp.path().join("scratch");
        fs::create_dir(&scratch).unwrap();
        std::os::unix::fs::symlink(&outside, scratch.join("link")).unwrap();

        clear_dir_contents(&scratch).unwrap();

        assert_eq!(
            fs::read_dir(&scratch).unwrap().count(),
            0,
            "symlink removed"
        );
        assert!(
            outside.join("keep.txt").exists(),
            "symlink target untouched"
        );
    }

    #[test]
    fn clear_dir_contents_missing_dir_is_noop() {
        let tmp = tempfile::TempDir::new().unwrap();
        clear_dir_contents(&tmp.path().join("nope")).unwrap();
    }

    #[test]
    fn parse_ipc_ids_skips_header_takes_id_column() {
        // Real /proc/sysvipc/shm shape: header, then `key shmid perms …`.
        let shm = "\
       key      shmid perms       size  cpid  lpid nattch   uid   gid  cuid  cgid
         0          3   600    1048576  1234  1235      1  1000  1000  1000  1000
         0         98   600       4096  2000  2001      0  1000  1000  1000  1000
";
        assert_eq!(parse_ipc_ids(shm), vec![3, 98]);
        assert_eq!(parse_ipc_ids(""), Vec::<i32>::new());
        assert_eq!(parse_ipc_ids("only a header line\n"), Vec::<i32>::new());
    }
}
