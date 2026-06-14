//! Spawn `bash -c <command>` in its own process group, redirecting the child's
//! stdout/stderr straight into per-stream capture files. The worker never
//! copies output bytes — the kernel writes the child's fds to disk. Supports
//! cancellation: the caller can flip a `watch::Receiver<bool>` to `true` to
//! SIGKILL the process group mid-command.
//!
//! Memory/CPU bounds are not enforced here: the worker container's k8s
//! resource limits bound it, and OOM is detected externally from the kubelet's
//! container status.

use crate::cgroup::{CpuTimes, read_cpu_times, read_memory_limit, read_memory_peak};
use anyhow::{Context, Result};
use nix::{
    sys::signal::{Signal, killpg},
    unistd::{Pid, setsid},
};
use shiitake_worker_api::{
    DropTo, ExecuteFrame, ResourceUsage, ResultFrame,
    capture::{Stream, handle_dir, stream_path},
};
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    process::{Child, Command},
    sync::watch,
    time::sleep,
};

pub struct Outcome {
    pub result: ResultFrame,
}

pub async fn run(
    execute: &ExecuteFrame,
    cancel: watch::Receiver<bool>,
    capture_root: &Path,
) -> Result<Outcome> {
    let cpu_before = read_cpu_times().await;

    let env = execute.env.clone();

    // Open the capture files as the worker uid, under the inherited umask, and
    // hand their fds to the child as stdout/stderr. The child writes through
    // inherited fds, so it never needs — or learns — the capture path.
    std::fs::create_dir_all(handle_dir(capture_root, &execute.request_id))
        .context("create capture dir")?;
    let stderr_path = stream_path(capture_root, &execute.request_id, Stream::Stderr);
    let stdout_file = std::fs::File::create(stream_path(
        capture_root,
        &execute.request_id,
        Stream::Stdout,
    ))
    .context("create stdout capture file")?;
    let stderr_file = std::fs::File::create(&stderr_path).context("create stderr capture file")?;

    let mut cmd = Command::new("bash");
    cmd.arg("-c")
        .arg(&execute.command)
        .current_dir(&execute.working_dir)
        .env_clear()
        .envs(&env)
        .stdin(Stdio::null())
        .stdout(stdout_file)
        .stderr(stderr_file)
        .kill_on_drop(true);

    let drop_to = execute.drop_to.clone();
    unsafe {
        cmd.pre_exec(move || {
            // Own process group so a timeout/cancel can killpg the whole tree.
            setsid().map_err(io_err)?;
            if let Some(d) = drop_to.as_ref() {
                apply_drop_to(d)?;
            }
            Ok(())
        });
    }

    let mut child = cmd.spawn().context("spawn bash -c")?;
    let raw_pid = child.id().context("child has no pid")?;
    let pid = Pid::from_raw(i32::try_from(raw_pid).context("pid out of range for pid_t")?);

    let dur = Duration::from_secs_f64(execute.timeout_secs.max(0.001));
    let (cancelled, timed_out, exit_status) = wait_with_signals(&mut child, pid, dur, cancel).await;

    if timed_out {
        append_marker(
            &stderr_path,
            &format!("\nCommand timed out after {}s\n", execute.timeout_secs),
        );
    }
    if cancelled {
        append_marker(&stderr_path, "\nCommand cancelled by server\n");
    }

    let (exit_code, exit_signal) = match exit_status {
        Some(status) => {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                (status.code(), status.signal())
            }
            #[cfg(not(unix))]
            {
                (status.code(), None)
            }
        }
        None => (None, Some(libc::SIGKILL)),
    };

    Ok(Outcome {
        result: ResultFrame {
            request_id: execute.request_id.clone(),
            exit_code,
            exit_signal,
            timed_out,
            cancelled,
            usage: usage(cpu_before).await,
        },
    })
}

/// Assemble the reported resource usage from cgroup readings taken after the
/// command finished, against the pre-exec CPU baseline.
async fn usage(cpu_before: Option<CpuTimes>) -> ResourceUsage {
    let (cpu_user_seconds, cpu_system_seconds) = match (cpu_before, read_cpu_times().await) {
        (Some(before), Some(after)) => (
            Some(
                Duration::from_micros(after.user_usec.saturating_sub(before.user_usec))
                    .as_secs_f64(),
            ),
            Some(
                Duration::from_micros(after.system_usec.saturating_sub(before.system_usec))
                    .as_secs_f64(),
            ),
        ),
        _ => (None, None),
    };
    ResourceUsage {
        memory_peak_bytes: read_memory_peak().await,
        memory_limit_bytes: read_memory_limit().await,
        cpu_user_seconds,
        cpu_system_seconds,
    }
}

async fn wait_with_signals(
    child: &mut Child,
    pid: Pid,
    timeout: Duration,
    mut cancel: watch::Receiver<bool>,
) -> (bool, bool, Option<std::process::ExitStatus>) {
    tokio::select! {
        biased;
        _ = cancel.changed() => {
            if *cancel.borrow() {
                let _ = killpg(pid, Signal::SIGKILL);
                let status = child.wait().await.ok();
                return (true, false, status);
            }
            // false transition — fall through to wait again. In practice the
            // channel only flips to true once.
            let status = child.wait().await.ok();
            (false, false, status)
        }
        _ = sleep(timeout) => {
            let _ = killpg(pid, Signal::SIGKILL);
            let status = child.wait().await.ok();
            (false, true, status)
        }
        res = child.wait() => {
            (false, false, res.ok())
        }
    }
}

/// Append a server-generated notice to the stderr capture file (timeout /
/// cancel). Best-effort: the child has already exited and released its fd.
fn append_marker(path: &PathBuf, marker: &str) {
    if let Ok(mut f) = std::fs::OpenOptions::new().append(true).open(path) {
        let _ = f.write_all(marker.as_bytes());
    }
}

/// Apply a privilege-drop directive in the post-fork child. Safe to call
/// from `pre_exec`: only async-signal-safe syscalls (setgid, setgroups,
/// setuid, umask). Order is `setgid → setgroups → setuid` so all gid
/// changes happen while the process is still uid 0.
#[cfg(target_os = "linux")]
fn apply_drop_to(d: &DropTo) -> std::io::Result<()> {
    use nix::unistd::{Gid, Uid};

    let gid = Gid::from_raw(d.gid);
    let uid = Uid::from_raw(d.uid);
    nix::unistd::setgid(gid).map_err(io_err)?;
    let supp: Vec<Gid> = d
        .supplementary_gids
        .iter()
        .map(|g| Gid::from_raw(*g))
        .collect();
    nix::unistd::setgroups(&supp).map_err(io_err)?;
    nix::unistd::setuid(uid).map_err(io_err)?;
    if let Some(mask) = d.umask {
        // SAFETY: umask is async-signal-safe. `mask` is the u32 the DropTo
        // carries; mode_t is u32 on Linux, so no conversion is needed.
        unsafe {
            libc::umask(mask);
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn apply_drop_to(_d: &DropTo) -> std::io::Result<()> {
    Ok(())
}

fn io_err(e: nix::errno::Errno) -> std::io::Error {
    e.into()
}

#[cfg(test)]
mod tests {
    //! Stress test for the direct-fd capture path: run many large-output
    //! commands concurrently and assert every byte lands in the capture file.
    //! Because the worker redirects the child's stdout fd straight to disk and
    //! never buffers output, this is also the flat-memory guarantee under load.

    use super::run;
    use shiitake_worker_api::{
        ExecId, ExecuteFrame,
        capture::{Stream, stream_len, stream_path},
    };
    use std::collections::BTreeMap;
    use tempfile::TempDir;
    use tokio::sync::watch;

    const STREAMS: usize = 16;
    const BYTES_PER_STREAM: u64 = 8 * 1024 * 1024; // 8 MiB

    fn exec_frame(request_id: &ExecId, command: String) -> ExecuteFrame {
        let mut env = BTreeMap::new();
        env.insert(
            "PATH".to_string(),
            std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into()),
        );
        ExecuteFrame {
            request_id: request_id.clone(),
            command,
            working_dir: std::env::temp_dir().to_string_lossy().into_owned(),
            env,
            timeout_secs: 120.0,
            drop_to: None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn many_concurrent_large_outputs_lose_no_bytes() {
        let root = TempDir::new().unwrap();
        let root_path = root.path().to_path_buf();

        // Run all commands concurrently (their bash subprocesses run in
        // parallel as OS processes); join the futures rather than spawning.
        let futs = (0..STREAMS).map(|i| {
            let root_path = root_path.clone();
            async move {
                let request_id = ExecId::new(format!("stress-{i}"));
                let frame = exec_frame(
                    &request_id,
                    format!("head -c {BYTES_PER_STREAM} /dev/zero | tr '\\0' 'x'; echo err 1>&2"),
                );
                let (_tx, rx) = watch::channel(false);
                let outcome = run(&frame, rx, &root_path).await.expect("exec run");
                assert_eq!(outcome.result.exit_code, Some(0), "stream {i} exit code");
                assert!(!outcome.result.timed_out);

                let stdout_len = stream_len(&root_path, &request_id, Stream::Stdout).await;
                assert_eq!(
                    stdout_len, BYTES_PER_STREAM,
                    "stream {i} captured stdout length"
                );

                let body = tokio::fs::read(stream_path(&root_path, &request_id, Stream::Stdout))
                    .await
                    .unwrap();
                assert!(
                    body.iter().all(|&b| b == b'x'),
                    "stream {i} stdout corrupted"
                );

                let stderr_len = stream_len(&root_path, &request_id, Stream::Stderr).await;
                assert_eq!(stderr_len, 4, "stream {i} captured stderr length"); // "err\n"
            }
        });

        futures_util::future::join_all(futs).await;
    }
}
