//! Interactive PTY sessions: `openpty` + a shell whose controlling terminal is
//! the pty. The master fd is pumped to/from the server as WebSocket binary —
//! unlike `exec`, output is never captured to disk (the client owns the
//! scrollback), and the worker is pinned to this one session for its lifetime.
//!
//! Identity and cwd match `exec`: the shell runs under the caller's `drop_to`
//! and in `working_dir`, so a terminal is just a persistent, interactive
//! command with the same isolation.

use crate::exec::{apply_drop_to, io_err};
use anyhow::{Context, Result};
use nix::pty::openpty;
use nix::unistd::setsid;
use shiitake_worker_api::PtyOpenFrame;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::process::{ExitStatus, Stdio};
use std::sync::OnceLock;
use tokio::io::unix::AsyncFd;
use tokio::process::{Child, Command};

/// The shell spawned when a `PtyOpen` carries no explicit command. Overridable
/// via `SHIITAKE_PTY_SHELL` (whitespace-split argv) so a deployment can make the
/// default terminal e.g. a transparent `tmux` without any protocol change; falls
/// back to an interactive bash. Read once per process.
fn default_shell() -> &'static [String] {
    static SHELL: OnceLock<Vec<String>> = OnceLock::new();
    SHELL.get_or_init(|| match std::env::var("SHIITAKE_PTY_SHELL") {
        Ok(s) if !s.trim().is_empty() => s.split_whitespace().map(String::from).collect(),
        _ => vec!["bash".to_string(), "-i".to_string()],
    })
}

/// A running interactive shell attached to a pty. The parent keeps the master
/// side; the child's stdio is the slave.
pub struct PtySession {
    child: Child,
    master: AsyncFd<OwnedFd>,
}

impl PtySession {
    /// Open a pty, spawn the shell on its slave as a session leader with the
    /// slave as controlling terminal, and register the master for async I/O.
    pub fn spawn(open: &PtyOpenFrame) -> Result<Self> {
        let pty = openpty(None, None).context("openpty")?;
        let (master, slave) = (pty.master, pty.slave);

        let argv: &[String] = if open.command.is_empty() {
            default_shell()
        } else {
            &open.command
        };
        let mut cmd = Command::new(&argv[0]);
        cmd.args(&argv[1..])
            .current_dir(&open.working_dir)
            .env_clear()
            .envs(&open.env)
            .env("TERM", "xterm-256color")
            .stdin(Stdio::from(slave.try_clone().context("dup slave stdin")?))
            .stdout(Stdio::from(slave.try_clone().context("dup slave stdout")?))
            .stderr(Stdio::from(slave.try_clone().context("dup slave stderr")?))
            .kill_on_drop(true);

        let drop_to = open.drop_to.clone();
        // Name the target uid (if the caller asked) while still privileged, in
        // the parent — before the async-signal-safe `pre_exec` drop below. When a
        // home was created, point HOME at it so `~`/`$HOME` and the passwd entry
        // agree, whatever `SHIITAKE_HOME_ROOT` is (overrides any caller HOME).
        if let Some(d) = drop_to.as_ref()
            && let Some(home) = crate::account::ensure(d).context("ensure named account")?
        {
            cmd.env("HOME", home);
        }
        // SAFETY: only async-signal-safe syscalls run before exec — setsid, a
        // TIOCSCTTY ioctl, and the same setgid/setgroups/setuid `exec` uses.
        unsafe {
            cmd.pre_exec(move || {
                setsid().map_err(io_err)?;
                // fd 0 is the pty slave (stdin, above); make it the controlling
                // terminal so job control and terminal signals work.
                if libc::ioctl(0, libc::TIOCSCTTY as _, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
                if let Some(d) = drop_to.as_ref() {
                    apply_drop_to(d)?;
                }
                Ok(())
            });
        }

        let child = cmd.spawn().context("spawn shell on pty")?;
        drop(slave); // the child holds its own dups of the slave

        set_nonblocking(master.as_raw_fd()).context("set master non-blocking")?;
        let master = AsyncFd::new(master).context("register master with tokio")?;
        let session = Self { child, master };
        // Best-effort initial size; the client resends on connect anyway.
        let _ = session.resize(open.cols, open.rows);
        Ok(session)
    }

    /// Read available pty output into `out`. `Ok(0)` means the shell closed the
    /// pty (EOF).
    pub async fn read_output(&self, out: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.master.readable().await?;
            match guard.try_io(|inner| {
                let fd = inner.get_ref().as_raw_fd();
                let n = unsafe { libc::read(fd, out.as_mut_ptr().cast(), out.len()) };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            }) {
                Ok(res) => return res,
                Err(_would_block) => continue,
            }
        }
    }

    /// Write client keystrokes to the pty master, fully.
    pub async fn write_input(&self, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            let mut guard = self.master.writable().await?;
            match guard.try_io(|inner| {
                let fd = inner.get_ref().as_raw_fd();
                let n = unsafe { libc::write(fd, data.as_ptr().cast(), data.len()) };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            }) {
                Ok(Ok(n)) => data = &data[n..],
                Ok(Err(e)) => return Err(e),
                Err(_would_block) => continue,
            }
        }
        Ok(())
    }

    /// Apply a window resize (`TIOCSWINSZ`) so full-screen programs reflow.
    pub fn resize(&self, cols: u16, rows: u16) -> io::Result<()> {
        let ws = libc::winsize {
            ws_row: rows,
            ws_col: cols,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let fd = self.master.get_ref().as_raw_fd();
        if unsafe { libc::ioctl(fd, libc::TIOCSWINSZ as _, &ws) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Wait for the shell to exit.
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        self.child.wait().await
    }

    /// End the session: SIGHUP the shell's process group (it is a session
    /// leader, so the pgid is its pid), then hard-kill if it lingers.
    pub async fn shutdown(&mut self) {
        if let Some(pid) = self.child.id() {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGHUP);
            }
        }
        let grace = std::time::Duration::from_secs(2);
        if tokio::time::timeout(grace, self.child.wait())
            .await
            .is_err()
        {
            let _ = self.child.start_kill();
        }
    }
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
