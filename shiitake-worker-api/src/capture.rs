//! On-disk capture of per-command stdout/stderr.
//!
//! Each stream is a single file the worker redirects the child's fd into —
//! the kernel writes output straight to disk, so neither the worker nor the
//! server ever holds command output in memory. The server reads those files
//! back over HTTP (with range support) and `stat`s them for byte counts.
//!
//! Layout:
//!
//! ```text
//! <root>/<request_id>/stdout
//! <root>/<request_id>/stderr
//! ```
//!
//! Both the server and the workers mount the capture root at the same path.
//! Storage is unbounded: output grows with the command and is bounded only by
//! the volume's capacity. Consumers read a byte range (tail/window) rather
//! than the whole file, and the server reports output sizes as metrics, so a
//! runaway command is observable after the fact rather than truncated.

use crate::ExecId;
use std::path::{Path, PathBuf};

/// The two streams a handle captures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    pub fn file_name(self) -> &'static str {
        match self {
            Stream::Stdout => "stdout",
            Stream::Stderr => "stderr",
        }
    }
}

/// Directory holding one handle's capture files (`<root>/<request_id>`).
pub fn handle_dir(root: &Path, request_id: &ExecId) -> PathBuf {
    root.join(sanitize_id(request_id.as_str()))
}

/// Path of one stream's capture file (`<root>/<request_id>/<stream>`).
pub fn stream_path(root: &Path, request_id: &ExecId, stream: Stream) -> PathBuf {
    handle_dir(root, request_id).join(stream.file_name())
}

/// Strip a request_id down to a safe single path component.
pub fn sanitize_id(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect()
}

/// Byte length of a stream's capture file. Returns 0 when the file does not
/// exist yet (the command produced no output on that stream, or hasn't run).
pub async fn stream_len(root: &Path, request_id: &ExecId, stream: Stream) -> u64 {
    match tokio::fs::metadata(stream_path(root, request_id, stream)).await {
        Ok(m) => m.len(),
        Err(_) => 0,
    }
}

/// Free space on the filesystem backing the capture root, in bytes. `None`
/// if the path can't be `statvfs`'d. Sampled into a gauge so the unbounded
/// storage decision stays observable (alert on this, don't cap writes).
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn free_bytes(root: &std::path::Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(root.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated path; `stat` is owned here.
    if unsafe { libc::statvfs(c.as_ptr(), &mut stat) } != 0 {
        return None;
    }
    Some(available_bytes(&stat))
}

/// Other targets have no `statvfs` binding here, so free space is unknown.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn free_bytes(_root: &std::path::Path) -> Option<u64> {
    None
}

// `available blocks × fragment size`, widened to u64. The statvfs field widths
// differ by OS, so the product is typed per-platform rather than cast.
#[cfg(target_os = "linux")]
fn available_bytes(stat: &libc::statvfs) -> u64 {
    // Both fields are already u64 on Linux.
    stat.f_bavail * stat.f_frsize
}

#[cfg(target_os = "macos")]
fn available_bytes(stat: &libc::statvfs) -> u64 {
    // f_bavail is u32 on macOS; f_frsize is u64.
    u64::from(stat.f_bavail) * stat.f_frsize
}

/// Remove a handle's entire capture directory. Idempotent.
pub async fn purge(root: &Path, request_id: &ExecId) -> std::io::Result<()> {
    match tokio::fs::remove_dir_all(handle_dir(root, request_id)).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Remove every handle directory under `root`, returning `(removed, failures)`.
///
/// The server calls this at startup: its registry is in memory, so anything
/// already on the volume belongs to a dead process and can never be served
/// again. Only directories are touched, and one failure doesn't stop the sweep,
/// so a stray `lost+found` can't block boot. Assumes one server per root.
pub async fn purge_orphans(
    root: &Path,
) -> std::io::Result<(usize, Vec<(PathBuf, std::io::Error)>)> {
    let mut dir = match tokio::fs::read_dir(root).await {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, Vec::new())),
        Err(e) => return Err(e),
    };
    let (mut removed, mut failed) = (0, Vec::new());
    while let Some(entry) = dir.next_entry().await? {
        match entry.file_type().await {
            Ok(t) if t.is_dir() => match tokio::fs::remove_dir_all(entry.path()).await {
                Ok(()) => removed += 1,
                Err(e) => failed.push((entry.path(), e)),
            },
            Ok(_) => {}
            Err(e) => failed.push((entry.path(), e)),
        }
    }
    Ok((removed, failed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_id_drops_path_chars() {
        assert_eq!(sanitize_id("abc-123_def"), "abc-123_def");
        assert_eq!(sanitize_id("../etc/passwd"), "etcpasswd");
        assert_eq!(sanitize_id("a/b/c"), "abc");
    }

    #[test]
    fn paths_are_under_root() {
        let root = Path::new("/capture");
        let id = ExecId::new("abc-123");
        assert_eq!(
            stream_path(root, &id, Stream::Stdout),
            Path::new("/capture/abc-123/stdout")
        );
        assert_eq!(
            stream_path(root, &id, Stream::Stderr),
            Path::new("/capture/abc-123/stderr")
        );
    }

    fn handle_with_output(root: &Path, id: &str) {
        let dir = handle_dir(root, &ExecId::new(id));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("stdout"), b"out").unwrap();
        std::fs::write(dir.join("stderr"), b"err").unwrap();
    }

    #[tokio::test]
    async fn purge_orphans_clears_handle_dirs_and_is_idempotent() {
        let root = tempfile::tempdir().unwrap();
        for id in ["a", "b", "c"] {
            handle_with_output(root.path(), id);
        }

        let (removed, failed) = purge_orphans(root.path()).await.unwrap();
        assert_eq!(removed, 3);
        assert!(failed.is_empty(), "{failed:?}");
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);

        // Nothing left to do on a second pass — the server may restart twice.
        let (removed, _) = purge_orphans(root.path()).await.unwrap();
        assert_eq!(removed, 0);
    }

    // Only handle directories are ours to remove. A file sitting in the capture
    // root (a mount marker, an operator's note) is left alone.
    #[tokio::test]
    async fn purge_orphans_leaves_stray_files() {
        let root = tempfile::tempdir().unwrap();
        handle_with_output(root.path(), "a");
        std::fs::write(root.path().join("README"), b"not a handle").unwrap();

        let (removed, failed) = purge_orphans(root.path()).await.unwrap();
        assert_eq!(removed, 1);
        assert!(failed.is_empty(), "{failed:?}");
        assert!(root.path().join("README").exists());
    }

    // A capture root that doesn't exist yet is not an error: the server creates
    // it moments later, and a missing root has nothing to reconcile.
    #[tokio::test]
    async fn purge_orphans_tolerates_a_missing_root() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("not-created-yet");
        let (removed, failed) = purge_orphans(&missing).await.unwrap();
        assert_eq!(removed, 0);
        assert!(failed.is_empty());
    }
}
