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
pub fn free_bytes(root: &std::path::Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(root.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated path; `stat` is owned here.
    if unsafe { libc::statvfs(c.as_ptr(), &mut stat) } != 0 {
        return None;
    }
    Some(stat.f_bavail as u64 * stat.f_frsize as u64)
}

/// Remove a handle's entire capture directory. Idempotent.
pub async fn purge(root: &Path, request_id: &ExecId) -> std::io::Result<()> {
    match tokio::fs::remove_dir_all(handle_dir(root, request_id)).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
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
}
