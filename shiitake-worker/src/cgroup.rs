//! Per-command resource readings from cgroup v2, for the metrics the worker
//! reports on the `Result` frame. OOM *detection* is deliberately not here —
//! the command shares the container cgroup, so a container OOM can kill the
//! worker itself; the kubelet's container `OOMKilled` status is the only
//! reliable signal and the server reads it externally.
//!
//! Each worker container runs one command and exits, so these readings (taken
//! after the command finishes, while the worker is still alive) are that
//! command's usage. Every reader returns `None` when the file isn't present
//! (non-cgroup-v2 host, or dev macOS).

use std::path::Path;

const MEMORY_PEAK: &str = "/sys/fs/cgroup/memory.peak";
const MEMORY_MAX_V2: &str = "/sys/fs/cgroup/memory.max";
const MEMORY_MAX_V1: &str = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
const CPU_STAT: &str = "/sys/fs/cgroup/cpu.stat";

/// CPU time consumed, in microseconds, split by mode.
#[derive(Debug, Clone, Copy, Default)]
pub struct CpuTimes {
    pub user_usec: u64,
    pub system_usec: u64,
}

/// Read the cgroup's peak memory high-water mark (`memory.peak`), in bytes.
/// Available on kernels >= 5.19; `None` elsewhere.
pub async fn read_memory_peak() -> Option<u64> {
    tokio::fs::read_to_string(MEMORY_PEAK)
        .await
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// Read the container's memory limit (cgroup v2 `memory.max`, then v1), in
/// bytes. `None` when unlimited (`max`) or unreadable.
pub async fn read_memory_limit() -> Option<u64> {
    for path in [MEMORY_MAX_V2, MEMORY_MAX_V1] {
        if let Some(v) = read_limit_from(Path::new(path)).await {
            return Some(v);
        }
    }
    None
}

async fn read_limit_from(p: &Path) -> Option<u64> {
    let raw = tokio::fs::read_to_string(p).await.ok()?;
    let raw = raw.trim();
    if raw == "max" {
        return None;
    }
    match raw.parse::<u64>().ok()? {
        0 => None,
        v => Some(v),
    }
}

/// Read cumulative CPU times from cgroup v2 `cpu.stat`.
pub async fn read_cpu_times() -> Option<CpuTimes> {
    read_cpu_times_from(Path::new(CPU_STAT)).await
}

pub async fn read_cpu_times_from(path: &Path) -> Option<CpuTimes> {
    let content = tokio::fs::read_to_string(path).await.ok()?;
    Some(CpuTimes {
        user_usec: field_from(&content, "user_usec")?,
        system_usec: field_from(&content, "system_usec")?,
    })
}

/// `value` of a `key value` line in a cgroup stat-format file.
fn field_from(content: &str, key: &str) -> Option<u64> {
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix(key)
            && let Some(v) = rest.strip_prefix(' ')
        {
            return v.trim().parse().ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cpu_stat_fields() {
        let s = "usage_usec 123\nuser_usec 100\nsystem_usec 23\n";
        assert_eq!(field_from(s, "user_usec"), Some(100));
        assert_eq!(field_from(s, "system_usec"), Some(23));
        assert_eq!(field_from(s, "usage_usec"), Some(123));
        assert_eq!(field_from(s, "missing"), None);
    }

    #[tokio::test]
    async fn parses_memory_limit_file() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "2147483648").unwrap();
        assert_eq!(read_limit_from(f.path()).await, Some(2_147_483_648));

        let mut unlimited = tempfile::NamedTempFile::new().unwrap();
        writeln!(unlimited, "max").unwrap();
        assert_eq!(read_limit_from(unlimited.path()).await, None);
    }
}
