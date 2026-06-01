//! Server-side metrics. All emission lives here (the worker reports raw
//! numbers on the wire); the instruments are created against the global OTel
//! meter that [`crate::telemetry::init`] installs, so they no-op until export
//! is configured.
//!
//! Names are self-describing and `shiitake_`-prefixed; units live in the name
//! suffix (`_bytes`/`_seconds`) rather than an OTel unit, so a Prometheus-style
//! exporter doesn't double-suffix them.

use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Gauge, Histogram},
};
use shiitake_worker_api::ResourceUsage;
use std::sync::OnceLock;

/// The process-wide metrics handle, created lazily on first use.
pub fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(Metrics::default)
}

pub struct Metrics {
    exec_terminated: Counter<u64>,
    exec_duration_seconds: Histogram<f64>,
    exec_memory_peak_bytes: Histogram<u64>,
    exec_memory_limit_bytes: Gauge<u64>,
    exec_cpu_seconds: Counter<f64>,
    exec_output_bytes: Histogram<u64>,
    capture_volume_free_bytes: Gauge<u64>,
    pool_workers: Gauge<u64>,
    pool_rejected: Counter<u64>,
}

impl Default for Metrics {
    fn default() -> Self {
        let m = global::meter("shiitake");
        Self {
            exec_terminated: m
                .u64_counter("shiitake_exec_terminated_total")
                .with_description("commands that reached a terminal state, by cause")
                .build(),
            exec_duration_seconds: m
                .f64_histogram("shiitake_exec_duration_seconds")
                .with_description("wall-clock command duration")
                .build(),
            exec_memory_peak_bytes: m
                .u64_histogram("shiitake_exec_memory_peak_bytes")
                .with_description("per-command cgroup memory high-water mark")
                .build(),
            exec_memory_limit_bytes: m
                .u64_gauge("shiitake_exec_memory_limit_bytes")
                .with_description("cgroup memory limit in effect for the command")
                .build(),
            exec_cpu_seconds: m
                .f64_counter("shiitake_exec_cpu_seconds_total")
                .with_description("per-command CPU time, by mode")
                .build(),
            exec_output_bytes: m
                .u64_histogram("shiitake_exec_output_bytes")
                .with_description("captured output size per stream")
                .build(),
            capture_volume_free_bytes: m
                .u64_gauge("shiitake_capture_volume_free_bytes")
                .with_description("free space on the capture volume")
                .build(),
            pool_workers: m
                .u64_gauge("shiitake_pool_workers")
                .with_description("worker pool occupancy, by state")
                .build(),
            pool_rejected: m
                .u64_counter("shiitake_pool_rejected_total")
                .with_description("exec requests rejected because the pool was full")
                .build(),
        }
    }
}

impl Metrics {
    /// Record a completed command: terminal cause, duration, output sizes, and
    /// the resource usage the worker reported.
    pub fn record_exec(
        &self,
        cause: &'static str,
        duration_seconds: f64,
        stdout_bytes: u64,
        stderr_bytes: u64,
        usage: &ResourceUsage,
    ) {
        self.exec_terminated
            .add(1, &[KeyValue::new("cause", cause)]);
        self.exec_duration_seconds.record(duration_seconds, &[]);
        self.exec_output_bytes
            .record(stdout_bytes, &[KeyValue::new("stream", "stdout")]);
        self.exec_output_bytes
            .record(stderr_bytes, &[KeyValue::new("stream", "stderr")]);

        if let Some(peak) = usage.memory_peak_bytes {
            self.exec_memory_peak_bytes.record(peak, &[]);
        }
        if let Some(limit) = usage.memory_limit_bytes {
            self.exec_memory_limit_bytes.record(limit, &[]);
        }
        if let Some(user) = usage.cpu_user_seconds {
            self.exec_cpu_seconds
                .add(user, &[KeyValue::new("mode", "user")]);
        }
        if let Some(system) = usage.cpu_system_seconds {
            self.exec_cpu_seconds
                .add(system, &[KeyValue::new("mode", "system")]);
        }
    }

    /// Record current pool occupancy.
    pub fn set_pool_workers(&self, idle: u64, inflight: u64) {
        self.pool_workers
            .record(idle, &[KeyValue::new("state", "idle")]);
        self.pool_workers
            .record(inflight, &[KeyValue::new("state", "inflight")]);
    }

    /// Record a rejected (pool-full) request.
    pub fn record_pool_rejected(&self) {
        self.pool_rejected.add(1, &[]);
    }

    /// Record free space on the capture volume.
    pub fn record_capture_free_bytes(&self, bytes: u64) {
        self.capture_volume_free_bytes.record(bytes, &[]);
    }
}
