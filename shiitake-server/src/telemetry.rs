//! OpenTelemetry wiring for the server: it installs the global tracer and
//! meter providers that carry `tracing` spans and the [`crate::metrics`]
//! instruments out over OTLP.
//!
//! Export is gated on `OTEL_EXPORTER_OTLP_ENDPOINT`: when unset, the server
//! logs to stdout only and metric calls are cheap no-ops. The transport is
//! chosen from `OTEL_EXPORTER_OTLP_PROTOCOL` (`grpc`, `http/protobuf`,
//! `http/json`; default `http/protobuf`) — all plaintext, which is the standard
//! ship-to-a-local-collector pattern and keeps the static-musl build free of
//! aws-lc-rs / native-tls.

use anyhow::{Context, Result};
use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_otlp::{MetricExporter, Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider, trace::SdkTracerProvider};
use tracing_subscriber::{EnvFilter, prelude::*};

/// Held by `main` for the process lifetime; flushes the exporters on
/// shutdown so the final spans/metrics aren't lost. The default (no providers)
/// is the logs-only mode used when OTLP export is not configured.
#[derive(Default)]
pub struct Telemetry {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl Telemetry {
    pub fn shutdown(self) {
        if let Some(tp) = self.tracer_provider {
            let _ = tp.shutdown();
        }
        if let Some(mp) = self.meter_provider {
            let _ = mp.shutdown();
        }
    }
}

/// Initialise logging, tracing, and metrics. Call once at startup. Exporters
/// are enabled only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
pub fn init(service_name: &str) -> Result<Telemetry> {
    // Pin rustls to the ring provider. The OTLP HTTP/gRPC stacks and kube each
    // pull rustls, and their feature mix leaves no single auto-selectable
    // provider — so without this, building the exporter panics. Ring keeps the
    // static-musl build clean (no aws-lc-rs). Idempotent; ignore "already set".
    let _ = rustls::crypto::ring::default_provider().install_default();

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")));

    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_err() {
        tracing_subscriber::registry().with(fmt_layer).init();
        return Ok(Telemetry::default());
    }

    let resource = Resource::builder()
        .with_service_name(service_name.to_string())
        .build();
    let protocol = otlp_protocol();

    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(build_metric_exporter(protocol)?)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(build_span_exporter(protocol)?)
        .with_resource(resource)
        .build();
    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer_provider.tracer(service_name.to_string()));
    global::set_tracer_provider(tracer_provider.clone());

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    Ok(Telemetry {
        tracer_provider: Some(tracer_provider),
        meter_provider: Some(meter_provider),
    })
}

#[derive(Clone, Copy)]
enum Transport {
    Grpc,
    HttpProtobuf,
    HttpJson,
}

/// Pick the OTLP transport from `OTEL_EXPORTER_OTLP_PROTOCOL` (the standard
/// OTel env var). Defaults to HTTP/protobuf when unset or unrecognised.
fn otlp_protocol() -> Transport {
    match std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok().as_deref() {
        Some("grpc") => Transport::Grpc,
        Some("http/json") => Transport::HttpJson,
        _ => Transport::HttpProtobuf,
    }
}

fn build_span_exporter(transport: Transport) -> Result<SpanExporter> {
    let builder = SpanExporter::builder();
    let exporter = match transport {
        Transport::Grpc => builder.with_tonic().build(),
        Transport::HttpProtobuf => builder
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .build(),
        Transport::HttpJson => builder
            .with_http()
            .with_protocol(Protocol::HttpJson)
            .build(),
    };
    exporter.context("build OTLP span exporter")
}

fn build_metric_exporter(transport: Transport) -> Result<MetricExporter> {
    let builder = MetricExporter::builder();
    let exporter = match transport {
        Transport::Grpc => builder.with_tonic().build(),
        Transport::HttpProtobuf => builder
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .build(),
        Transport::HttpJson => builder
            .with_http()
            .with_protocol(Protocol::HttpJson)
            .build(),
    };
    exporter.context("build OTLP metric exporter")
}
