# syntax=docker/dockerfile:1

# The distributable artifact: the shiitake-server control plane. Built as a
# static musl binary so the runtime image can be a minimal Alpine.
FROM rust:1-slim-bookworm AS builder
ARG TARGETARCH
RUN apt-get update \
 && apt-get install -y --no-install-recommends musl-tools \
 && rm -rf /var/lib/apt/lists/*
ENV CC_x86_64_unknown_linux_musl=musl-gcc \
    CC_aarch64_unknown_linux_musl=musl-gcc
WORKDIR /src
COPY . .
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) triple=x86_64-unknown-linux-musl ;; \
      arm64) triple=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    rustup target add "$triple"; \
    cargo build --release --locked --target "$triple" --bin shiitake-server; \
    install -D "target/${triple}/release/shiitake-server" /out/shiitake-server

# The server makes outbound TLS calls to the Kubernetes API (container-OOM
# detection) so it needs a CA bundle; it never executes user commands itself.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/shiitake-server /usr/local/bin/shiitake-server
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/shiitake-server"]
