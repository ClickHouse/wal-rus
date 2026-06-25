# walrus wal-receive image — the Rust sync-standby receiver.
#
# Build from the wal-rus repo root (native arm64 on a Graviton/aarch64 host):
#   docker build --platform linux/arm64 \
#     -f docker/wal-receive/Dockerfile -t walg-receive:walrus .
#
# Runs as uid 10001, WORKDIR + partials at /var/lib/walg/partials, mTLS material
# mounted at /etc/walg/tls. See entrypoint.sh for the env contract.

FROM rust:1-bookworm AS builder
# aws-lc-rs builds AWS-LC via cmake (+ clang/libclang for its bindgen); the
# bundled zstd/lz4 *-sys crates need a C compiler (gcc ships in the base image).
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake clang libclang-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --bin walrus

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/walrus /usr/bin/walrus
COPY docker/wal-receive/entrypoint.sh /usr/local/bin/wal-receive-entrypoint
RUN chmod +x /usr/local/bin/wal-receive-entrypoint \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin walg \
    && mkdir -p /var/lib/walg/partials \
    && chown 10001:10001 /var/lib/walg/partials
WORKDIR /var/lib/walg/partials
USER walg
ENTRYPOINT ["/usr/local/bin/wal-receive-entrypoint"]
