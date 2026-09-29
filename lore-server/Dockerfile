# Bookworm (glibc 2.36), not trixie (glibc 2.41): the binary must run on the
# Ubuntu 24.04 origin (glibc 2.39), and a glibc is forward- but not
# backward-compatible, so it must be built against 2.39 or older.
FROM rust:slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

# The build context has no .git, so build-helper.rs cannot recover the commit from
# git. Pass it in, and fail the build if it is missing rather than stamp "(build
# unknown)" — an unidentifiable build makes the CLI/server handshake permissive.
ARG LORE_BUILD_SHA
ENV LORE_BUILD_SHA=${LORE_BUILD_SHA}

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    test -n "$LORE_BUILD_SHA" || { echo "ERROR: LORE_BUILD_SHA build-arg is required (see lore-server/Dockerfile)" >&2; exit 1; }; \
    cargo build --locked --release --bin loreserver && \
    cp /build/target/release/loreserver /build/loreserver-bin

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/loreserver-bin /usr/local/bin/loreserver
COPY --from=builder /build/lore-server/config/default.toml /etc/lore/config/default.toml

# Docker environment config: local stores at /data. No QUIC certificate is
# configured, so the server generates an ephemeral self-signed cert for the
# public endpoint at startup (zero-config). For a durable deployment, mount a
# real cert into the container and add a [server.quic.certificate] override.
RUN cat <<'EOF' > /etc/lore/config/docker.toml
[immutable_store.local]
path = "/data"

[mutable_store.local]
path = "/data"
EOF

ENV LORE_CONFIG_PATH=/etc/lore/config
ENV LORE_ENV=docker
ENV RUST_LOG=info

RUN mkdir -p /data
VOLUME /data

# QUIC + gRPC: 41337, HTTP: 41339
EXPOSE 41337 41339

ENTRYPOINT ["loreserver"]
