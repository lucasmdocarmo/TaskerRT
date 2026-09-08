# syntax=docker/dockerfile:1
# Multi-stage: a full toolchain builds; a slim image runs. Two final targets.

FROM rust:1.96-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release -p taskerd -p tasker-worker -p tasker-cli

FROM debian:bookworm-slim AS runtime
RUN useradd --system --uid 10001 --no-create-home tasker
COPY --from=builder /src/target/release/taskerd /src/target/release/tasker-worker /src/target/release/tasker-cli /usr/local/bin/
USER 10001
ENV RUST_LOG=info

FROM runtime AS taskerd
ENTRYPOINT ["taskerd"]

FROM runtime AS tasker-worker
ENTRYPOINT ["tasker-worker"]
