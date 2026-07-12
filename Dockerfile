FROM rust:bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM node:22-bookworm-slim
ARG CODEX_VERSION
RUN test -n "$CODEX_VERSION" && npm install --global "@openai/codex@${CODEX_VERSION}" && npm cache clean --force
# node:22-bookworm-slim already provides the `node` user with UID/GID 1000.
# Reuse it so the image builds without a duplicate-UID error and so it matches
# the typical host user's ownership of the bind-mounted Codex home.
RUN mkdir -p /home/codex/.codex /home/codex/runtime && chown -R node:node /home/codex
COPY --from=builder /build/target/release/codex-openai-gateway /usr/local/bin/codex-openai-gateway
USER node
WORKDIR /home/codex/runtime
ENV CODEX_HOME=/home/codex/.codex RUST_LOG=info
EXPOSE 8989
ENTRYPOINT ["codex-openai-gateway"]
