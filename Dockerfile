# syntax=docker/dockerfile:1
FROM rust:1-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release --package sabnzbd-cli --no-default-features

FROM alpine:3.21

RUN apk add --no-cache ca-certificates par2cmdline p7zip unzip su-exec \
    && mkdir -p /config /downloads/complete /downloads/incomplete /downloads/watch

COPY --from=builder /app/target/release/rustnzbd /usr/local/bin/rustnzbd
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENV RUSTNZBD_CONFIG=/config/rustnzbd.toml \
    RUSTNZBD_STATE=/config/rustnzbd_state.json

VOLUME ["/config", "/downloads"]
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
