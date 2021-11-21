FROM rust:alpine3.14 as buildstage

RUN apk add musl-dev && mkdir -p /build/backup_app_rust

ADD Cargo.toml /build/backup_app_rust/
COPY src /build/backup_app_rust/src

RUN mkdir -p /backup_app_rust && \
    cd /build/backup_app_rust && \
    cargo install --path=. --root=/backup_app_rust

FROM alpine:3.14

COPY --from=buildstage /backup_app_rust/bin/* /usr/bin/

RUN apk add postgresql-client

STOPSIGNAL SIGTERM

CMD ["/usr/bin/backup-app-rust"]

