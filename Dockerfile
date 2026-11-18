FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /laminad ./cmd/laminad
FROM debian:bookworm-slim
LABEL org.opencontainers.image.title="LaminaDB" \
      org.opencontainers.image.description="Single-process relational database kernel and HTTP service"
RUN groupadd -g 10001 lamina && useradd -r -u 10001 -g lamina -s /usr/sbin/nologin lamina && mkdir /data && chown lamina:lamina /data
COPY --from=build /laminad /usr/local/bin/laminad
USER 10001:10001
EXPOSE 7386
VOLUME /data
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 CMD ["/usr/local/bin/laminad", "-healthcheck", "-listen", "127.0.0.1:7386"]
ENTRYPOINT ["/usr/local/bin/laminad"]
CMD ["-data", "/data/lamina", "-listen", "0.0.0.0:7386", "-drain-timeout", "10s"]
