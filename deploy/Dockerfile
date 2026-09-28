# syntax=docker/dockerfile:1
# One build stage compiles every binary; per-role targets keep each image to
# its own entrypoint plus the chunkd CLI used by health checks.

FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ \
      ./cmd/chunkd ./cmd/chunkd-meta ./cmd/chunkd-node ./cmd/chunkd-gateway

# distroless: no shell or package manager; runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot AS base
COPY --from=build /out/chunkd /usr/local/bin/chunkd

FROM base AS meta
COPY --from=build /out/chunkd-meta /usr/local/bin/chunkd-meta
EXPOSE 7000 9000
ENTRYPOINT ["/usr/local/bin/chunkd-meta"]

FROM base AS node
COPY --from=build /out/chunkd-node /usr/local/bin/chunkd-node
EXPOSE 7000 9000
ENTRYPOINT ["/usr/local/bin/chunkd-node"]

FROM base AS gateway
COPY --from=build /out/chunkd-gateway /usr/local/bin/chunkd-gateway
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/chunkd-gateway"]
