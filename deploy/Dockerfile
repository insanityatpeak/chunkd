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
      ./cmd/chunkd ./cmd/chunkd-meta ./cmd/chunkd-node ./cmd/chunkd-gateway ./cmd/chunkd-demo
# The dashboard's simulated cluster, so the page served by the gateway can
# also switch to the in-browser simulation.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build     mkdir -p /out/wasm && GOOS=js GOARCH=wasm go build -trimpath -ldflags="-s -w" -o /out/wasm/cluster.wasm ./cmd/chunkd-wasm &&     cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" /out/wasm/
# Distroless has no shell to mkdir; ship an empty /data owned by nonroot so
# named volumes mounted there inherit the ownership.
RUN mkdir -p /out/data

# The dashboard, served by the gateway at / (LIVE mode).
FROM node:24-slim AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY web/ ./
COPY --from=build /out/wasm/ public/
RUN npm run build

# distroless: no shell or package manager; runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot AS base
COPY --from=build /out/chunkd /usr/local/bin/chunkd
COPY --from=build --chown=65532:65532 /out/data /data
ENV CHUNKD_DATA=/data

FROM base AS meta
COPY --from=build /out/chunkd-meta /usr/local/bin/chunkd-meta
EXPOSE 7000 9000
ENTRYPOINT ["/usr/local/bin/chunkd-meta"]

FROM base AS node
COPY --from=build /out/chunkd-node /usr/local/bin/chunkd-node
EXPOSE 7000 9000
ENTRYPOINT ["/usr/local/bin/chunkd-node"]

FROM base AS demo
COPY --from=build /out/chunkd-demo /usr/local/bin/chunkd-demo
ENTRYPOINT ["/usr/local/bin/chunkd-demo"]

FROM base AS gateway
COPY --from=build /out/chunkd-gateway /usr/local/bin/chunkd-gateway
COPY --from=web /web/dist /ui
ENV CHUNKD_UI=/ui
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/chunkd-gateway"]
