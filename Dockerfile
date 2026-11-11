# syntax=docker/dockerfile:1.7
# Multi-target build. Use --target to select a service:
#   docker build --target plc-sim  -t andon/plc-sim .
#   docker build --target gateway  -t andon/gateway .
#   docker build --target processor -t andon/processor .
#   docker build --target api      -t andon/api .       (includes the React dashboard)

FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS gobase
WORKDIR /src
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY pkg ./pkg
ARG TARGETOS TARGETARCH
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH

FROM gobase AS build-plc-sim
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/plc-sim ./cmd/plc-sim
FROM gobase AS build-gateway
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway
FROM gobase AS build-processor
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/processor ./cmd/processor
FROM gobase AS build-api
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api

FROM --platform=$BUILDPLATFORM node:22-alpine AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY web .
RUN npm run build

FROM gcr.io/distroless/static-debian12:nonroot AS plc-sim
COPY --from=build-plc-sim /out/plc-sim /plc-sim
EXPOSE 5020-5040 8080
ENTRYPOINT ["/plc-sim"]

FROM gcr.io/distroless/static-debian12:nonroot AS gateway
COPY --from=build-gateway /out/gateway /gateway
EXPOSE 9101
ENTRYPOINT ["/gateway"]

FROM gcr.io/distroless/static-debian12:nonroot AS processor
COPY --from=build-processor /out/processor /processor
EXPOSE 9102
ENTRYPOINT ["/processor"]

FROM gcr.io/distroless/static-debian12:nonroot AS api
COPY --from=build-api /out/api /api
COPY --from=web /web/dist /web
ENV STATIC_DIR=/web
EXPOSE 8080
ENTRYPOINT ["/api"]
