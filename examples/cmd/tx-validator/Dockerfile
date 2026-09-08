# syntax=docker/dockerfile:1.4

FROM golang:1.26.5 AS build-stage

WORKDIR /app

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOCACHE=/root/.cache/go-build \
    GOMODCACHE=/go/pkg/mod

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o /out/main ./cmd/tx-validator

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build-stage /out/main /main
COPY --from=build-stage /app/db/migrations/idempotent-consumer ./db/migrations/idempotent-consumer

ENTRYPOINT ["/main"]