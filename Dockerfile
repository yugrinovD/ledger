# syntax=docker/dockerfile:1.7

FROM golang:1.27-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w \
        -X github.com/yugrinovD/ledger/internal/version.Version=${VERSION} \
        -X github.com/yugrinovD/ledger/internal/version.Commit=${COMMIT} \
        -X github.com/yugrinovD/ledger/internal/version.Date=${DATE}" \
      -o /out/ledger-api ./cmd/api && \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w \
        -X github.com/yugrinovD/ledger/internal/version.Version=${VERSION} \
        -X github.com/yugrinovD/ledger/internal/version.Commit=${COMMIT} \
        -X github.com/yugrinovD/ledger/internal/version.Date=${DATE}" \
      -o /out/ledger-worker ./cmd/worker && \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w \
        -X github.com/yugrinovD/ledger/internal/version.Version=${VERSION} \
        -X github.com/yugrinovD/ledger/internal/version.Commit=${COMMIT} \
        -X github.com/yugrinovD/ledger/internal/version.Date=${DATE}" \
      -o /out/ledgerctl ./cmd/ledgerctl

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/ledger-api /out/ledger-worker /out/ledgerctl /usr/local/bin/
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/ledger-api"]
