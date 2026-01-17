# syntax=docker/dockerfile:1

FROM golang:1.22 AS builder
WORKDIR /src
COPY . ./

ENV CGO_ENABLED=0
ARG HASHBOX_REVISION
ARG HASHBOX_VERSION
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    echo "Building version ${HASHBOX_VERSION} (${HASHBOX_REVISION})"; \
    if [ -n "${HASHBOX_REVISION}" ]; then \
        LDFLAGS="-s -w -X main.Version=${HASHBOX_REVISION}"; \
    else \
        LDFLAGS="-s -w"; \
    fi; \
    go build -ldflags="$LDFLAGS" -o /out/hashbox-server ./server && \
    go build -ldflags="$LDFLAGS" -o /out/hashbox-util ./util

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /out/hashbox-server /usr/local/bin/hashbox-server
COPY --from=builder /out/hashbox-util /usr/local/bin/hashbox-util

WORKDIR /

EXPOSE 7411
