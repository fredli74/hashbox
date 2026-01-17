#!/bin/sh
set -e

COMPOSE_FILE=${COMPOSE_FILE:-docker-compose.yml}

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

REVISION=""
VERSION=""
if command -v git >/dev/null 2>&1; then
    if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        REVISION="$(git describe HEAD --tags --always 2>/dev/null || true)"
        VERSION="$(git describe HEAD --tags --always --abbrev=0 2>/dev/null || true)"
    fi
fi

HASHBOX_REVISION="$REVISION" HASHBOX_VERSION="$VERSION" \
    docker build -t hashbox:local .
