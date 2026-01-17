#!/bin/sh
set -e

COMPOSE_FILE=${COMPOSE_FILE:-docker-compose.yml}
SERVICE=${SERVICE:-hashbox-util}
REMOTE=${REMOTE:?remote host required (e.g. 10.0.0.5:7411)}
INCLUDE=${INCLUDE:-}
EXCLUDE=${EXCLUDE:-}
DRY_RUN=${DRY_RUN:-}

# Move to repo root (docker-compose.yml lives alongside docker/)
cd "$(dirname "$0")/.."

set --
[ -n "$INCLUDE" ] && set -- "$@" --include "$INCLUDE"
[ -n "$EXCLUDE" ] && set -- "$@" --exclude "$EXCLUDE"
[ -n "$DRY_RUN" ] && set -- "$@" --dry-run

echo "Running sync to $REMOTE..."
docker compose -f "$COMPOSE_FILE" run --rm "$SERVICE" sync "$REMOTE" "$@"

echo "Sync complete."
