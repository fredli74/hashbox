#!/bin/sh
set -e

COMPOSE_FILE=${COMPOSE_FILE:-docker-compose.yml}
SERVER_SERVICE=${SERVER_SERVICE:-hashbox}
SERVICE=${SERVICE:-hashbox-util}
LOGDIR=${LOGDIR:-.}

# Move to repo root (docker-compose.yml lives alongside docker/)
cd "$(dirname "$0")/.."

mkdir -p "$LOGDIR"

echo "Stopping $SERVER_SERVICE..."
docker compose -f "$COMPOSE_FILE" stop "$SERVER_SERVICE" || true

echo "Running GC..."
docker compose -f "$COMPOSE_FILE" run --rm "$SERVICE" gc -loglevel=4 \
  2>&1 | tee "$LOGDIR/gc.log" | grep -v "........ ..:..:.. [(?] "

echo "Starting $SERVER_SERVICE..."
docker compose -f "$COMPOSE_FILE" start "$SERVER_SERVICE"

echo "Running verify..."
docker compose -f "$COMPOSE_FILE" run --rm "$SERVICE" verify -content -readonly \
  2>&1 | tee "$LOGDIR/verify.log" | grep -v "........ ..:..:.. [(?] "

echo "GC + verify complete."
