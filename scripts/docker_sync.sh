#!/bin/sh
set -e

if [ ! -f docker-compose.yml ]; then
  echo "Error: Run from directory with docker-compose.yml"
  exit 1
fi

REMOTE="$1"
if [ -z "$REMOTE" ]; then
  echo "Usage: $0 <remote> [sync options] (e.g. 10.0.0.5:7411 -dry-run)"
  exit 1
fi

shift
echo "Running sync to $REMOTE..."
set -- "$REMOTE" "$@"
if [ -n "$INCLUDE" ]; then
  set -- "$@" -include "$INCLUDE"
fi
if [ -n "$EXCLUDE" ]; then
  set -- "$@" -exclude "$EXCLUDE"
fi
if [ -n "$DRY_RUN" ]; then
  set -- "$@" -dry-run
fi
docker compose run --rm hashbox-util sync "$@"

echo "Sync complete."
