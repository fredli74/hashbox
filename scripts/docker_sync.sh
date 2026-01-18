#!/bin/sh
set -e

if [ ! -f docker-compose.yml ]; then
  echo "Error: Run from directory with docker-compose.yml"
  exit 1
fi

if [ -z "$REMOTE" ]; then
  echo "Error: remote host required (e.g. 10.0.0.5:7411)"
  exit 1
fi

INCLUDE_OPT=${INCLUDE:+--include "$INCLUDE"}
EXCLUDE_OPT=${EXCLUDE:+--exclude "$EXCLUDE"}
DRY_RUN_OPT=${DRY_RUN:+--dry-run}

echo "Running sync to $REMOTE..."
docker compose run --rm hashbox-util sync "$REMOTE" $INCLUDE_OPT $EXCLUDE_OPT $DRY_RUN_OPT

echo "Sync complete."