#!/bin/sh
set -e
if [ ! -f docker-compose.yml ]; then
  echo "Error: Run from directory with docker-compose.yml"
  exit 1
fi
echo "Stopping hashbox..."
docker compose stop hashbox || true
echo "Running GC..."
docker compose run --rm hashbox -loglevel=4 -compact gc 2>&1 | tee gc.log | grep -v "........ ..:..:.. [(?] " || true
echo "Starting hashbox..."
docker compose start hashbox
echo "Running verify..."
docker compose run --rm hashbox -loglevel=3 -content -readonly verify 2>&1 | tee verify.log | grep -v "........ ..:..:.. [(?] " || true
echo "GC + verify complete."