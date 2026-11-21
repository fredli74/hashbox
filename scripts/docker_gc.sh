#!/bin/sh
set -e
if [ ! -f docker-compose.yml ]; then
  echo "Error: Run from directory with docker-compose.yml"
  exit 1
fi

# run_logged <name> <command...>: shows filtered output, keeps the full log in <name>.log and returns the command's exit status
run_logged() {
  name=$1
  shift
  ( "$@" 2>&1; echo $? > "$name.status" ) | tee "$name.log" | grep -v "........ ..:..:.. [(?] " || true
  status=$(cat "$name.status")
  rm -f "$name.status"
  return "$status"
}

echo "Stopping hashbox..."
docker compose stop hashbox || true
echo "Running GC..."
gc_status=0
run_logged gc docker compose run --rm hashbox -loglevel=4 -compact gc || gc_status=$?
echo "Starting hashbox..."
docker compose start hashbox
if [ "$gc_status" -ne 0 ]; then
  echo "GC failed (exit $gc_status), see gc.log"
  exit "$gc_status"
fi
echo "Running verify..."
if ! run_logged verify docker compose run --rm hashbox -loglevel=3 -content verify; then
  echo "Verify failed, see verify.log"
  exit 1
fi
echo "GC + verify complete."
