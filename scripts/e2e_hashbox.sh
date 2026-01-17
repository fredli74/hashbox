#!/usr/bin/env bash
set -euo pipefail

# Hashbox end-to-end smoke: build client, start server (local or Docker), store a tiny fixture (or custom source), restore, and diff.
# Uses only temp directories and a test user; nothing is written to your real HOME.
# Examples:
#   E2E_SOURCE_DIR="$HOME" E2E_DATASET=home-e2e ./scripts/e2e_hashbox.sh
#   E2E_TMP_ROOT=/tmp/hashbox-e2e E2E_SOURCE_DIR="$HOME" ./scripts/e2e_hashbox.sh   # reuse server data across runs
#   E2E_DOCKER=1 ./scripts/e2e_hashbox.sh   # run the server in Docker
#   E2E_DOCKER=1 E2E_DOCKER_BUILD=0 ./scripts/e2e_hashbox.sh   # skip docker build (use existing image)
#   E2E_DOCKER=1 E2E_DOCKER_CMD="sudo docker" ./scripts/e2e_hashbox.sh   # docker via sudo
#   E2E_PORT=19001 ./scripts/e2e_hashbox.sh   # fixed port

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
USE_DOCKER="${E2E_DOCKER:-0}"
DOCKER_CMD="${E2E_DOCKER_CMD:-docker}"
DOCKER_BUILD="${E2E_DOCKER_BUILD:-1}"
if [[ -n "${E2E_TMP_ROOT:-}" ]]; then
  TMP_ROOT="$(mkdir -p "${E2E_TMP_ROOT}" && cd "${E2E_TMP_ROOT}" && pwd)"
  PERSIST_ROOT=1
else
  TMP_ROOT="$(mktemp -d)"
  PERSIST_ROOT=0
fi
PID_FILE="$TMP_ROOT/server.pid"
cleanup() {
  if [[ "$USE_DOCKER" -eq 1 ]]; then
    "$DOCKER_CMD" rm -f hashbox-e2e >/dev/null 2>&1 || true
  else
    if [[ -n "${SERVER_PID:-}" ]]; then
      kill "$SERVER_PID" >/dev/null 2>&1 || true
      wait "$SERVER_PID" 2>/dev/null || true
    fi
  fi
  rm -f "$PID_FILE" >/dev/null 2>&1 || true
  chmod -R u+w "$TMP_ROOT" >/dev/null 2>&1 || true
  if [[ "$PERSIST_ROOT" -eq 0 ]]; then
    rm -rf "$TMP_ROOT"
  fi
}
trap cleanup EXIT

DATA_DIR="$TMP_ROOT/data"
IDX_DIR="$TMP_ROOT/index"
LOG_DIR="$TMP_ROOT/logs"
FIXTURE_DIR="$TMP_ROOT/fixture"
RESTORE_DIR="$TMP_ROOT/restore"
HB_HOME="$TMP_ROOT/home"
rm -rf "$FIXTURE_DIR" "$RESTORE_DIR" "$HB_HOME"
mkdir -p "$DATA_DIR" "$IDX_DIR" "$LOG_DIR" "$FIXTURE_DIR" "$RESTORE_DIR" "$HB_HOME"

SERVER_BIN="$TMP_ROOT/hashbox-server"
CLIENT_BIN="$TMP_ROOT/hashback"
SERVER_LOG="$LOG_DIR/server.log"

echo "Building client..."
GOFLAGS=${GOFLAGS:-}
if [[ "$USE_DOCKER" -eq 1 ]]; then
  DOCKER_USER="$(id -u):$(id -g)"
  ( cd "$ROOT/hashback" && GOWORK=off go build $GOFLAGS -o "$CLIENT_BIN" ./ )
else
  go build $GOFLAGS -o "$SERVER_BIN" "$ROOT/server"
  go build $GOFLAGS -o "$CLIENT_BIN" "$ROOT/hashback"
fi

USER="testuser"
PASS="testpass"
DATASET="${E2E_DATASET:-testset}"
PORT="${E2E_PORT:-}"
if [[ -z "$PORT" ]]; then
  PORT=$(shuf -i 15000-25000 -n 1)
fi

SOURCE_DIR="${E2E_SOURCE_DIR:-}"
if [[ -z "$SOURCE_DIR" ]]; then
  echo "Creating fixture..."
  printf "hello world\n" >"$FIXTURE_DIR/file1.txt"
  mkdir -p "$FIXTURE_DIR/sub"
  printf "subfile\n" >"$FIXTURE_DIR/sub/file2.txt"
  ln -s "../file1.txt" "$FIXTURE_DIR/sub/link-to-file1"
  SOURCE_DIR="$FIXTURE_DIR"
else
  # Expand ~ and normalize path
  SOURCE_DIR="$(cd "$SOURCE_DIR" && pwd)"
  echo "Using custom source dir: $SOURCE_DIR"
fi

if [[ "$USE_DOCKER" -eq 1 ]]; then
  if [[ "$DOCKER_BUILD" -ne 0 ]]; then
    echo "Building Docker image..."
    DOCKER_BUILDKIT=1 "$DOCKER_CMD" build -t hashbox:local "$ROOT"
  fi

  echo "Creating test user (docker)..."
  if ! "$DOCKER_CMD" run --rm \
    --user "$DOCKER_USER" \
    -v "$DATA_DIR:/data" \
    -v "$IDX_DIR:/index" \
    hashbox:local /usr/local/bin/hashbox-server -data /data -index /index adduser "$USER" "$PASS" >/dev/null 2>&1; then
    echo "User may already exist, continuing..."
  fi

  echo "Starting server container on 127.0.0.1:$PORT ..."
  "$DOCKER_CMD" run -d --name hashbox-e2e \
    --user "$DOCKER_USER" \
    -p "$PORT:$PORT" \
    -v "$DATA_DIR:/data" \
    -v "$IDX_DIR:/index" \
    hashbox:local /usr/local/bin/hashbox-server -data /data -index /index -port "$PORT" -loglevel 2 >/dev/null
else
  echo "Creating test user..."
  if ! "$SERVER_BIN" -data "$DATA_DIR" -index "$IDX_DIR" adduser "$USER" "$PASS" >/dev/null 2>&1; then
    echo "User may already exist, continuing..."
  fi

  PID_FILE="$TMP_ROOT/server.pid"
  if [[ -f "$PID_FILE" ]]; then
    OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" >/dev/null 2>&1; then
      echo "Stopping previous server pid $OLD_PID"
      kill "$OLD_PID" >/dev/null 2>&1 || true
      sleep 0.2
    fi
    rm -f "$PID_FILE"
  fi

  echo "Starting server on 127.0.0.1:$PORT ..."
  "$SERVER_BIN" -data "$DATA_DIR" -index "$IDX_DIR" -port "$PORT" -loglevel 2 >"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  echo "$SERVER_PID" >"$PID_FILE"
fi

# Wait for server to listen
for _ in {1..50}; do
  if [[ "$USE_DOCKER" -eq 1 ]]; then
    if "$DOCKER_CMD" logs hashbox-e2e 2>/dev/null | grep -q "listening on"; then
      break
    fi
  else
    if grep -q "listening on" "$SERVER_LOG" >/dev/null 2>&1; then
      break
    fi
  fi
  sleep 0.1
done
if [[ "$USE_DOCKER" -eq 1 ]]; then
  if ! "$DOCKER_CMD" ps -q -f name=hashbox-e2e | grep -q .; then
    echo "Server container exited early. Log:"
    "$DOCKER_CMD" logs hashbox-e2e || true
    exit 1
  fi
else
  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    echo "Server failed to start. Log:"
    cat "$SERVER_LOG"
    exit 1
  fi
fi

CLIENT_FLAGS=(-verbose -user "$USER" -password "$PASS" -server "127.0.0.1:$PORT")

echo "Running backup..."
HOME="$HB_HOME" "$CLIENT_BIN" "${CLIENT_FLAGS[@]}" store "$DATASET" "$SOURCE_DIR"

# Mutate the default fixture to force a delta before the second backup.
if [[ "$SOURCE_DIR" == "$FIXTURE_DIR" ]]; then
  echo "second run content" >>"$FIXTURE_DIR/file1.txt"
fi

echo "Running second backup..."
HOME="$HB_HOME" "$CLIENT_BIN" "${CLIENT_FLAGS[@]}" store "$DATASET" "$SOURCE_DIR"

# Basic client queries against the latest backup
echo "Running client info/list commands..."
HOME="$HB_HOME" "$CLIENT_BIN" "${CLIENT_FLAGS[@]}" info
HOME="$HB_HOME" "$CLIENT_BIN" "${CLIENT_FLAGS[@]}" list "$DATASET"
HOME="$HB_HOME" "$CLIENT_BIN" "${CLIENT_FLAGS[@]}" list "$DATASET" .

rm -rf "$RESTORE_DIR"
mkdir -p "$RESTORE_DIR"

echo "Running restore..."
HOME="$HB_HOME" "$CLIENT_BIN" "${CLIENT_FLAGS[@]}" restore "$DATASET" . "$RESTORE_DIR"

echo "Running diff against restore..."
HOME="$HB_HOME" "$CLIENT_BIN" "${CLIENT_FLAGS[@]}" diff "$DATASET" . "$RESTORE_DIR"

echo "Server log:"
if [[ "$USE_DOCKER" -eq 1 ]]; then
  "$DOCKER_CMD" logs hashbox-e2e
else
  cat "$SERVER_LOG"
fi

echo "Comparing restored tree..."
if ! diff -ru "$SOURCE_DIR" "$RESTORE_DIR"; then
  echo "Diff above shows mismatched paths (expected skips may be intentional)."
  exit 1
fi

echo "Success! Fixture == restored."
