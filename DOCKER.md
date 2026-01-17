# Docker setup

Hashbox containers run as a non-root user (default UID/GID 65532). Bind-mounted
host paths must be writable by the chosen UID/GID.

## Quick start
- Create bind mount directories:
  - `mkdir -p ./data ./index`
- Ensure ownership for the container user:
  - `sudo chown -R 65532:65532 ./data ./index`
- Build/run:
  - `./scripts/docker_build.sh`
- Create the first Hashback user:
  - `docker compose run --rm hashbox adduser <username> <password>`
- Start the server:
  - `docker compose up -d`
- Run GC:
  - `./scripts/docker_gc.sh`

## Notes
- Default bind mounts are `./data:/data` and `./index:/index` in
  `docker-compose.yml`. Override with `HASHBOX_DATA_DIR`/`HASHBOX_INDEX_DIR`
  via a `.env` file (see `.env.example`) or your shell environment.
- `HASHBOX_UID`/`HASHBOX_GID` control the user inside the container.
- `adduser` needs exclusive access to the data directory; stop the server before
  running it if the container is already up.
- For `docker compose run`, the service entrypoint is already set, so pass the
  subcommand directly (omit `/usr/local/bin/hashbox-server`).
