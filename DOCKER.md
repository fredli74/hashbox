# Docker setup

Hashbox containers run as a non-root user (default UID/GID 65532). Bind-mounted
host paths must be writable by the chosen UID/GID.

## Quick start
- Create bind mount directories:
  - `mkdir -p ./data ./index`
- Ensure ownership for the container user:
  - `sudo chown -R 65532:65532 ./data ./index`
- Build/run:
  - `docker compose build`
  - `docker compose up -d`
- Create the first Hashback user:
  - `docker compose run --rm hashbox /usr/local/bin/hashbox-server -data /data -index /index adduser <username> <password>`
- Run GC:
  - `./scripts/docker_gc.sh`

## Notes
- Default bind mounts are `./data:/data` and `./index:/index` in
  `docker-compose.yml`. Override with `HASHBOX_DATA_DIR`/`HASHBOX_INDEX_DIR`
  via a `.env` file (see `.env.example`) or your shell environment.
- `HASHBOX_UID`/`HASHBOX_GID` control the user inside the container.
