#!/usr/bin/env bash
# psql against the WSL-local Postgres from scripts/wsl-services.sh.
#
#   scripts/wsl-psql.sh -c 'select count(*) from orders'
set -euo pipefail
PREFIX="$HOME/.local/launchos-services"
export LD_LIBRARY_PATH="$PREFIX/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PGPASSWORD="${POSTGRES_PASSWORD:-launchos}"
exec "$PREFIX/usr/lib/postgresql/16/bin/psql" \
  -h 127.0.0.1 -p "${PGPORT:-5432}" -U "${POSTGRES_USER:-launchos}" \
  -d "${POSTGRES_DB:-launchos}" "$@"
