#!/usr/bin/env bash
# Applies the launchos Postgres settings to an already-initialised data directory.
#
# scripts/wsl-services.sh only writes postgresql.conf when it runs initdb, so an
# existing database never picks up a changed setting. This applies them in place with
# ALTER SYSTEM and restarts, which is the only way to change shared_buffers.
#
#   scripts/pg-tune.sh          # apply and restart
#   scripts/pg-tune.sh --show   # report the current values
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
HERE="$(cd "$(dirname "$0")" && pwd)"
PSQL="$HERE/wsl-psql.sh"
PREFIX="$HOME/.local/launchos-services"
PGBIN="$PREFIX/usr/lib/postgresql/16/bin"
PGDATA="${PGDATA:-$PREFIX/pgdata}"
export LD_LIBRARY_PATH="$PREFIX/usr/lib/x86_64-linux-gnu:$PREFIX/usr/lib/postgresql/16/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

SETTINGS=(
  commit_delay commit_siblings shared_buffers effective_cache_size
  max_wal_size min_wal_size checkpoint_completion_target wal_buffers
  wal_compression fsync synchronous_commit
)

show() {
  echo "== current settings =="
  for name in "${SETTINGS[@]}"; do
    printf '%-32s %s\n' "$name" "$(bash "$PSQL" -tAc "SHOW $name" | tr -d '[:space:]')"
  done
}

if [[ "${1:-}" == "--show" ]]; then
  show
  exit 0
fi

# commit_delay and wal_compression are deliberately zero and off. Both were measured as
# throughput regressions on this workload - see the note in wsl-services.sh - and both
# are the obvious things to reach for, so they are set explicitly rather than left to
# whatever the last experiment happened to leave behind.
echo "==> applying settings"
# fsync and synchronous_commit are set explicitly rather than left alone, because an
# earlier experiment turned synchronous_commit off to measure what durability costs and
# leaving it off would silently inflate every number after it.
bash "$PSQL" -q <<'SQL'
ALTER SYSTEM SET commit_delay = 0;
ALTER SYSTEM SET commit_siblings = 5;
ALTER SYSTEM SET shared_buffers = '1GB';
ALTER SYSTEM SET effective_cache_size = '4GB';
ALTER SYSTEM SET work_mem = '8MB';
ALTER SYSTEM SET max_wal_size = '2GB';
ALTER SYSTEM SET min_wal_size = '512MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '64MB';
ALTER SYSTEM SET wal_compression = 'off';
ALTER SYSTEM SET fsync = 'on';
ALTER SYSTEM SET synchronous_commit = 'on';
SQL

echo "==> restarting (shared_buffers cannot change without it)"
"$PGBIN/pg_ctl" -D "$PGDATA" -l "$PREFIX/postgres.log" -o "-p ${PGPORT:-5432}" -m fast restart -w

show
