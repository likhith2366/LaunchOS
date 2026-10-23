#!/usr/bin/env bash
# Reports how much disk the queue and reservation tables are using, and optionally
# clears the synthetic rows a scale measurement leaves behind.
#
# A million seeded queue entries with their indexes is a few hundred megabytes, which is
# fine to create and not fine to forget about -- the WSL virtual disk never shrinks on
# its own.
#
#   scripts/db-size.sh            # report
#   scripts/db-size.sh --clean    # drop seeded launches, then VACUUM
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
HERE="$(cd "$(dirname "$0")" && pwd)"
PSQL="$HERE/wsl-psql.sh"

if [[ "${1:-}" == "--clean" ]]; then
  echo "==> dropping measurement launches"
  bash "$PSQL" -q <<'SQL'
DELETE FROM launches WHERE sku IN ('position-scale', 'seedtest', 'sweep-probe');
DELETE FROM launches WHERE sku LIKE 'horizon-%';
DELETE FROM launches WHERE sku LIKE 'tail-%';
SQL
  # VACUUM FULL, not plain VACUUM. Plain VACUUM marks the space reusable by Postgres
  # but does not hand it back to the filesystem, so a million deleted queue rows still
  # show as a 169 MB table -- and the WSL virtual disk only ever grows. FULL rewrites
  # the table and releases the space. It takes an exclusive lock, which is fine on a
  # measurement database and would not be in production.
  echo "==> vacuuming (full, so the space actually comes back)"
  bash "$PSQL" -q -c 'VACUUM FULL ANALYZE queue_entries' >/dev/null
  bash "$PSQL" -q -c 'VACUUM FULL ANALYZE reservations' >/dev/null
  bash "$PSQL" -q -c 'VACUUM FULL ANALYZE idempotency_keys' >/dev/null
  bash "$PSQL" -q -c 'VACUUM FULL ANALYZE orders' >/dev/null
fi

bash "$PSQL" <<'SQL'
SELECT relname AS table,
       to_char(n_live_tup, 'FM999,999,999') AS rows,
       pg_size_pretty(pg_total_relation_size(relid)) AS total
  FROM pg_stat_user_tables
 WHERE pg_total_relation_size(relid) > 0
 ORDER BY pg_total_relation_size(relid) DESC;

SELECT pg_size_pretty(pg_database_size(current_database())) AS database_total;
SQL
