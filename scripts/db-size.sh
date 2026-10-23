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

-- Idempotency keys have no foreign key to launches, on purpose: a key identifies one
-- request across the whole system and legitimately outlives the launch it was for. That
-- also means deleting a launch leaves its keys behind, so a million-customer run leaves
-- a million rows and 471MB that no CASCADE will reach. Retention handles this in a
-- running deployment; a development reset has to say so explicitly.
TRUNCATE idempotency_keys;
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

# Where the disk actually goes, which is not always the tables.
#
# A million-customer run writes three million rows with fsync on, and that generates
# more WAL than the data itself: measured at 1.1GB of pg_wal against 503MB of base after
# one such run. Postgres recycles WAL segments rather than deleting them, so that space
# stays allocated up to max_wal_size and a VACUUM FULL will not touch it. It is not a
# leak, and shrinking max_wal_size to reclaim it would mean more frequent checkpoints and
# more IO during exactly the runs being measured -- a bad trade on a box whose job is
# measuring throughput.
PGDATA="${PGDATA:-$HOME/.local/launchos-services/pgdata}"
if [[ -d "$PGDATA" ]]; then
  echo "== on disk =="
  du -sh "$PGDATA/base" 2>/dev/null | sed 's/$/  (tables and indexes)/'
  du -sh "$PGDATA/pg_wal" 2>/dev/null | sed 's/$/  (write-ahead log, recycled not freed)/'
  du -sh "$PGDATA" 2>/dev/null | sed 's/$/  (total)/'
  echo
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
