#!/usr/bin/env bash
# Lists indexes by how little they are used, against what they cost.
#
# An index that serves a query running every two minutes is still paid for on every
# single write: each entry is a WAL-logged insert. This project measured 3KB of WAL per
# sale against about 900 bytes of actual row data, and index maintenance is where the
# difference goes - so an index with a scan count near zero on a write-heavy run is a
# tax, not an optimisation.
#
#   scripts/index-usage.sh
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
HERE="$(cd "$(dirname "$0")" && pwd)"
PSQL="$HERE/wsl-psql.sh"

bash "$PSQL" <<'SQL'
SELECT relname AS table,
       indexrelname AS index,
       idx_scan AS scans,
       pg_size_pretty(pg_relation_size(indexrelid)) AS size
  FROM pg_stat_user_indexes
 ORDER BY idx_scan ASC, pg_relation_size(indexrelid) DESC
 LIMIT 20;

SELECT 'inserts+updates since stats reset' AS writes,
       to_char(sum(n_tup_ins + n_tup_upd), 'FM999,999,999') AS rows
  FROM pg_stat_user_tables;
SQL
