#!/usr/bin/env bash
# Shows what Postgres is actually spending its time on right now.
#
# Written as a file rather than a -c one-liner because the quoting needed to get a
# regexp through two shells into psql is its own small nightmare.
#
#   scripts/pg-activity.sh          # in-flight queries, slowest first
#   scripts/pg-activity.sh --top    # slowest statements overall, if pg_stat_statements is on
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
HERE="$(cd "$(dirname "$0")" && pwd)"
PSQL="$HERE/wsl-psql.sh"

if [[ "${1:-}" == "--top" ]]; then
  bash "$PSQL" <<'SQL'
SELECT round(mean_exec_time::numeric, 2) AS mean_ms,
       calls,
       round(total_exec_time::numeric) AS total_ms,
       left(regexp_replace(query, '\s+', ' ', 'g'), 100) AS query
  FROM pg_stat_statements
 WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
 ORDER BY total_exec_time DESC
 LIMIT 15;
SQL
  exit 0
fi

bash "$PSQL" <<'SQL'
SELECT pid,
       state,
       round(extract(epoch FROM (now() - query_start)) * 1000) AS running_ms,
       wait_event_type,
       left(regexp_replace(query, '\s+', ' ', 'g'), 95) AS query
  FROM pg_stat_activity
 WHERE datname = current_database()
   AND state <> 'idle'
   AND pid <> pg_backend_pid()
 ORDER BY running_ms DESC NULLS LAST
 LIMIT 10;

SELECT relname AS table,
       to_char(n_live_tup, 'FM999,999,999') AS live_rows,
       seq_scan,
       to_char(seq_tup_read, 'FM999,999,999,999') AS seq_rows_read,
       idx_scan
  FROM pg_stat_user_tables
 WHERE seq_scan > 0 OR idx_scan > 0
 ORDER BY seq_tup_read DESC
 LIMIT 8;
SQL
