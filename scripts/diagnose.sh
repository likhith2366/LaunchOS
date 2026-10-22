#!/usr/bin/env bash
# Dumps the state the invariant check looks at, for when it fails and the question is
# where the units went.
set -euo pipefail
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
HERE="$(cd "$(dirname "$0")" && pwd)"
PSQL="$HERE/wsl-psql.sh"

bash "$PSQL" <<'SQL'
\echo '== launches =='
SELECT sku, total_units, shard_count, collapse_threshold, collapsed FROM launches;

\echo '== inventory totals vs launch total =='
SELECT l.sku, l.total_units,
       SUM(i.available) AS available, SUM(i.reserved) AS reserved, SUM(i.sold) AS sold,
       SUM(i.available + i.reserved + i.sold) AS accounted,
       l.total_units - SUM(i.available + i.reserved + i.sold) AS missing
  FROM launches l JOIN inventory_shards i ON i.launch_id = l.id
 GROUP BY l.sku, l.total_units;

\echo '== reservations by state =='
SELECT state, count(*), SUM(units) AS units FROM reservations GROUP BY state ORDER BY state;

\echo '== orders =='
SELECT count(*) AS orders, COALESCE(SUM(units),0) AS units FROM orders;

\echo '== committed reservations without an order (lost units) =='
SELECT r.id, r.shard_id, r.units, r.state
  FROM reservations r LEFT JOIN orders o ON o.reservation_id = r.id
 WHERE r.state = 'committed' AND o.id IS NULL;

\echo '== per-shard detail =='
SELECT i.shard_id, i.available, i.reserved, i.sold,
       (SELECT count(*) FROM reservations r
         WHERE r.launch_id = i.launch_id AND r.shard_id = i.shard_id AND r.state = 'held') AS held_rows
  FROM inventory_shards i ORDER BY i.shard_id;

\echo '== sold on shard vs committed reservations on that shard =='
SELECT i.shard_id, i.sold,
       (SELECT COALESCE(SUM(r.units),0) FROM reservations r
         WHERE r.launch_id = i.launch_id AND r.shard_id = i.shard_id AND r.state = 'committed') AS committed_units,
       i.sold - (SELECT COALESCE(SUM(r.units),0) FROM reservations r
         WHERE r.launch_id = i.launch_id AND r.shard_id = i.shard_id AND r.state = 'committed') AS drift
  FROM inventory_shards i ORDER BY i.shard_id;
SQL
