#!/usr/bin/env bash
# Measures how many bytes of write-ahead log one sale costs.
#
# Worth measuring rather than assuming. Postgres tuning aimed at the fsync path - group
# commit via commit_delay, plus wal_compression - made throughput *worse* here
# (977 -> 730 allocations/s), so "commit durability is the ceiling" is not by itself
# enough to act on. If a sale writes a lot of WAL then write volume is the thing to
# attack; if it writes little, the cost is elsewhere and shrinking rows is wasted effort.
#
#   scripts/probe-wal.sh [customers]
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
HERE="$(cd "$(dirname "$0")" && pwd)"
PSQL="$HERE/wsl-psql.sh"
WSL_ROOT="${LAUNCHOS_WSL_ROOT:-$HOME/launchos}"
SIM="$WSL_ROOT/.build/release/launch-sim"
N="${1:-40000}"

ulimit -n 65536 2>/dev/null || true

lsn() {
  bash "$PSQL" -tAc 'SELECT pg_current_wal_lsn()' | tr -d '[:space:]'
}

orders() {
  bash "$PSQL" -tAc 'SELECT count(*) FROM orders' | tr -d '[:space:]'
}

before_lsn="$(lsn)"
before_orders="$(orders)"

"$SIM" shard-sweep --units "$N" --contenders "$N" --shards 16 --connections 256 \
  2>&1 | grep -E 'shards='

after_lsn="$(lsn)"
after_orders="$(orders)"

bytes="$(bash "$PSQL" -tAc "SELECT pg_wal_lsn_diff('$after_lsn', '$before_lsn')" | tr -d '[:space:]')"

python3 - "$bytes" "$before_orders" "$after_orders" <<'PY'
import sys

wal_bytes = int(float(sys.argv[1]))
before, after = int(sys.argv[2]), int(sys.argv[3])
# The run resets the database before it starts, so the order count goes down rather than
# up and a naive delta is negative. After a reset the surviving orders are this run's.
sold = after if after < before else after - before
print()
print(f"  WAL written      {wal_bytes / 1_048_576:>10.1f} MB")
print(f"  sales            {sold:>10,}")
if sold:
    per = wal_bytes / sold
    print(f"  WAL per sale     {per:>10.0f} bytes")
    print()
    print("  A sale writes a reservation row, an order row and an idempotency record,")
    print("  updates two shard counters, and commits three times. The idempotency record")
    print("  carries the response body it exists to replay, which measured ~560 bytes of")
    print("  the row width - so if WAL per sale is far above the few hundred bytes those")
    print("  rows actually need, write volume is worth attacking.")
PY
