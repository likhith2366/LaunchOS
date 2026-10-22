#!/usr/bin/env bash
# Finds out where the bottleneck went after sharding removed the hot row.
#
# "Sharding gave 3x" is a ratio. "Sharding gave 3x and then the limit became X" is a
# finding, and this script is how X gets named rather than guessed:
#
#  1. Sweep the simulator's connection cap at a fixed shard count. If throughput rises
#     with concurrency the client was the limit; if it stays flat while latency climbs
#     in proportion, the server is saturated and the extra concurrency is pure queueing.
#  2. Count Postgres transactions committed during a run, to get transactions per
#     allocation — i.e. how many round-trips one checkout actually costs.
#  3. Re-run with synchronous_commit off, to see how much of the ceiling is commit
#     durability rather than round-trip count.
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
HERE="$(cd "$(dirname "$0")" && pwd)"
WSL_ROOT="${LAUNCHOS_WSL_ROOT:-$HOME/launchos}"
SIM="$WSL_ROOT/.build/release/launch-sim"
PSQL="$HERE/wsl-psql.sh"
UNITS="${UNITS:-2000}"
CONTENDERS="${CONTENDERS:-4000}"
SHARDS="${SHARDS:-16}"

xact() {
  bash "$PSQL" -tAc "select xact_commit from pg_stat_database where datname = current_database()" \
    | tr -d '[:space:]'
}

one_run() {
  local conns="$1"
  "$SIM" shard-sweep --units "$UNITS" --contenders "$CONTENDERS" \
    --connections "$conns" --shards "$SHARDS" 2>&1 | head -1
}

echo "== 1. connection-cap sweep at $SHARDS shards =="
echo "   flat throughput with rising p99 means the server is saturated, not the client"
for conns in 32 64 128 256 512; do
  printf '   conns=%-4s %s\n' "$conns" "$(one_run "$conns")"
done

echo
echo "== 2. Postgres transactions per allocation =="
before="$(xact)"
line="$(one_run 128)"
after="$(xact)"
echo "   $line"
echo "   transactions committed during run: $((after - before))"
echo "   (divide by allocations to get round-trips per checkout)"

echo
echo "== 3. how much of the ceiling is commit durability =="
bash "$PSQL" -q -c "ALTER SYSTEM SET synchronous_commit = off" -c "SELECT pg_reload_conf()" >/dev/null
echo "   synchronous_commit=off: $(one_run 128)"
bash "$PSQL" -q -c "ALTER SYSTEM SET synchronous_commit = on" -c "SELECT pg_reload_conf()" >/dev/null
echo "   synchronous_commit=on:  $(one_run 128)"
echo
echo "synchronous_commit restored to on."
