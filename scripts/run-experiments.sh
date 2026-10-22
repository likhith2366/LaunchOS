#!/usr/bin/env bash
# Runs the whole measurement suite and writes results/results.json and results/RESULTS.md.
#
# Every number in the README comes from here. Nothing is typed in by hand, so the prose
# cannot drift from the measurements.
#
#   scripts/run-experiments.sh                 # full suite
#   SCALE=0.5 scripts/run-experiments.sh       # smaller, for a constrained machine
#   NODES=3 scripts/run-experiments.sh         # against a three-node cluster
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
HERE="$(cd "$(dirname "$0")" && pwd)"
WSL_ROOT="${LAUNCHOS_WSL_ROOT:-$HOME/launchos}"
WIN_ROOT="${LAUNCHOS_WIN_ROOT:-/mnt/a/Projects/LaunchOs}"
SIM="$WSL_ROOT/.build/release/launch-sim"
SCALE="${SCALE:-1.0}"
NODES="${NODES:-1}"
BASE_PORT="${BASE_PORT:-8080}"
CONNECTIONS="${CONNECTIONS:-1500}"
SEED="${SEED:-2779166260}"

# The simulator opens a connection per concurrent customer. The default limit of 1024
# is below what the burst arms need, and hitting it looks like server errors rather
# than like a client running out of descriptors.
ulimit -n 65536 2>/dev/null || true

if ! [[ -x "$SIM" ]]; then
  echo "no simulator at $SIM; run scripts/wsl-build.sh -c release first" >&2
  exit 1
fi

NODE_ARGS=()
for ((i = 0; i < NODES; i++)); do
  NODE_ARGS+=(--node "http://127.0.0.1:$((BASE_PORT + i))")
done

echo "==> ensuring $NODES node(s) are up"
NODES="$NODES" BASE_PORT="$BASE_PORT" bash "$HERE/wsl-cluster.sh" start

# Hardware and configuration are recorded with the results. A latency figure without
# the machine and the offered load behind it is not a measurement.
CORES="$(nproc)"
MEM="$(awk '/MemTotal/ {printf "%.0f GB", $2/1048576}' /proc/meminfo)"
PG_VERSION="$(bash "$HERE/wsl-psql.sh" -tAc 'show server_version' 2>/dev/null | awk 'NR==1{print $1}' || echo unknown)"
NOTES="WSL2 on Windows, ${CORES} cores, ${MEM} RAM. Postgres ${PG_VERSION} and Redis on the same host, fsync on, synchronous_commit on. ${NODES} allocator node(s); simulator capped at ${CONNECTIONS} concurrent connections. Simulated customers are not simultaneous sockets: offered arrival rate, peak concurrent connections and completed purchases are reported separately."

mkdir -p "$WIN_ROOT/results"

echo "==> running suite (scale $SCALE)"
"$SIM" all \
  "${NODE_ARGS[@]}" \
  --scale "$SCALE" \
  --connections "$CONNECTIONS" \
  --seed "$SEED" \
  --capacity "${CAPACITY:-620}" \
  --target-p99 "${TARGET_P99:-600}" \
  --output "$WIN_ROOT/results" \
  --notes "$NOTES"

echo
echo "==> where the next ceiling went after sharding"
UNITS=2000 CONTENDERS=4000 SHARDS=16 bash "$HERE/probe-bottleneck.sh" \
  | tee "$WIN_ROOT/results/bottleneck.txt"

echo
echo "==> refreshing the README's results block"
python3 "$HERE/update-readme.py" "$WIN_ROOT/results/results.json" "$WIN_ROOT/README.md"
python3 "$HERE/gen-resume.py" "$WIN_ROOT/results/results.json" "$WIN_ROOT/docs/RESUME.md"

echo
echo "wrote $WIN_ROOT/results/results.json"
echo "wrote $WIN_ROOT/results/RESULTS.md"
echo "wrote $WIN_ROOT/results/bottleneck.txt"
