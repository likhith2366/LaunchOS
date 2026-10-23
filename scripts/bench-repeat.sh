#!/usr/bin/env bash
# Runs the allocation benchmark several times and reports the spread.
#
# This exists because of a mistake worth not repeating. Single runs of this workload on
# this box returned 1084, 977, 907, 1076 and 962 allocations/s for configurations that
# differed little or not at all - a spread of about 12%. Several tuning changes were
# measured once each and "improvements" of 10% read off, which is indistinguishable from
# noise. Everything is co-resident here: Postgres, Redis, the node and the load generator
# all compete for the same eight cores, and checkpoints land where they land.
#
# So: repeat, report the median and the full range, and only believe a difference that
# clears the range.
#
#   scripts/bench-repeat.sh                 # 5 runs at 40k
#   RUNS=7 N=100000 scripts/bench-repeat.sh
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
HERE="$(cd "$(dirname "$0")" && pwd)"
WSL_ROOT="${LAUNCHOS_WSL_ROOT:-$HOME/launchos}"
SIM="$WSL_ROOT/.build/release/launch-sim"
RUNS="${RUNS:-5}"
N="${N:-40000}"
SHARDS="${SHARDS:-16}"
CONNECTIONS="${CONNECTIONS:-256}"
LABEL="${LABEL:-current}"

ulimit -n 65536 2>/dev/null || true

PSQL="$HERE/wsl-psql.sh"

# Each run has to start from the same database state, or the sequence drifts downward as
# dead tuples and recycled WAL accumulate: an unisolated five-run sample measured 1090,
# 862, 1049, 881 and 602 allocations/s, a 55% spread that swamps anything worth
# measuring. A truncate leaves dead tuples behind, so this vacuums them away and forces a
# checkpoint, so that a run never begins mid-checkpoint because of its predecessor.
isolate() {
  bash "$PSQL" -q -c 'TRUNCATE launches, idempotency_keys CASCADE' >/dev/null 2>&1 || true
  bash "$PSQL" -q -c 'VACUUM (ANALYZE) orders, reservations, idempotency_keys, inventory_shards, queue_entries' >/dev/null 2>&1 || true
  bash "$PSQL" -q -c 'CHECKPOINT' >/dev/null 2>&1 || true
}

echo "==> $LABEL: $RUNS runs of $N customers at $SHARDS shards"
echo "    (vacuumed and checkpointed before each run)"
rates=()
p99s=()
for i in $(seq "$RUNS"); do
  isolate
  line="$("$SIM" shard-sweep --units "$N" --contenders "$N" \
    --shards "$SHARDS" --connections "$CONNECTIONS" 2>&1 | grep -E 'shards=' | head -1)"
  rate="$(printf '%s' "$line" | awk '{print $3}')"
  p99="$(printf '%s' "$line" | awk '{print $(NF-1)}')"
  rates+=("$rate")
  p99s+=("$p99")
  printf '    run %-2s  %8s alloc/s   p99 %8s ms\n' "$i" "$rate" "$p99"
done

python3 - "$LABEL" "${rates[@]}" -- "${p99s[@]}" <<'PY'
import statistics
import sys

args = sys.argv[1:]
label = args[0]
split = args.index("--")
rates = [float(v) for v in args[1:split]]
p99s = [float(v) for v in args[split + 1:]]


def summarise(name, values, unit):
    median = statistics.median(values)
    low, high = min(values), max(values)
    spread = (high - low) / median * 100 if median else 0
    print(
        f"  {name:<12} median {median:>8.1f} {unit}   "
        f"range {low:.1f} - {high:.1f}   spread {spread:.1f}% of median"
    )


print()
print(f"== {label} ==")
summarise("throughput", rates, "alloc/s")
summarise("p99", p99s, "ms")
print()
print("  Only believe a difference between configurations that is larger than the")
print("  spread above. Anything smaller is this box, not the change.")
PY
