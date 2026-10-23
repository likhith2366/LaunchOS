#!/usr/bin/env bash
# Samples how fast orders are actually being committed, right now.
#
# Written as a file because eyeballing two order counts taken an unknown number of
# minutes apart is how you convince yourself throughput has collapsed when it has not --
# which happened once already during this work.
#
#   scripts/order-rate.sh [seconds]
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
HERE="$(cd "$(dirname "$0")" && pwd)"
PSQL="$HERE/wsl-psql.sh"
WINDOW="${1:-30}"

count() {
  bash "$PSQL" -tAc 'SELECT count(*) FROM orders' | tr -d '[:space:]'
}

before="$(count)"
start="$(date +%s)"
sleep "$WINDOW"
after="$(count)"
end="$(date +%s)"

elapsed=$((end - start))
delta=$((after - before))
python3 - "$delta" "$elapsed" "$before" "$after" <<'PY'
import sys

delta, elapsed, before, after = (int(v) for v in sys.argv[1:5])
rate = delta / elapsed if elapsed else 0
print(f"{rate:.1f} orders/s over {elapsed}s  ({before:,} -> {after:,})")
PY
