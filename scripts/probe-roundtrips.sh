#!/usr/bin/env bash
# Measures Postgres round-trips per checkout, separating the success path from the
# sold-out path.
#
# Why separate them. The two paths cost very different amounts, so a single blended
# average hides whichever one you just changed. The shard sweep runs twice as many
# requests as there is stock, so half its requests fail, and a change that makes the
# *purchase* path much cheaper barely moves the blended figure. Measuring a
# success-dominated workload is the only way to see it.
#
#   scripts/probe-roundtrips.sh
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
HERE="$(cd "$(dirname "$0")" && pwd)"
WSL_ROOT="${LAUNCHOS_WSL_ROOT:-$HOME/launchos}"
SIM="$WSL_ROOT/.build/release/launch-sim"
PSQL="$HERE/wsl-psql.sh"
SHARDS="${SHARDS:-16}"
CONNECTIONS="${CONNECTIONS:-64}"
N="${N:-3000}"

ulimit -n 65536 2>/dev/null || true

xact() {
  bash "$PSQL" -tAc "select xact_commit from pg_stat_database where datname = current_database()" \
    | tr -d '[:space:]'
}

measure() {
  local label="$1" units="$2" contenders="$3"
  local before after line allocations
  before="$(xact)"
  line="$("$SIM" shard-sweep --units "$units" --contenders "$contenders" \
    --connections "$CONNECTIONS" --shards "$SHARDS" 2>&1 | head -1)"
  after="$(xact)"
  allocations="$(printf '%s' "$line" | awk '{print $3}')"
  echo "   $label"
  echo "     $line"
  echo "     transactions: $((after - before)) over $contenders requests"
  python3 - "$((after - before))" "$contenders" "$units" <<'PY'
import sys
xacts, requests, units = (int(v) for v in sys.argv[1:4])
purchases = min(units, requests)
failures = requests - purchases
print(f"     round-trips per request: {xacts / max(requests, 1):.2f}")
if failures == 0:
    print(f"     every request succeeded, so that figure is the success path alone")
else:
    print(f"     blended over {purchases} purchases and {failures} sold-out requests")
PY
}

echo "== round-trips per checkout at $SHARDS shards =="
echo
measure "all requests succeed (stock = requests)" "$N" "$N"
echo
measure "half the requests sell out (stock = requests / 2)" "$((N / 2))" "$N"
echo
echo "The success path is what the commit statement affects. The sold-out path pays for"
echo "its probe sweep, the availability check that tells fragmentation from a sell-out,"
echo "and storing the terminal answer against the idempotency key."
