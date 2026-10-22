#!/usr/bin/env bash
# Measures what a queue position costs as the queue gets deeper, and how far the
# constant-time estimate is from the exact count.
#
# This is the measurement that decides whether a million-person waiting room is
# answerable. "How many people are ahead of me" is the most frequent request in a launch,
# and the obvious implementation counts them, which is O(queue depth). The estimate is
# arithmetic on a dense arrival index and a departure counter, so it should not care how
# deep the queue is.
#
# Both are asked of the same running node over the same rows, and `?exact=true` forces
# the count, so the comparison is like-for-like rather than two different runs.
#
#   scripts/probe-position-scale.sh
#   DEPTHS="1000 10000 100000 1000000" scripts/probe-position-scale.sh
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
HERE="$(cd "$(dirname "$0")" && pwd)"
PSQL="$HERE/wsl-psql.sh"
NODE="${NODE:-http://127.0.0.1:8080}"
DEPTHS="${DEPTHS:-1000 10000 100000 1000000}"
SKU="${SKU:-position-scale}"
# How many polls to time at each depth. Enough to see past one-off noise.
SAMPLES="${SAMPLES:-40}"

curl -fsS -m 10 "$NODE/health" >/dev/null || {
  echo "no node at $NODE; run scripts/wsl-cluster.sh start" >&2
  exit 1
}

echo "==> creating $SKU"
curl -fsS -m 20 -X POST "$NODE/launches" \
  -H 'content-type: application/json' \
  -d "{\"sku\":\"$SKU\",\"totalUnits\":1000,\"shardCount\":16,
       \"admission\":{\"kind\":\"fixed\",\"policy\":\"strictFIFO\",\"initialRate\":1}}" \
  >/dev/null

LAUNCH_ID="$(bash "$PSQL" -tAc "select id from launches where sku = '$SKU'" | tr -d '[:space:]')"
SEQUENCE="$(bash "$PSQL" -tAc "select queue_sequence from launches where sku = '$SKU'" | tr -d '[:space:]')"
if [[ -z "$LAUNCH_ID" || -z "$SEQUENCE" ]]; then
  echo "could not read the launch back; is $SKU already present with an older schema?" >&2
  exit 1
fi

echo
printf '%10s  %14s  %14s  %12s  %10s\n' \
  depth "est server(ms)" "exact srv(ms)" "speedup" "error"
printf '%10s  %14s  %14s  %12s  %10s\n' \
  "---------" "-------------" "-------------" "-----------" "---------"

seeded=0
for depth in $DEPTHS; do
  need=$((depth - seeded))
  if (( need > 0 )); then
    # Seeded with generate_series rather than through the API. A million HTTP joins is a
    # separate measurement (launch-sim does that one); here the queue depth is the
    # independent variable and the cost of getting there is not what is being timed.
    bash "$PSQL" -q -c "
      INSERT INTO queue_entries
          (launch_id, customer_id, arrival_index, bucket, arrival_at, state)
      SELECT '$LAUNCH_ID',
             'seed-' || g,
             nextval('$SEQUENCE'),
             0,
             now() - interval '1 hour',
             'waiting'
        FROM generate_series($((seeded + 1)), $depth) g
      ON CONFLICT DO NOTHING" >/dev/null
    seeded=$depth
  fi

  # Poll the customer at the very back, which is the worst case for the count: it has
  # the whole queue ahead of it.
  last="seed-$depth"

  time_ms() {
    local url="$1"
    local source="$2"
    python3 - "$url" "$SAMPLES" "$NODE/metrics" "$source" <<'PY'
import json
import re
import subprocess
import sys
import time

url, samples, metrics_url, source = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
def metric(suffix):
    body = subprocess.check_output(["curl", "-fsS", "-m", "30", metrics_url], text=True)
    pattern = rf'^launchos_position_duration_ms_{suffix}{{source="{source}"}} ([0-9.eE+-]+)$'
    match = re.search(pattern, body, re.MULTILINE)
    return float(match.group(1)) if match else 0.0

before_sum, before_count = metric("sum"), metric("count")
timings = []
position = None
reported_source = None
for _ in range(samples):
    start = time.perf_counter()
    out = subprocess.run(
        ["curl", "-fsS", "-m", "120", url], capture_output=True, text=True, check=False
    )
    timings.append((time.perf_counter() - start) * 1000)
    if out.returncode == 0 and out.stdout:
        try:
            body = json.loads(out.stdout)
            position, reported_source = body["position"], body["positionSource"]
        except Exception:
            pass
after_sum, after_count = metric("sum"), metric("count")
observed = after_count - before_count
if observed != samples:
    raise SystemExit(f"expected {samples} {source} observations, got {observed}")
server_mean = (after_sum - before_sum) / observed
print(f"{server_mean:.3f} {position if position is not None else -1} {reported_source or 'unknown'}")
PY
  }

  # Force the estimator on every sample. The default path caches the first answer in
  # Redis, so repeated default polls measure cache reads instead of estimation.
  read -r est_ms est_pos est_src <<<"$(time_ms "$NODE/launches/$SKU/queue/$last?position=estimated" estimated)"
  read -r exact_ms exact_pos _ <<<"$(time_ms "$NODE/launches/$SKU/queue/$last?exact=true" exactCount)"

  python3 - "$depth" "$est_ms" "$exact_ms" "$est_pos" "$exact_pos" "$est_src" <<'PY'
import sys

depth, est_ms, exact_ms, est_pos, exact_pos, src = sys.argv[1:7]
depth, est_ms, exact_ms = int(depth), float(est_ms), float(exact_ms)
est_pos, exact_pos = int(est_pos), int(exact_pos)
speedup = exact_ms / est_ms if est_ms > 0 else 0
error = abs(est_pos - exact_pos)
rel = (error / exact_pos * 100) if exact_pos else 0.0
print(
    f"{depth:>10}  {est_ms:>14.3f}  {exact_ms:>14.3f}  "
    f"{speedup:>11.2f}x  {error:>5} ({rel:.3f}%)"
)
if src != "estimated":
    raise SystemExit(f"expected estimated position, got '{src}'")
PY
done

echo
echo "The estimated column should be flat and the exact column should grow with depth."
echo "Error is the estimate's distance from the count it replaced, in queue places."
echo
echo "Both numbers are server-side mean time over $SAMPLES forced polls of the customer"
echo "at the back of the queue. Client process startup and HTTP transport are excluded."
