#!/usr/bin/env bash
# Pulls the headline numbers out of results/results.json.
#
# The README and the resume bullet quote numbers; this is where they are read from, so
# a number in the prose can always be traced back to a field in the record rather than
# to someone's memory of a run.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
RESULTS="${1:-$HERE/../results/results.json}"

if ! [[ -f "$RESULTS" ]]; then
  echo "no results at $RESULTS; run scripts/run-experiments.sh first" >&2
  exit 1
fi

python3 - "$RESULTS" <<'PY'
import json
import sys

with open(sys.argv[1]) as handle:
    data = json.load(handle)

env = data.get("environment", {})
print(f"recorded   {env.get('recordedAt','?')} on {env.get('host','?')}, {env.get('cores','?')} cores, {env.get('nodes','?')} node(s)")
print()

oversell = data.get("oversell")
if oversell:
    print("CORRECTNESS")
    print(f"  oversold units                 {oversell['oversoldUnits']}")
    print(f"  invariant held every trial     {oversell['invariantHeldEveryTrial']}")
    print(f"  trials                         {oversell['trials'] * 2} (1 shard and 16 shards)")
    print(f"  successes per trial            {oversell['successesPerTrial']}")
    print()

sweep = data.get("shardSweep")
if sweep:
    print("SHARDING")
    for point in sweep["points"]:
        print(
            f"  {point['shardCount']:>3} shards   "
            f"{point['reservationThroughput']:>8.1f} alloc/s   "
            f"{point['speedupVsSingleRow']:>5.2f}x   "
            f"p99 {point['run']['checkoutP99MS']:>7.1f} ms"
        )
    print(f"  best speedup                   {sweep['bestSpeedup']:.2f}x")
    print(f"  plateau at                     {sweep.get('plateauAtShards')} shards")
    print()

controller = data.get("controller")
if controller:
    print("ADMISSION CONTROL (burst)")
    for arm in controller["burstArms"]:
        print(
            f"  {arm['name']:<20} p99 {arm['run']['checkoutP99MS']:>8.1f} ms   "
            f"completion {arm['run']['completionRate']*100:>5.1f}%   "
            f"errors {arm['run']['errorRate']*100:>5.2f}%   "
            f"drain {arm['timeToDrainSeconds']:>5.1f} s"
        )
    best = min(controller["burstArms"], key=lambda a: a["run"]["checkoutP99MS"])
    worst = max(controller["burstArms"], key=lambda a: a["run"]["checkoutP99MS"])
    if best["run"]["checkoutP99MS"] > 0:
        ratio = worst["run"]["checkoutP99MS"] / best["run"]["checkoutP99MS"]
        print(
            f"  -> {worst['name']} {worst['run']['checkoutP99MS']:.0f} ms vs "
            f"{best['name']} {best['run']['checkoutP99MS']:.0f} ms ({ratio:.1f}x)"
        )
    print()
    print("ADMISSION CONTROL (steady — where the controller should lose)")
    for arm in controller["steadyArms"]:
        print(
            f"  {arm['name']:<20} p99 {arm['run']['checkoutP99MS']:>8.1f} ms   "
            f"drain {arm['timeToDrainSeconds']:>5.1f} s"
        )
    print()

fairness = data.get("fairness")
if fairness:
    print("FAIRNESS")
    for arm in fairness["arms"]:
        report = arm.get("report") or {}
        print(
            f"  {arm['profile']:<16} {arm['policy']:<14} "
            f"correlation {report.get('rankCorrelation', 0):>7.4f}   "
            f"admit stmt {arm['admitMeanMS']:>6.1f} ms mean / {arm['admitP99MS']:>6.1f} p99   "
            f"wait mean {report.get('waitMean', 0):>5.2f} s"
        )
    print(f"  retries that changed a position {fairness['retriesEverImprovedPosition']}")
    print()

horizon = data.get("commitHorizon")
if horizon:
    print("FIFO COMMIT HORIZON")
    for point in horizon["points"]:
        print(
            f"  {point['horizonMS']:>6.0f} ms   "
            f"{point['fifoInversions']:>5} inversions of {point['admissionsChecked']:<6} "
            f"({point['inversionRate']*100:>5.2f}%)   "
            f"correlation {point['rankCorrelation']:.5f}   "
            f"mean wait {point['meanWaitSeconds']:.2f} s"
        )
    print()

tail = data.get("tailFragmentation")
if tail:
    print("TAIL FRAGMENTATION")
    for arm in (tail.get("withoutCollapse"), tail.get("withCollapse")):
        if not arm:
            continue
        label = "collapse on " if arm["collapseEnabled"] else "collapse off"
        print(
            f"  {label}   {arm['fragmentedFailures']:>4} of {arm['requestsInWave']:<4} failed "
            f"({arm['fragmentedFailureRate']*100:>5.1f}%)   "
            f"stock on {arm['shardsHoldingStockBeforeWave']}/{tail['shardCount']} shards"
        )
    print()

idem = data.get("idempotency")
if idem:
    print("IDEMPOTENCY")
    print(f"  customers (each sent 2 concurrent) {idem['customers']}")
    print(f"  orders                             {idem['purchases']}")
    print(f"  distinct order ids                 {idem['distinctOrderIDs']}")
    print(f"  duplicate orders                   {idem['duplicateOrders']}")
    print(f"  responses replayed                 {idem['replayedResponses']}")
    print()
PY
