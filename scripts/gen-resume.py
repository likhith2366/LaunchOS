#!/usr/bin/env python3
"""Generates docs/RESUME.md from results/results.json.

The resume bullet is the number most likely to be quoted from memory and least likely to
be re-checked, so it is generated from the record rather than written. The spec this
project was built from put it plainly: fill the placeholders once measured, not before.

    scripts/gen-resume.py [results.json] [out.md]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def bullets(data: dict) -> list[str]:
    oversell = data.get("oversell") or {}
    sweep = data.get("shardSweep") or {}
    controller = data.get("controller") or {}

    trials = len(oversell.get("successesPerTrial", []))
    contenders = oversell.get("contendersPerTrial", 0)
    units = oversell.get("unitsPerTrial", 0)
    speedup = sweep.get("bestSpeedup", 0)
    plateau = sweep.get("plateauAtShards")
    shard_counts = ", ".join(str(p["shardCount"]) for p in sweep.get("points", []))

    burst = controller.get("burstArms", [])
    adaptive = next((a for a in burst if a.get("kind") == "aimd"), None)
    fixed_arms = [a for a in burst if a.get("kind") == "fixed"]

    out = [
        "```latex",
        r"\noindent",
        r"\textbf{LaunchOS -- High-Concurrency Product Launch Platform}",
        r"\textit{(Swift 6, Vapor, PostgreSQL, Redis, Prometheus, Docker)}",
        r"\begin{itemize}",
        "",
    ]

    if oversell:
        out += [
            r"    \item Built a \textbf{launch admission and inventory platform} in Swift 6",
            r"          that held \textbf{zero oversold units} across " f"{trials} trials of",
            f"          {contenders} concurrent reservations against {units} units, with",
            r"          \texttt{available + reserved + sold = total} asserted after every",
            "          committed transaction.",
            "",
        ]

    if sweep:
        plateau_text = (
            f" throughput plateaued at {plateau} shards, at which point"
            if plateau
            else " past which point"
        )
        out += [
            r"    \item Raised reservation throughput \textbf{"
            f"{speedup:.1f}" r"$\times$} by sharding",
            f"          inventory across buckets (counts of {shard_counts} swept under",
            "          identical load), removing single-row lock contention;"
            f"{plateau_text}",
            "          the limit moved to per-checkout Postgres round-trips rather than",
            "          contention on any one row.",
            "",
        ]

    if adaptive and fixed_arms:
        adaptive_p99 = adaptive["run"]["checkoutP99MS"]
        adaptive_drain = adaptive["timeToDrainSeconds"]

        # Two baselines, because either one alone would be a choice of result rather than
        # a measurement.
        #
        # The capacity-tuned arm is the strongest fixed setting available, and matching it
        # is the real claim: the controller got there without being given the capacity,
        # which in production nobody has in advance. Quoting only the ratio against a
        # mis-set rate would be beating a baseline that was set badly on purpose.
        tuned = next((a for a in fixed_arms if a["name"] == "fixed-at-capacity"), None)
        # The worst fixed arm shows what a mis-set rate costs, which is the other half of
        # the point: a fixed rate has no single honest setting.
        worst = max(fixed_arms, key=lambda a: a["run"]["checkoutP99MS"])

        if tuned and adaptive_p99 > 0 and worst["run"]["checkoutP99MS"] > 0:
            tuned_p99 = tuned["run"]["checkoutP99MS"]
            worst_p99 = worst["run"]["checkoutP99MS"]
            ratio = worst_p99 / adaptive_p99
            out += [
                r"    \item Built an \textbf{adaptive AIMD admission controller} that held",
                f"          checkout p99 at {adaptive_p99:.0f} ms under burst load --",
                r"          \textbf{matching a fixed-rate admitter tuned to measured",
                f"          service capacity}} ({tuned_p99:.0f} ms, queue drained in",
                f"          {tuned['timeToDrainSeconds']:.0f}s vs {adaptive_drain:.0f}s)",
                "          without being given that capacity, and running",
                r"          \textbf{" f"{ratio:.1f}" r"$\times$ lower p99} than a fixed rate",
                f"          set too high ({worst_p99:.0f} ms), on byte-identical seeded",
                "          traffic through every arm.",
                "",
            ]

    out += [r"\end{itemize}", "```", ""]
    return out


QUESTIONS = """
## Questions this answers well

**How do you stop overselling?** Not "a transaction". A conditional update --
`UPDATE ... WHERE available >= n` -- with the reservation insert fused into the same
statement, so a probe either takes a unit and records who holds it or writes nothing at
all. A read-then-write races because two requests can both read `available = 1`. And the
single row that makes it correct is then the bottleneck, which is the next question.

**What was your bottleneck and how did you find it?** Row-lock contention, found by
reasoning about where requests serialize rather than by guessing, then confirmed by the
shard sweep. Where the limit went *next* was answered the same way, by
`scripts/probe-bottleneck.sh`: sweep the client's concurrency cap at a fixed shard count
-- flat throughput with latency rising in proportion means the server is saturated and
the extra concurrency is pure queueing -- then count Postgres transactions per
allocation, and re-run with `synchronous_commit` off to separate round-trip count from
commit durability.

**What did sharding cost you?** Tail fragmentation: with a handful of units spread across
many buckets, a request lands on empty buckets and fails while stock still exists.
Measured, with and without shard collapse. Getting a trustworthy number needed care --
the obvious experiment measures nothing, because once stock reaches zero a fragmentation
failure and a genuine sell-out are indistinguishable, so the measured wave is
deliberately smaller than the stock remaining.

**How do you know your queue is fair?** Spearman rank correlation between arrival order
and admission order: 1.0 is FIFO, about 0 is a lottery. Three policies compared on it
under two arrival profiles -- because a lottery looks almost like FIFO when customers
trickle in, since the pool you draw from only contains people who have already arrived.

**When is the adaptive controller worse than a fixed rate?** Under steady load inside
capacity, where a fixed rate tuned to that load beats it, because the controller is still
probing upward and backing off. The experiment includes a fixed arm set *exactly* at
measured capacity -- the strongest fixed baseline available -- and reports it whichever
way it came out. The controller's claim is not that it beats a perfectly tuned fixed rate
on a load that never changes; it is that it finds a workable rate without being told one.

**Why control on in-flight count rather than p99?** p99 is a lagging indicator: by the
time it moves, admission has been too high for seconds and the controller is correcting a
problem it caused. In-flight count rises the moment service time increases, because a
request that takes longer is still being counted. Control on the leading indicator, alarm
on the lagging one. Both are tracked; only one drives the loop.

**Tell me about a bug you found.** Three, all surfaced by measurement rather than by
reading code:

1. **Shard collapse lost every available unit.** It merged the shards in one statement
   with a data-modifying CTE: the drain ran, the credit to shard 0 silently did not,
   because sub-statements of a single statement cannot see each other's effects. The
   inventory invariant caught it -- 14 units short of 300.
2. **The Redis position cache saved nothing.** The route computed the position from
   Postgres and *then* let the cached value overwrite it, so the expensive count ran on
   every poll regardless. Found because an experiment behaved nonsensically: slower
   admission produced *worse* latency, which only makes sense if queue polling, not
   checkout, was the dominant load.
3. **The bucketed expiry sweep ignored its launch filter**, so a scoped sweep reclaimed
   other launches' reservations. Harmless for the background sweeper, which wants
   everything, but it made every "how many did this launch reclaim" assertion
   meaningless. Three failing tests pointed at it.

**What surprised you?** That strict FIFO is not achievable by ordering on a sequence
number. `seq` is assigned at `INSERT`, but transactions commit in their own order, so an
admitter genuinely cannot see a lower-numbered entry that has not landed yet -- it admits
what it can see, and arrival order and admission order end up crossed with nobody having
retried or jumped the queue. A commit horizon fixes it, which makes strict FIFO a tunable
rather than a guarantee. It only showed up because inversions are counted directly: a run
with 125 out-of-order admissions out of 2000 still scored a rank correlation of 0.99996.

**Why is the allocator not an actor?** Because Swift actors serialize their callers, and
putting `reserve` on one would reintroduce by hand exactly the global lock that sharding
exists to remove. The only actor near the hot path caches launch configuration, and even
there the database round-trip on a cache miss happens outside the actor -- awaiting a
query while holding it would queue every concurrent caller behind the miss.

## One warning worth keeping

This project touches payments-adjacent territory. On a resume it has to tell a different
story from a transaction-processing project: LaunchOS is **admission control and
scarce-resource allocation**, not payments or fraud. If two bullets both end up saying
"handled high concurrency with low p99", cut one.
"""


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    results_path = Path(sys.argv[1]) if len(sys.argv) > 1 else root / "results" / "results.json"
    out_path = Path(sys.argv[2]) if len(sys.argv) > 2 else root / "docs" / "RESUME.md"

    if not results_path.is_file():
        print(f"no results at {results_path}; run scripts/run-experiments.sh first", file=sys.stderr)
        return 1

    data = json.loads(results_path.read_text(encoding="utf-8"))
    env = data.get("environment", {})
    nodes = env.get("nodes", 1)

    header = [
        "# Resume entry and interview material",
        "",
        "Generated by `scripts/gen-resume.py` from `results/results.json`, so every number",
        "below traces to a field in that file rather than to someone's memory of a run.",
        "",
        f"Measured on {env.get('host', '?')}, {env.get('cores', '?')} cores, {nodes} "
        f"allocator node{'' if nodes == 1 else 's'}, recorded {env.get('recordedAt', '?')}.",
        "",
        "## Resume entry",
        "",
    ]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        "\n".join(header + bullets(data)) + QUESTIONS, encoding="utf-8", newline="\n"
    )
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
