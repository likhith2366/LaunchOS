# LaunchOS — Project Spec

Oct 21, 2026 · @Likhith

## What LaunchOS is

The infrastructure behind a product launch. A hundred thousand people arrive in the same ten seconds for a thousand units. The system decides who gets in, in what order, who gets inventory, and stays correct when things break.

Storing orders is not the problem. The problems are: admitting people at a rate the backend can actually survive, allocating scarce inventory without ever selling the same unit twice, and keeping both correct when a request succeeds but its response is lost, or a worker dies mid-checkout, or a customer abandons a reservation and the units have to come back.

**What makes it an engineering project rather than a commerce backend.** A commerce backend would put inventory in a row and update it. That works until ten thousand requests a second hit the same row, at which point Postgres serializes every one of them behind a single lock and your p99 collapses. The interesting work is everything you do about that: sharding the inventory, controlling admission so the backend never sees more load than it can take, and defining fairness precisely enough to measure whether your queue is actually fair.

**The differentiator is the adaptive admission controller.** Anyone can admit 500 customers a second. The project is a controller that watches downstream latency and error rate and adjusts admission to whatever the system can currently sustain — then proves, against a fixed-rate baseline, that it protects p99 and completion rate during a spike. That comparison is the result. Build the baseline first so you have something to beat.

**Why it suits you.** You have done deterministic failure injection twice (Riptide, Conductor) and careful A/B benchmarking with negative results. This project is mostly those two skills pointed at a scheduling problem. And for Apple, launch day is a problem they genuinely have.

## The four engines

**1. Fair Admission Scheduler.** Customers join a queue, get a position, and are admitted when capacity allows. The hard parts: a retry must not let someone jump the queue, queue order has to survive a restart (which means it cannot live only in Redis), and admitting a batch of 500 at once creates a thundering herd at checkout — so admissions need smoothing or jitter rather than releasing in lockstep.

**2. Concurrent Inventory Allocator.** Thousands of admitted customers compete for the last few units. The invariant is `available + reserved + sold = total`, and it must hold after every committed transaction including expiry and cancellation. Reservations expire, and expired inventory returns to the pool. The performance problem here is the whole point of the project and gets its own section below.

**3. Fault-Tolerant Checkout Coordinator.** A request succeeds and the response is lost. The customer retries. You return the original result rather than creating a second order. That means idempotency keys with conditional writes, durable state transitions, and recovery when a worker dies between reserving inventory and recording the order — the window where a crash silently loses a unit forever if you get it wrong.

**4. Launch Traffic Simulator.** Not an afterthought; it is where your numbers come from. Bursty arrivals, retries, abandoned sessions, duplicate requests, network delay, killed workers. Make it **seeded and deterministic** so any failure replays exactly — the same technique you used in Riptide and Conductor, and the thing that makes the benchmark trustworthy rather than anecdotal.

**One addition the draft is missing: more than one node.** If a single Swift process owns all allocation, the distributed systems claim is thin. Run two or three allocator nodes that must agree on inventory without double-allocating. This is the single biggest step up in difficulty available, and it is where your Raft and lease work actually transfers. Section 7 covers how.

## Architecture

&#91;embedded content: request path · 7 components, two feedback loops\]

Two loops matter more than the straight path. Checkout latency feeds back into admission, so the controller throttles itself before the backend falls over rather than after. And the expiry sweeper returns abandoned units to the allocator, which is what stops a launch from appearing sold out while a thousand people sit on reservations they never completed.

## The hot row, and inventory sharding

This is the optimization the draft is missing, and it should be the centrepiece of your performance work.

**The problem.** The obvious allocator is one row per SKU and a conditional update:

```sql
UPDATE inventory SET available = available - 1, reserved = reserved + 1
WHERE sku = 'LAUNCH-001' AND available > 0 RETURNING available;
```

That is correct. It is also a single row that every request in the system must take a lock on, one at a time. At 10,000 requests a second against one row, Postgres serializes all of them, and your throughput ceiling is `1 / row-lock-hold-time` no matter how many cores or connections you add. Everything else you build is irrelevant if you leave this in place.

**The fix: shard the inventory.** Split 1,000 units across N buckets of 1,000/N each. A request picks a bucket at random and takes its lock. Contention drops by roughly N, because requests now spread across N independent rows instead of queueing behind one.

**And the trade-off, which is the interesting part.** Sharding creates **tail fragmentation**. When 20 units remain spread across 16 buckets, a request that lands on an empty bucket fails even though stock exists. Naive retry turns the end of the launch into a storm. The fixes, in increasing order of sophistication:

1. On an empty bucket, retry against a different bucket rather than failing — bounded, with a deterministic probe order so it terminates.
2. Below a threshold of remaining units, **collapse the shards** back into one row. Contention stops mattering once there are only 20 units left, because almost every request is going to fail anyway.
3. Rebalance periodically, moving units from full buckets to empty ones.

**What to measure.** Throughput and p99 at shard counts of 1, 4, 16 and 64, under identical load. Expect throughput to climb and then flatten once the bottleneck moves somewhere else — and **find out where it moved**, because "sharding helped until X, at which point the limit became Y" is a far better answer than a ratio.

Also measure the failure rate in the last 5% of inventory, with and without shard collapse. That is where the trade-off becomes visible and where the second measurement earns its place.

**Why this matters for you specifically.** It is the same shape as the fsync batching cap you found in Conductor: an artificial ceiling, identified by reasoning about the mechanism rather than guessing, then removed, with the new ceiling named. Doing that twice in two projects is a pattern an interviewer will notice.

## The adaptive admission controller

The differentiator, and the part that is a control problem rather than a database problem.

**Build the baseline first.** A fixed-rate admitter: let in R customers per second, R configured by hand. You cannot claim an adaptive controller is better without a number to beat, and the baseline takes an afternoon.

**Then the controller.** Additive increase, multiplicative decrease, driven by downstream health:

- While checkout p99 and error rate stay under threshold, raise the admission rate by a fixed step each interval.
- When p99 crosses the threshold, cut the rate by a factor — half is the usual starting point.
- Enforce a hard ceiling on in-flight checkout sessions regardless of what the controller wants.

**Three things to get right, each worth a paragraph in your writeup:**

**Oscillation.** A controller that reacts to every sample will swing between full-open and nearly-closed, and the admission graph will look like a saw. Smooth the signal with an exponentially weighted moving average and leave a dead band around the threshold where nothing changes.

**The metric you control on.** p99 latency is laggy — by the time it moves you are already in trouble. **Queue depth or in-flight count responds faster**, because it rises the moment service time increases. Controlling on a leading indicator and alarming on a lagging one is the standard answer and it is worth being able to explain why.

**Thundering herd.** Admitting a batch of 500 at an interval boundary means 500 simultaneous checkout attempts. Release them with jitter spread across the interval instead.

**The experiment.** Drive the same burst profile through both admitters and compare: checkout p99, completion rate, error rate, and time to drain the queue. The expected result is that the fixed-rate admitter either under-utilizes (set too low) or collapses under the spike (set too high), while the adaptive one holds p99 and finishes the queue faster than a conservative fixed rate.

**And measure the case where it loses.** Under a perfectly steady load, a well-tuned fixed rate should match or beat the controller, because the controller is still probing upward and backing off. Reporting that is what makes the rest credible.

## Fairness, defined

The draft says "fairness policy" and "fairness deviation" without defining either. Define it numerically or it is not a metric, and an interviewer will ask what you meant.

**Pick a definition.** The simplest defensible one: **Spearman rank correlation between arrival order and admission order**. Perfect FIFO is 1.0. Pure random admission is around 0. You now have a single number that says how fair a run was, and you can compare policies on it.

**Compare at least three policies and report the number for each:**

| Policy | How it works | Where it fails |
| --- | --- | --- |
| **Strict FIFO** | Admit in exact arrival order | Needs durable global ordering, which costs throughput; one slow admission blocks the line |
| **Lottery** | Random selection from the waiting pool | Trivially scalable, no ordering cost, but someone who arrived first can wait forever |
| **Bucketed FIFO** | Order within time buckets, random within a bucket | Near-FIFO fairness at a fraction of the coordination cost |

**The result worth producing is the trade-off curve.** Strict FIFO costs throughput because it needs a globally ordered, durable queue. Lottery is nearly free. Bucketed sits between. Plot fairness against throughput across the three and you have a finding rather than an implementation — something like "bucketed FIFO retains 0.94 rank correlation at 3× the admission throughput of strict FIFO."

**Also measure wait time, not just order.** Fair ordering with unbounded waits is not fair in any sense a customer recognizes. Report the distribution of time-in-queue, and check it against Little's Law — expected wait equals queue length divided by admission rate. Predicting a number from theory and then confirming it with a measurement is a strong thing to have in a writeup.

**Retries must not improve your position.** A customer who refreshes twenty times should hold exactly the position they got on arrival. This is the detail that separates a real waiting room from a demo, and the simulator should actively try to break it.

## Failure handling

**Idempotency.** Every checkout carries a client-generated key. The first request writes a record keyed by it in the same transaction as the order; a retry finds that record and returns the original result. The subtlety is the in-flight case — a retry arriving while the first request is still running. Either block briefly on the key or return a "still processing" response; silently creating a second order is the failure you are preventing.

**The crash window.** Between reserving inventory and recording the order there is a gap where a worker death loses a unit. It is reserved, so it is not available, but no order exists to complete it. Reservation expiry eventually returns it, which is the safety net — but the expiry has to be durable and the reservation has to carry enough state for recovery to tell "abandoned" from "in progress."

**Expiry at scale.** Scanning the reservation table for expired rows does not survive contact with a hundred thousand reservations. Index on expiry time and sweep in bounded batches, or bucket reservations by expiry minute so the sweeper reads one partition per tick. This is the same timer-density problem you solved in Conductor, and the same answer applies.

**Durable queue order.** Redis alone cannot give FIFO across a restart — the draft is right about that. Options, cheapest first: append queue joins to Postgres and keep Redis as a read cache; or use a Redis stream with AOF persistence and accept the durability gap; or write an append-only queue log yourself. The first is the right call for a six-week project. Whichever you pick, **test it by killing Redis mid-launch and asserting that queue order survives**, because that demo is worth more than the implementation detail.

**Multi-node allocation — the ambitious addition.** Run two or three allocator processes. Options:

1. **Postgres as the arbiter.** All nodes do conditional updates against the same sharded rows. Simple, correct, and the database is the bottleneck.
2. **Lease-based shard ownership.** Each node leases a subset of shards and allocates from them without touching the others. Far less database contention; when a node dies its lease expires and its shards are reclaimed. This uses the fencing-token work you already did in Conductor.
3. **Lease-based with rebalancing.** As above, plus shards migrating between nodes when distribution goes lopsided.

Build option 1 first so you have a correct baseline, then option 2 and measure the difference. "Lease-based shard ownership removed N% of database contention at the cost of M seconds of reclaim latency after a node failure" is a genuine distributed systems result.

## Stack and running it on Windows

| Layer | Choice | Why |
| --- | --- | --- |
| Language | Swift 6 | The point, for Apple. Actors give you per-shard isolation the compiler enforces. |
| Web | Vapor on SwiftNIO | Mature, async, and what Apple used for Password Monitoring. |
| Durable state | PostgreSQL | Inventory, reservations, orders, idempotency keys, queue log. Source of truth. |
| Coordination | Redis | Queue positions, admission leases, in-flight counters. Fast, and explicitly not authoritative. |
| Observability | Prometheus and Grafana | Admission rate, queue depth, p99, oversell count. Also closes a real gap on your resume. |
| Load and faults | Swift CLI | The simulator. Seeded, deterministic, replayable. |
| Dashboard | React, TypeScript | Operator view: inventory, queue depth, admission rate, checkout health. |
| Runtime | Docker Compose | Multiple allocator nodes, Postgres, Redis, Prometheus, all local. |

**Windows setup.** Do not build this natively on Windows — Vapor targets Linux and macOS and Foundation on Windows is incomplete.

1. **WSL2 with Ubuntu**, Swift 6 toolchain inside it, VS Code attached over the WSL remote. This is your inner loop.
2. **Docker Compose** for Postgres, Redis, Prometheus, Grafana, and for running several allocator nodes at once.

On 16 GB of RAM, run the simulator in the same WSL instance and keep the service count modest — two or three allocator nodes is plenty to make the point.

**A credibility note worth keeping from the draft.** 100,000 simulated customers is not 100,000 simultaneous TCP connections, and claiming otherwise on a laptop will not survive questioning. Report arrival rate, peak concurrent connections, and completed purchases as three separate numbers. Stating the limitation yourself is what makes the rest of the numbers believable.

## Build plan

Six weeks. The order is chosen so that every week ends with something demonstrable, and so the baseline exists before the thing that beats it.

**Week 1 — the correct, slow version.** Vapor service, Postgres schema, launches, inventory, reservations, orders. One inventory row per SKU with the conditional update. No queue, no controller, no sharding. By the end of the week, a concurrency test that fires 1,000 simultaneous reservations at 100 units and asserts exactly 100 succeed. Slow is fine. **Correct is the baseline for everything after.**

**Week 2 — the simulator.** Build it early, not late. Seeded, deterministic, replayable: bursty arrivals, retries, abandonment, duplicate requests, configurable delay. Then measure week 1's implementation with it and watch the hot row serialize everything. That measurement is where the rest of the project comes from.

**Week 3 — the waiting room, fixed-rate.** Durable queue order in Postgres, Redis for positions, admission passes, retry-safe positions. Fixed-rate admission only. Measure fairness as rank correlation and queue wait distribution. You now have the baseline to beat.

**Week 4 — sharding and the adaptive controller.** The two performance pieces. Shard inventory and sweep shard counts of 1, 4, 16, 64. Build the AIMD controller and A/B it against fixed-rate under the same burst. This is the week that produces your headline numbers.

**Week 5 — failures and multiple nodes.** Idempotency keys, durable checkout transitions, expiry sweeper, crash recovery. Then a second and third allocator node, Postgres-arbitrated first, lease-based second, with the contention difference measured.

**Week 6 — observability, dashboard, writeup.** Prometheus and Grafana, a small React operator view, the full measurement suite re-run cleanly, and a README with the architecture, the results, and what you rejected.

**If you only have three weeks:** 1, 2 and 4. Correct allocator, honest simulator, and the two optimizations with before-and-after numbers. Skip the queue, the dashboard, and multi-node. A correct system with two measured optimizations beats a broad one you cannot prove.

## What to measure

| Measurement | How | What it proves |
| --- | --- | --- |
| Oversold units | Full launch simulation, repeated; assert `available + reserved + sold = total` after every commit | Correctness. Must be zero, always |
| Reservation throughput by shard count | Identical load at 1, 4, 16, 64 shards | The optimization worked, and where the next ceiling is |
| Checkout p99 under burst, adaptive vs fixed | Same burst profile through both admitters | The controller earns its complexity |
| Completion rate under burst | Percentage of admitted customers who finish checkout | The controller protects the user, not just the server |
| Fairness as rank correlation | Spearman between arrival and admission order, per policy | Your queue is as fair as you claim |
| Duplicate orders per idempotency key | Simulator sends duplicates deliberately | Must be zero |
| Recovery after node kill | Kill an allocator mid-launch; time until reservations are reclaimed and inventory is consistent | Failure is bounded |
| Failure rate in the last 5% of inventory | With and without shard collapse | The sharding trade-off, honestly reported |

**Three for the resume:** zero overselling under concurrent load, the sharding throughput gain, and the adaptive-vs-fixed p99 comparison. One correctness claim, one optimization, one controlled experiment.

**Same rules as Conductor.** State the hardware and the load profile every time. Never publish a number you have not run. Report the cases where your change lost — the steady-load case where fixed-rate beats the controller, and the tail fragmentation sharding introduces. Those two admissions are what make the other numbers believable.

## Resume entry and interview material

Fill the placeholders once measured. Not before.

```latex
\noindent
\textbf{LaunchOS -- High-Concurrency Product Launch Platform}
\textit{(Swift 6, Vapor, PostgreSQL, Redis, Prometheus, Docker)}
\begin{itemize}

    \item Built a \textbf{launch admission and inventory platform} in Swift 6
          that held \textbf{zero oversold units} across \textbf{N simulated
          launches} of 100K customers against limited stock.

    \item Raised reservation throughput \textbf{X$\times$} by sharding
          inventory across N buckets, removing single-row lock contention,
          with shard collapse to avoid tail allocation failures.

    \item Cut checkout \textbf{p99 from Y to Z ms} under burst load with an
          adaptive AIMD admission controller, measured against a fixed-rate
          baseline on identical traffic.

\end{itemize}
```

**Questions this lets you answer well:**

- How do you stop overselling? Not "a transaction" — the conditional update, why a read-then-write races, and why the single row then becomes the bottleneck.
- What was your bottleneck and how did you find it? Row lock contention, found by reasoning about where requests serialize, then confirmed by the shard sweep.
- What did sharding cost you? Tail fragmentation, and what you did about it. Volunteering the downside is the strongest move available here.
- How do you know your queue is fair? A number, not an adjective.
- When is the adaptive controller worse than fixed rate? Under steady load. Having this answer ready is worth more than the headline.

**Keep a decisions log from week one.** Shard count and why. The metric the controller watches and why not p99. Which fairness policy and what it cost. These become interview answers you will not have to invent under pressure.

**One warning.** This project touches payments-adjacent territory, and you already have PayFlow. On a resume they must tell different stories — PayFlow is transaction processing and fraud, LaunchOS is admission control and scarce-resource allocation. If both bullets end up saying "handled high concurrency with low p99," cut one.
