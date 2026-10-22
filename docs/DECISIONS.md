# Decisions log

Kept from the first week, because these are the questions an interviewer asks and the
answers are much better written down at the time than reconstructed afterwards. Each
entry is a decision, the alternative it beat, and what it cost.

---

## 1. Inventory is sharded across rows, not held in one

**Decision.** A launch's units are split across `shardCount` rows of
`inventory_shards`, and a request picks a starting shard from a stable hash of its
customer id and probes forward.

**The alternative.** One row per SKU with a conditional update:

```sql
UPDATE inventory SET available = available - 1, reserved = reserved + 1
WHERE sku = 'LAUNCH-001' AND available > 0;
```

That is correct, and it is also a single row every request in the system must take a
lock on, one at a time. The throughput ceiling is `1 / row-lock-hold-time` no matter how
many cores or connections exist. The measured speedup and the shard count it plateaus at
are in [`../results/RESULTS.md`](../results/RESULTS.md); they are not quoted here,
because a number copied into prose is a number that goes stale silently.

**Cost.** Tail fragmentation, which gets its own entry below. Also a more complicated
release path, because a unit does not always go back to the shard it came from.

**Shard count.** The sweep (1, 4, 16, 64) plateaus at 16 on this hardware and is
slightly *worse* at 64. 16 is the default. The right number is not universal: it is
wherever the next bottleneck starts to dominate, which is why the sweep is a script and
not a one-off.

---

## 2. The probe sequence is deterministic per customer

**Decision.** `start + i * step (mod shardCount)`, with both derived from a stable FNV
hash of the customer id and `step` forced coprime to the shard count.

**Why coprime.** It makes the sequence a full cover: every shard is visited exactly once,
so a bounded sweep never wastes a probe on a shard it has already tried, and the
sequence terminates.

**Why deterministic per customer.** A retrying customer probes the same shards in the
same order. Without that, a retry could succeed where the first attempt failed purely
by landing somewhere else, which is a position improvement by another name — and it
would make a failed simulator run unreproducible.

**Why not Swift's `Hashable`.** It is seeded per process. Two nodes would disagree about
which shard a customer belongs to, and no run would replay.

---

## 3. Shard collapse below a threshold, triggered by demand

**Decision.** When an allocation fails every shard it was willing to try and stock still
remains below `collapseThreshold`, the shards are merged into shard 0 and the launch is
marked collapsed.

**Why.** Contention stops mattering once only a few units are left, because nearly every
request is going to fail anyway. Trading parallelism for a guarantee that a request never
fails while stock exists is the right way round at that point. The measured effect on
failures in the last 5% of inventory, with and without collapse, is in
[`../results/RESULTS.md`](../results/RESULTS.md) — not quoted here, because a number
copied into prose is a number that goes stale silently.

**Why demand-triggered rather than a background task.** No timer to tune, and it fires
exactly when it matters. The cost is that the first few failures are what trigger it, so
a handful of requests still see fragmentation before the merge completes, and that is
most of what the collapse arm still reports as failures rather than a shortcoming of the
merge itself.

**What it cost to get right.** The first implementation did the drain and the credit to
shard 0 in a single statement with data-modifying CTEs. The drain ran; the credit
silently did not; every unit still available at collapse time disappeared. Sub-statements
of one statement cannot see each other's effects, so arithmetic that has to read what
another part just wrote does not belong in one statement. It is now three explicit steps
under one transaction, with every shard row locked in ascending id order first.

---

## 4. The controller acts on in-flight count, alarms on p99

**Decision.** AIMD on the maximum of two pressure ratios, in-flight against its threshold
and smoothed latency against its own. In practice in-flight is what moves first.

**Why not p99.** It is a lagging indicator: by the time it moves, admission has already
been too high for seconds, and a controller that reacts then is always correcting a
problem it caused. In-flight count rises the moment service time increases, because a
request that takes longer is still being counted.

**Why both are tracked.** Control on the leading indicator, alarm on the lagging one.
p99 is what a human should be paged about; it is not what the loop should chase.

**Smoothing and the dead band.** An EWMA (α = 0.2) on both signals, plus a ±15% dead band
around the threshold in which the rate is not changed at all. Without the dead band the
controller reacts to every sample and the admission graph is a sawtooth.

**The hard ceiling is not part of the control law.** A separate absolute cap on in-flight
sessions, enforced whatever the loop concluded. The loop is feedback and can be wrong;
the ceiling is the thing that is not allowed to be. It is deliberately set out of the way
in the A/B experiment, because if the ceiling were what held p99 down then the
comparison would be measuring the ceiling and not AIMD.

---

## 5. Fairness is Spearman rank correlation between arrival and admission order

**Decision.** One number per run. Perfect FIFO is 1.0; a lottery over the whole
population is near 0.

**Why a number.** "Fairness policy" and "fairness deviation" are not metrics. This is
one, it is comparable across policies, and it can be argued with.

**The tie-aware form, not the shortcut.** The `1 - 6Σd²/n(n²-1)` form is only valid
without ties, and bucketed FIFO produces ties by construction. The implementation is
Pearson on average ranks.

**What it is bad at, and the fix.** Rank correlation is dominated by the bulk of the
distribution and is almost blind to a handful of inversions: the run described in entry 6,
with 125 genuinely out-of-order admissions out of 2000, still scored 0.99996. Inversions
are therefore *counted* as well as correlated. Had the project reported only the
correlation, the ordering bug in entry 6 would never have been found — which is the single
strongest argument in here for picking metrics that can actually fail.

---

## 6. Strict FIFO needs a commit horizon, and that makes it a tunable

**Decision.** Admission only considers queue entries older than a commit horizon
(250 ms by default).

**Why.** `seq` comes from a Postgres sequence and is assigned at `INSERT`, but
transactions commit in their own order. An admitter polling the waiting set genuinely
cannot see a lower-numbered entry that has not committed yet, so it admits what it can
see, and arrival order and admission order end up crossed — with nobody having retried
or jumped the queue. Ordering on a pre-commit sequence number is not sufficient for
FIFO, and this was the single most surprising thing the project turned up.

**Measured.** The sweep of inversions against horizon is in the results file. One run
worth recording separately, because it is what exposed the mechanism: 2000 customers
arriving at once produced **125 inversions (6.25%) with no horizon and zero at 100 ms and
above**. The effect is load-dependent — it needs an admit tick to land inside the window
where a lower-numbered join has taken its sequence number but not yet committed — so a
later run with no inversions at a zero horizon does not mean the horizon is unnecessary.
The justification is the mechanism; the sizing is whichever runs exhibit it.

**The honest framing.** The horizon has to exceed how long a join takes to *commit*, not
how far apart arrivals are, so the correct setting depends on load. That makes strict
FIFO a tunable rather than a guarantee, and the sweep is what justifies the number
instead of asserting it.

**The alternative, rejected.** Allocating the sequence under a row lock so that sequence
order equals commit order. That is a true guarantee, and it serializes every queue join —
which is precisely the "strict FIFO costs throughput" trade-off, paid in full. The
horizon buys almost all of the ordering for a bounded, uniform delay instead.

---

## 7. Queue order lives in Postgres; Redis is a cache and never authoritative

**Decision.** Arrival order is a Postgres sequence on `queue_entries`. Redis caches
positions and holds advisory counters.

**Why.** Redis alone cannot give FIFO across a restart. The cheapest durable option that
actually works is appending joins to Postgres and treating Redis as a read cache, which
is also the right call for a six-week project.

**The property that falls out for free.** `ON CONFLICT (launch_id, customer_id) DO UPDATE`
touching only a retry counter means a retry reads back the row the first join created.
A customer who refreshes twenty times holds the position they got on arrival, enforced by
a primary key rather than by application logic.

**A bug worth recording.** The position cache initially saved nothing: the route computed
the position from Postgres and *then* let the cached value overwrite it. The expensive
query ran every time. Position is now a separate call made only on a cache miss — which
matters because "where am I in line?" is by far the most frequent request in a launch.

---

## 8. Idempotency: a conditional insert decides ownership, and the response is committed with the order

**Decision.** `INSERT ... ON CONFLICT DO NOTHING RETURNING` — exactly one concurrent
caller gets a row back and therefore owns the work. The order id is generated *before*
the transaction so the response body can be written in the same commit as the order.

**Why that ordering matters.** There is no instant at which the order exists but the
record a retry would read from does not. Writing the response afterwards would leave a
window in which a crash produces an order nobody can ever be told about.

**The in-flight case.** A retry arriving while the original is still running blocks
briefly (250 ms) and then returns 409. Blocking absorbs the common case — a client
retrying after a few hundred milliseconds — without holding a connection open for the
pathological one. Silently creating a second order is the failure being prevented;
either answer is acceptable, a duplicate order is not.

**Sold out is terminal and is stored.** A retry gets the same "sold out" rather than a
fresh attempt that might succeed off the back of someone else's cancellation.

**Keys are global, not per launch.** The primary key is the key alone, because a key
identifies one request across the whole system.

---

## 9. Expiry is bucketed by minute, and swept in bounded batches

**Decision.** Reservations carry an `expiry_bucket` (minute of expiry) with a partial
index on it. The sweeper reads a few due buckets per tick, in batches, one transaction
per reservation.

**Why not one query that reclaims everything expired.** It holds locks proportional to
the backlog. With a hundred thousand reservations outstanding it stalls allocation while
it runs.

**Why one transaction per reservation.** Reclaim has to be independently retryable. A
batch-wide transaction means one contended row stalls the reclaim of every other unit in
the batch.

**Why both strategies exist.** The index-scan variant is the obvious implementation and
is kept so the two can be compared; the bucketed variant keeps the sweeper's work
proportional to what actually expired rather than to how many reservations are
outstanding.

**A bug worth recording.** The bucketed path took a `launchID` parameter and ignored it,
so a scoped sweep cheerfully reclaimed other launches' reservations. Harmless for the
background sweeper, which wants everything — but it made every "how many did this launch
reclaim" assertion meaningless, and it is what three failing tests eventually pointed at.

---

## 10. Multi-node: Postgres arbitrates first, leases second

**Decision.** Both modes are implemented. `postgresArbitrated` is the default and the
baseline; `leasedShards` gives each node a disjoint subset of shards.

**Why build the slower one first.** It is correct with no coordination at all, because
the database serializes the contended rows. That makes it the thing the lease-based mode
has to beat, and without it there is no number to compare against.

**Fence tokens.** A lease carries a token from a sequence, re-issued only on a change of
owner. Renewing is not a change of ownership and must not invalidate work the owner
already started; a node that was paused long enough to lose its lease is told its token
is stale rather than continuing to act as owner.

**Cost.** Reclaim latency after a node dies — its shards are unavailable until the lease
expires.

---

## 10a. Redis SCAN is a cursor, and treating it as a query made recovery unsafe

**The bug.** `liveNodes()` issued a single `SCAN 0 MATCH launchos:node:* COUNT 250` and
used whatever came back. SCAN examines a slice of the keyspace per call and returns what
matched *in that slice*; it has to be iterated until the cursor returns to zero. With a
few thousand position keys in the database, one pass routinely matched nothing, and
`/node` cheerfully reported zero live nodes while the node answering the request was
itself heartbeating.

**Why it mattered more than it looked.** `CrashRecovery` reclaims reservations whose
owner is *not* in that list. A partially iterated result therefore meant reclaiming units
a live node was still holding, so a customer about to complete a purchase would lose
their reservation. It could not oversell — the commit is conditional on the reservation
still being held, so the owner's commit simply fails — but losing a live reservation is
bad enough.

**The fix.** Iterate the cursor to completion, bounded so a cursor that never returns to
zero cannot spin forever, and return nothing at all on error. A partial answer is worse
than no answer here, because "no answer" already has a safe interpretation: the existing
guard treats an empty list as "Redis is down" and declines to reclaim anything.

**Found by inspection, not by a test** — which is itself worth noting. The recovery path
is exercised by a route the measurement suite never calls, so no number moved and nothing
failed. It is the one bug in this list that the measurements would not have caught.

---

## 11. The simulator is seeded, and separate RNG streams per decision

**Decision.** SplitMix64 from a root seed, split into independent streams for arrival
times and for behaviour.

**Why separate streams.** Changing the duplicate rate must not shift every arrival time
as well. Otherwise two arms of an experiment are not seeing the same traffic and the
comparison is worthless.

**Why it matters more than it looks.** Every A/B in this project depends on two arms
receiving byte-identical traffic. The seed is recorded in `results.json`, so any run
replays — including a failure.

---

## 12. Metrics exposition is hand-written

**Decision.** A small actor that renders Prometheus text format directly, rather than a
client library.

**Why.** The exposition format is about forty lines and is then visible in the codebase,
with no dependency to keep in step with a toolchain that is itself moving. The histogram
is fixed-bucket and its quantile is bucket-interpolated — which is honest about its
resolution, and is why the simulator keeps exact samples for anything that becomes a
headline number.

**The self-check.** The invariant is verified on a timer by the running node and exported
as a gauge. Tests and the simulator only check when someone runs them; a launch is a
one-shot event, and a unit sold twice at 09:00:03 has no second chance to be noticed. The
self-check never repairs anything, because a silent repair destroys the evidence.

---

## 13. Build runs in WSL, not natively on Windows

**Decision.** Vapor targets Linux and macOS, so the inner loop is WSL2 with the Swift
toolchain, and the project is mirrored into a WSL-local directory before building.

**Why not build over `/mnt/a`.** SwiftPM does a very large amount of small-file and git
IO, and over the Windows drive mount a dependency resolve that takes seconds natively
takes many minutes. The mirror is one `rsync` and it is the difference between a usable
loop and an unusable one.

**Docker Compose is still the documented way to run it** — it is also how several nodes,
Prometheus and Grafana come up. `scripts/wsl-services.sh` exists because Docker Desktop
is a single point of failure for the whole loop on Windows, and a measurement run should
not be blocked on it; it brings up Postgres and Redis from unpacked packages under
`~/.local`, without root.
