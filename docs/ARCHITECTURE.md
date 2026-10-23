# Architecture

## The request path

```
                          ┌─────────────────────────────────────────────┐
                          │                                             │
   customer               │        ┌──────────────────────────┐         │
      │                   │        │  Admission controller    │         │
      │ 1. join queue     │        │  AIMD · fairness policy  │         │
      ▼                   │        └───────────┬──────────────┘         │
┌──────────────┐          │                    │ 3. admit N/interval    │
│ Waiting room │──────────┘                    │    (jittered release)  │
│              │   2. health feedback          ▼                        │
│ seq from     │                        ┌──────────────┐                │
│ Postgres     │◀───────────────────────│  admitted    │                │
│ positions in │                        └──────┬───────┘                │
│ Redis        │                               │ 4. checkout            │
└──────────────┘                               ▼                        │
                                   ┌───────────────────────┐            │
                                   │ Checkout coordinator  │            │
                                   │ idempotency key owns  │            │
                                   │ the work              │            │
                                   └───────────┬───────────┘            │
                                               │ 5. reserve             │
                                               ▼                        │
                                   ┌───────────────────────┐            │
                                   │ Allocator             │            │
                                   │ probe shard h(c)+i·s  │            │
                                   │ collapse when low     │            │
                                   └───────────┬───────────┘            │
                                               │ 6. conditional update  │
                                               ▼                        │
                              ┌────────────────────────────────┐        │
                              │ Postgres — source of truth     │        │
                              │ inventory_shards, reservations │        │
                              │ orders, idempotency_keys,      │        │
                              │ queue_entries, shard_leases    │        │
                              └────────────────┬───────────────┘        │
                                               │                        │
                                               │ 7. expired reservations│
                                               ▼                        │
                                   ┌───────────────────────┐            │
                                   │ Expiry sweeper        │────────────┘
                                   │ bucketed, batched     │  8. units back to pool
                                   └───────────────────────┘
```

Two feedback loops matter more than the straight path.

**Checkout health feeds back into admission** (edge 2). The controller throttles itself
*before* the backend falls over rather than after. Without this edge the system is a
fixed-rate admitter with extra machinery.

**The expiry sweeper returns abandoned units to the allocator** (edge 8). This is what
stops a launch appearing sold out while a thousand customers sit on reservations they
never completed, and it is the only thing that ever recovers a unit from a worker that
died mid-checkout.

## Components

| Component | File | Responsibility |
|---|---|---|
| `Store` | `Sources/LaunchCore/Store.swift` | Every durable write. All invariants are enforced here by conditional updates and transactions. |
| `Allocator` | `Sources/LaunchCore/Allocator.swift` | Picks shards, probes, collapses. Deliberately not an actor. |
| `WaitingRoom` | `Sources/LaunchCore/Queue.swift` | Durable arrival order, positions, admission under a fairness policy. |
| `AdmissionController` | `Sources/LaunchCore/Admission.swift` | Fixed-rate baseline and the AIMD controller. |
| `HealthMonitor` | `Sources/LaunchCore/Admission.swift` | In-flight count and latency, smoothed. The controller's input. |
| `CheckoutCoordinator` | `Sources/LaunchCore/Checkout.swift` | Idempotency ownership, commit, crash recovery. |
| `Sweeper` / `CrashRecovery` | `Sources/LaunchCore/Sweeper.swift` | Reclaims expired and orphaned reservations. |
| `LeaseManager` / `LeaseHolder` | `Sources/LaunchCore/Leases.swift` | Lease-based shard ownership with fence tokens. |
| `PositionEstimator` | `Sources/LaunchCore/Position.swift` | Queue position in constant time, from a dense arrival index and a departure counter. |
| `PositionBenchmark` | `Sources/LaunchCore/PositionBenchmark.swift` | Times the estimate against the count it replaced, inside the node. |
| `Coordination` | `Sources/LaunchCore/Coordination.swift` | Redis: position cache, admission set, departure counter, in-flight counters, node heartbeats. Never authoritative. |
| `SelfCheck` | `Sources/LaunchCore/SelfCheck.swift` | Verifies the invariant on a timer in the running node. |
| `MetricsRegistry` | `Sources/LaunchCore/Metrics.swift` | Prometheus exposition, plus the statistics the reports are built from. |
| HTTP layer | `Sources/LaunchServer/` | Vapor routes, per-node wiring, background tasks. |
| Simulator | `Sources/LaunchSim/` | Seeded workload, the experiments, report generation. |

## Why the allocator is not an actor

Swift actors serialize their callers. Putting `reserve` on an actor would reintroduce by
hand exactly the serialization that sharding exists to remove — a global lock in front of
the thing whose entire purpose is to avoid one.

So `Allocator` is a `Sendable` final class and the only actor near the hot path is a
small cache of launch configuration, touched for a dictionary read. Even there, the
database round-trip on a cache miss happens *outside* the actor: awaiting a query while
holding it would queue every concurrent caller behind the miss.

Actors are used where they are the right tool: the health monitor and the controller are
inherently stateful and sequential, and the registry of controllers is per-node
bookkeeping, not per-request work.

## What it takes to reach a million

Three things stood between this and a million-person waiting room, and all three were
about the *number of operations per request* rather than about how fast any one of them
was.

### The question that could not scale

"How many people are ahead of me" is the most frequent request in a launch, and it was

```sql
SELECT count(*) FROM queue_entries
 WHERE launch_id = $1 AND state = 'waiting' AND seq < $2
```

That is O(the number of people ahead of you). At a million waiting it walks up to a
million index entries per poll, and a million people polling makes it the entire system.
No cache downstream helps, because the cost is in the question.

It is now arithmetic. Each launch owns a Postgres sequence that hands out a dense
`arrival_index` at join time, and departures are counted once per admitted *batch* in
Redis, so a position is `(arrival index − 1) − departed`. Measured inside the node, both
paths over the same rows:

| Queue depth | Estimated | Exact count | Speedup | Position error |
| --- | --- | --- | --- | --- |
| 1,000 | 0.48 ms | 1.19 ms | 2.5× | 0 |
| 10,000 | 0.53 ms | 3.45 ms | 6.5× | 0 |
| 100,000 | 0.52 ms | 24.0 ms | 46× | 0 |
| 1,000,000 | 0.48 ms | 85.6 ms | **177×** | 0 |

Flat across three orders of magnitude. The sequence has to be *per launch*: the first
version derived the index from the globally shared `queue_seq`, which is only dense while
one launch is taking joins — with several, their numbers interleave and the estimate
drifts without bound.

The estimate can be wrong, and the honest version of that is in the doc comment on
`PositionEstimator`: index gaps from rolled-back joins make it read high, and
out-of-order departures under a lottery policy make it read high at the back and low at
the front. `?position=exact` always answers from the count, which is how the error stays
a measured number.

### Round-trips per request

Once sharding removed the row contention, throughput went flat while latency rose in
proportion to concurrency — a saturated service spending its time on round-trips. A
checkout cost nine:

| Step | Before | After |
| --- | --- | --- |
| Confirm admission | 1 (Postgres) | 0 on a Redis hit |
| Claim the idempotency key | 1 | 1 |
| Probe for a shard | up to 4, one per shard | 1, or 2 under contention |
| Commit | 6 (BEGIN, three writes, key update, COMMIT) | 1 |

The commit is one statement whose four writes all read from the same data-modifying CTE.
That is safe here, and the distinction matters, because collapsing writes into one
statement caused the worst bug in this project: sub-statements cannot see each other's
effects, so arithmetic that has to read what another part just wrote does not belong in
one statement. Here the four writes touch four different tables and none of them reads
another's output.

The batched probe needed a second pass. `FOR UPDATE SKIP LOCKED` spreads requests across
shards instead of queueing them behind one row, but on its own it turns a momentary lock
into an allocation failure — a request whose shards were all briefly busy was told the
launch was sold out while stock sat there. The first pass skips, the second waits, so a
request only fails if its shards were genuinely empty.

### A load generator that could hold a million

The simulator spawned one task per customer. Fine at six thousand; at a million it is a
million live tasks each holding a continuation and sleeping on its own arrival time, and
the process spends its memory and its scheduler on bookkeeping rather than on load.

It is now a bounded worker pool pulling from a shared cursor in arrival order, and
customer scripts are derived from the seed and the customer's index rather than stored —
only the arrival offsets stay in memory, eight megabytes at a million instead of a few
hundred. Deriving behaviour from the index also makes the determinism stronger: customer
500,000 behaves identically whether the run has a thousand customers or a million.

A bounded pool can fall behind its own schedule, so **arrival lag** is measured and
reported. If it is large, the generator rather than the server was the limit, and the
offered arrival rate is the rate that was asked for rather than the rate delivered. A
benchmark that quietly falls behind is measuring its own harness.

## What each store guarantees

**Postgres is the arbiter.** Every correctness claim reduces to a conditional update:

- *No oversell.* `UPDATE ... WHERE available >= n` with the reservation insert fused into
  the same statement. A probe either takes a unit and records who holds it, or writes
  nothing.
- *No double commit.* `UPDATE reservations SET state = 'committed' WHERE state = 'held'
  AND expires_at > now()`. Exactly one concurrent caller matches.
- *No duplicate order.* `INSERT INTO idempotency_keys ... ON CONFLICT DO NOTHING
  RETURNING` decides ownership; the order and the key's completion share one commit.
- *No lost queue position.* `ON CONFLICT (launch_id, customer_id) DO UPDATE` touching
  only a retry counter.

**Redis is a cache and is allowed to be wrong.** It holds queue positions (so the most
frequent request in a launch does not reach Postgres), the departure counter behind
constant-time positions, the set of admitted customers, an advisory cross-node in-flight
counter, and node heartbeats. Every read may miss or be stale; nothing consults it to
make a decision it cannot also make from Postgres. Killing it mid-launch costs latency,
not correctness — that is one of the tests.

Two of those deserve their asymmetry spelled out, because both are read on the hot path:

- The **admission set** caches only *positive* answers. A hit was written by the admitter
  itself, so it is trustworthy. A miss means nothing — the cache may be cold, evicted or
  down — so it falls through to Postgres rather than turning a cache miss into a customer
  locked out of a launch they were admitted to.
- The **departure counter** is allowed to drift. A lost increment makes positions read
  high, which is the safe direction for someone waiting, and it is reconciled against a
  count of the table on a slow timer.

## The crash window

Between reserving inventory and recording the order there is a gap. If the worker dies
inside it, the unit is reserved, no order exists, and nothing in the request path will
ever complete it. Three things close it, in order of how long they take:

1. **The order and its idempotency record commit together**, so there is no state in
   which an order exists that a retry cannot find.
2. **Abandoned in-flight keys are released** after a timeout, so the customer is not
   locked out of their own order by a key whose owner is gone.
3. **Reservation expiry** is the backstop. It is durable, it is bucketed so it scales,
   and it is the only mechanism that recovers a unit whose owner vanished. A node's
   reservations can also be reclaimed early once it stops heartbeating, which turns a
   90-second TTL into a few seconds — but expiry is what makes that an optimization
   rather than a requirement.

## Multi-node

Three allocator nodes under Compose, sharing Postgres and Redis and holding no
authoritative state of their own. Which node serves a request does not matter, and the
simulator deliberately spreads requests rather than pinning a customer to one.

- **`postgresArbitrated`** — every node issues conditional updates against the same
  shard rows. Correct with no coordination, because the database serializes the
  contended rows, which is also why it is the slower mode. This is the baseline.
- **`leasedShards`** — each node leases a disjoint subset of shards and allocates only
  from those, so two nodes never touch the same row. Leases carry fence tokens from a
  sequence, re-issued only on a change of owner. The cost is reclaim latency: a dead
  node's shards are unavailable until its lease expires.

Concurrent admitters are safe because the admit statement takes `FOR UPDATE SKIP LOCKED`
over its candidate set, so two nodes never hand the same customer out twice.

## Running it

See the README. In short: `scripts/wsl-deps.sh` once, then `scripts/wsl-build.sh -c
release`, `scripts/wsl-services.sh start`, `scripts/wsl-cluster.sh start`, and
`scripts/run-experiments.sh`. Docker Compose (`deploy/docker-compose.yml`) brings up the
same thing plus Prometheus, Grafana and the operator dashboard.
