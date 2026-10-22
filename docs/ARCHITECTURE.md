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
| `Coordination` | `Sources/LaunchCore/Coordination.swift` | Redis: position cache, in-flight counters, node heartbeats. Never authoritative. |
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
frequent request in a launch does not reach Postgres), an advisory cross-node in-flight
counter, and node heartbeats. Every read may miss or be stale; nothing consults it to
make a decision. Killing it mid-launch costs latency, not correctness — that is one of
the tests.

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
