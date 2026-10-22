# LaunchOS measurements

Recorded 2026-10-22T01:23:31Z on LIKHITH, 8 cores, 1 allocator node.

WSL2 on Windows, 8 cores, 8 GB RAM. Postgres 16.15 and Redis on the same host, fsync on, synchronous_commit on. 1 allocator node(s); simulator capped at 1500 concurrent connections. Simulated customers are not simultaneous sockets: offered arrival rate, peak concurrent connections and completed purchases are reported separately.

## Correctness: overselling

6 trials, each firing 1000 simultaneous reservations at 100 units, at shard counts of 1 and 16. Sharding is the change most likely to break this, because the invariant then has to survive units spread over rows, probed in a customer-specific order, and returned to a different shard than they came from.

| Metric | Value |
| --- | --- |
| Oversold units | **0** |
| Invariant held every trial | yes |
| Successful purchases per trial | 100, 100, 100, 100, 100, 100 |

The invariant checked after every trial is `available + reserved + sold = total`, asserted by the server, plus a second check that units marked sold equal units with an order behind them.

## Optimization: inventory sharding

4000 simultaneous reservation attempts against 2000 units, identical load at each shard count. Shard collapse disabled so this measures contention alone.

| Shards | Allocations/s | Speedup | p99 (ms) | Peak conns | Purchases |
| --- | --- | --- | --- | --- | --- |
| 1 | 176.2 | 1.00x | 7459.7 | 1500 | 2000 |
| 4 | 377.9 | 2.14x | 2871.2 | 1500 | 2000 |
| 16 | 544.8 | 3.09x | 1920.0 | 1500 | 2000 |
| 64 | 532.0 | 3.02x | 2192.4 | 1500 | 2000 |

Best speedup **3.09x**, plateauing at **16 shards**.

At the plateau the shard rows are no longer the limit. Peak concurrent connections reached 1500 against the simulator's cap, and checkout p99 at the highest shard count was 2192.4 ms, so the remaining cost is per-request work — connection handling, the Postgres round-trip per probe, and the simulator's own concurrency ceiling — rather than contention on a single row.

## Controlled experiment: adaptive vs fixed-rate admission

4000 customers against 4000 units, identical seeded traffic through each admitter.

The adaptive arm's thresholds are derived, not picked: at a measured capacity of 620 checkouts/s, holding p99 at 600 ms means holding in-flight work near 372 — Little's Law, used forwards. An earlier run of this experiment left the in-flight threshold far below anything the system can sit at; the controller backed off to its floor and behaved like the conservative fixed arm, which measures the tuning rather than AIMD.

### Burst (spike profile)

| Admitter | Rate | p99 (ms) | Completion | Errors | Drain (s) | Fairness |
| --- | --- | --- | --- | --- | --- | --- |
| fixed-conservative | 100 | 1477.1 | 100.0% | 0.00% | 33.4 | 0.979 |
| fixed-at-capacity | 620 | 1038.7 | 100.0% | 0.00% | 16.1 | 0.988 |
| fixed-aggressive | 2000 | 2900.3 | 100.0% | 0.00% | 17.6 | 0.991 |
| adaptive-aimd | 100 | 1082.9 | 100.0% | 0.00% | 18.3 | 0.991 |

### Steady load — the case where the controller loses

| Admitter | Rate | p99 (ms) | Completion | Errors | Drain (s) |
| --- | --- | --- | --- | --- | --- |
| fixed-tuned | 300 | 278.6 | 100.0% | 0.00% | 14.5 |
| fixed-at-capacity | 620 | 500.9 | 100.0% | 0.00% | 14.5 |
| fixed-aggressive | 2000 | 499.0 | 100.0% | 0.00% | 14.5 |
| adaptive-aimd | 100 | 872.7 | 100.0% | 0.00% | 14.6 |

Under a steady arrival rate a well-tuned fixed admitter should match or beat the controller, because the controller is still probing upward and backing off. The numbers above are reported whichever way they came out.

`fixed-at-capacity` is the honest baseline: a fixed rate set exactly at the measured service capacity, which is the strongest fixed setting available and the one the controller actually has to beat. It is also tuned with knowledge nobody has in advance — the capacity was measured beforehand, on this hardware, at this shard count. The controller's claim is not that it beats a perfectly tuned fixed rate on a load that never changes; it is that it finds a workable rate without being told one.

## Fairness, defined as rank correlation

Spearman rank correlation between arrival order and admission order. 1.0 is perfect FIFO; ~0.0 is a lottery.

| Policy | Rank correlation | Admissions/s | Mean wait (s) | p99 wait (s) | Little's Law predicted wait (s) |
| --- | --- | --- | --- | --- | --- |
| strictFIFO | **1.000** | 217.3 | 9.42 | 15.73 | 7.55 |
| lottery | **0.064** | 185.4 | 12.65 | 20.25 | 9.36 |
| bucketedFIFO | **0.862** | 194.9 | 11.36 | 17.89 | 8.72 |
| strictFIFO | **1.000** | 248.9 | 1.07 | 3.08 | 0.93 |
| lottery | **0.972** | 264.6 | 0.87 | 3.80 | 0.82 |
| bucketedFIFO | **0.988** | 179.4 | 5.24 | 9.40 | 3.48 |

Retries that changed a customer's queue position: **none**. A customer who refreshes holds the position they got on arrival, enforced by the (launch, customer) primary key on the queue table: the conflict branch of the join statement touches only a retry counter.

The last column predicts mean wait from queue length over admission rate and is printed next to the measured mean so the two can be compared rather than asserted.

A lottery is only as unfair as the arrival process lets it be. Drawing at random from everyone waiting looks close to FIFO when customers trickle in, because the pool to draw from only contains people who have already arrived. The policies separate under a thundering herd, where everyone is waiting at once and the draw is over the whole population — which is why both profiles are reported rather than whichever one flatters the result.

The admit-statement columns are the policy's real cost. Comparing policies on admissions per second would hide it, because that number is set by the configured admission rate rather than by what the ordering cost to compute.

## The cost of sharding: tail fragmentation

The last units of 640 spread over 64 shards. The measured wave is deliberately smaller than the stock remaining, so stock cannot run out and every failure is fragmentation rather than a sell-out — the two are indistinguishable once availability reaches zero.

| Shard collapse | Units left | Shards holding them | Requests in wave | Failed while stock remained | Spurious sold-out |
| --- | --- | --- | --- | --- | --- |
| off | ~40 | 18/64 | 159 | **60 (37.7%)** | 0 |
| on | ~41 | 17/64 | 164 | **52 (31.7%)** | 0 |

Last 5% of inventory over 64 shards, pooled over 8 trials per arm, with fewer requests in each wave than units remaining so every failure is fragmentation rather than a sell-out. Without collapse: 60 of 159 requests failed while stock remained (37.7%), with stock sitting on about 18 of 64 shards. With collapse: 52 of 164 (31.7%).

## Failure handling: duplicate requests

| Metric | Value |
| --- | --- |
| Customers, each sending two concurrent checkouts with one key | 800 |
| Completed purchases | 500 |
| Distinct order ids | 500 |
| **Duplicate orders per idempotency key** | **0** |
| Responses replayed from the idempotency record | 552 |

800 customers each sent two concurrent checkouts with one key: 500 orders, 500 distinct order ids, 0 duplicate orders, 552 responses replayed from the idempotency record.

## Strict FIFO is a tunable, not a guarantee

`seq` is taken from a Postgres sequence at INSERT, but transactions commit in their own order. An admitter polling the waiting set cannot see a lower-numbered entry that has not committed yet, so it admits what it can see — and arrival order and admission order end up crossed with nobody having retried or jumped the queue. Ordering on a pre-commit sequence number is not sufficient for FIFO.

Admitting only entries older than a commit horizon fixes it, provided the horizon exceeds how long a join takes to commit under load. 2000 customers arriving at once, with a short control interval so admit ticks land inside the window as often as possible:

| Commit horizon (ms) | FIFO inversions | Admissions | Inversion rate | Rank correlation | Mean wait (s) |
| --- | --- | --- | --- | --- | --- |
| 0 | **0** | 2000 | 0.00% | 1.00000 | 2.92 |
| 100 | **0** | 2000 | 0.00% | 1.00000 | 3.37 |
| 250 | **0** | 2000 | 0.00% | 1.00000 | 3.02 |
| 500 | **0** | 2000 | 0.00% | 1.00000 | 3.35 |
| 1000 | **0** | 2000 | 0.00% | 1.00000 | 3.25 |

Inversions fall as the horizon grows, because the horizon has to cover how long a join takes to commit rather than how far apart arrivals are. The cost is that every customer waits at least the horizon longer, so the right setting is the smallest one that holds under the commit latency the service actually sees under load — which is why this is measured rather than picked.

## Expiry sweep under backlog

3000 reservations abandoned, 3000 of them still outstanding when their TTL fired.

| Metric | Value |
| --- | --- |
| Strategy | bucketed |
| Batches driven by the harness | 4 |
| Batch duration, mean | 1746.3 ms |
| Batch duration, p99 | 2056.3 ms |
| **Time for the pool to come back in full** | **7.00 s** |
| Units recovered | 5000 |

Reservations are reclaimed in bounded batches, one transaction each, so a large backlog cannot hold locks proportional to its size or stall allocation while it drains. The node sweeps continuously as well, so the figure that matters is the time for the pool to come back in full rather than how many any one sweeper happened to claim. Measured on node node-0 with the strategy it was started with; set SWEEP_STRATEGY=indexScan or bucketed and re-run to compare the two, since two sweepers in one process would contend for the same rows.

## How to reproduce

Under Docker Compose:

```
docker compose -f deploy/docker-compose.yml up -d --build
docker compose -f deploy/docker-compose.yml run --rm sim all --output /results
```

Or natively in WSL2, which is how these numbers were produced:

```
scripts/wsl-deps.sh               # Swift toolchain, once
scripts/wsl-build.sh -c release
scripts/wsl-services.sh start     # Postgres and Redis, no root needed
scripts/wsl-cluster.sh start      # allocator node(s)
scripts/run-experiments.sh        # the whole suite, writing results/
```

Every run is seeded; the seed is recorded in `results.json` next to each arm, and re-running with the same seed replays the same arrivals, the same duplicate requests and the same abandonments.

Simulated customers are not simultaneous sockets. Each result reports offered arrival rate, peak concurrent connections and completed purchases as three separate numbers, because on one laptop they are three different things.
