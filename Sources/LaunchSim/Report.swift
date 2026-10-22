import Foundation
import LaunchCore

/// Writes the results out as JSON and as a Markdown report.
///
/// The JSON is the record; the Markdown is what goes in the README. Both are generated
/// from the same run so the prose can never drift from the numbers, and every number in
/// the report traces back to a field in the JSON.
struct ReportWriter {
    let directory: URL

    init(directory: String) {
        self.directory = URL(fileURLWithPath: directory, isDirectory: true)
    }

    func write(_ suite: ExperimentSuite) throws -> (json: URL, markdown: URL) {
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        encoder.dateEncodingStrategy = .iso8601

        let jsonURL = directory.appendingPathComponent("results.json")
        try encoder.encode(suite).write(to: jsonURL)

        let mdURL = directory.appendingPathComponent("RESULTS.md")
        try markdown(suite).data(using: .utf8)?.write(to: mdURL)
        return (jsonURL, mdURL)
    }

    private func f(_ v: Double, _ places: Int = 1) -> String {
        String(format: "%.\(places)f", v)
    }

    /// Collapses whitespace runs in a paragraph that came from elsewhere.
    ///
    /// A Swift multi-line string with `\`-continuations drops the newline but keeps the
    /// following line's indentation, so prose composed in one file and embedded in
    /// another arrives with runs of spaces in the middle of sentences.
    private func prose(_ text: String) -> String {
        text.split(whereSeparator: \.isWhitespace).joined(separator: " ")
    }

    private func markdown(_ suite: ExperimentSuite) -> String {
        var out = "# LaunchOS measurements\n\n"
        let e = suite.environment
        out += """
            Recorded \(ISO8601DateFormatter().string(from: e.recordedAt)) on \(e.host), \
            \(e.cores) cores, \(e.nodes) allocator node\(e.nodes == 1 ? "" : "s").

            \(prose(e.notes))

            """

        if let o = suite.oversell {
            out += """

                ## Correctness: overselling

                \(o.successesPerTrial.count) trials, each firing \
                \(o.contendersPerTrial) simultaneous reservations at \
                \(o.unitsPerTrial) units, at shard counts of \
                \(o.shardCountsTested.map(String.init).joined(separator: " and ")). \
                Sharding is the change most likely to \
                break this, because the invariant then has to survive units spread over \
                rows, probed in a customer-specific order, and returned to a different \
                shard than they came from.

                | Metric | Value |
                | --- | --- |
                | Oversold units | **\(o.oversoldUnits)** |
                | Invariant held every trial | \(o.invariantHeldEveryTrial ? "yes" : "**no**") |
                | Successful purchases per trial | \(o.successesPerTrial.map(String.init).joined(separator: ", ")) |

                The invariant checked after every trial is \
                `available + reserved + sold = total`, asserted by the server, plus a \
                second check that units marked sold equal units with an order behind them.

                """
        }

        if let s = suite.shardSweep {
            out += """

                ## Optimization: inventory sharding

                \(s.contenders) simultaneous reservation attempts against \(s.units) \
                units, identical load at each shard count. Shard collapse disabled so \
                this measures contention alone.

                | Shards | Allocations/s | Speedup | p99 (ms) | Peak conns | Purchases |
                | --- | --- | --- | --- | --- | --- |

                """
            for p in s.points {
                out += "| \(p.shardCount) | \(f(p.reservationThroughput)) | "
                out += "\(f(p.speedupVsSingleRow, 2))x | \(f(p.run.checkoutP99MS)) | "
                out += "\(p.run.peakConcurrentConnections) | \(p.run.completedPurchases) |\n"
            }
            out += "\nBest speedup **\(f(s.bestSpeedup, 2))x**"
            if let plateau = s.plateauAtShards {
                out += ", plateauing at **\(plateau) shards**.\n\n\(s.nextBottleneckEvidence)\n"
            } else {
                out += ". No plateau within the range swept.\n"
            }
        }

        if let c = suite.controller {
            out += """

                ## Controlled experiment: adaptive vs fixed-rate admission

                \(c.customers) customers against \(c.units) units, identical seeded \
                traffic through each admitter.

                The adaptive arm's thresholds are derived, not picked: at a measured \
                capacity of \(f(c.serviceCapacityPerSecond, 0)) checkouts/s, holding p99 \
                at \(f(c.targetP99MS, 0)) ms means holding in-flight work near \
                \(f(c.inFlightThreshold, 0)) — Little's Law, used forwards. An earlier \
                run of this experiment left the in-flight threshold far below anything \
                the system can sit at; the controller backed off to its floor and \
                behaved like the conservative fixed arm, which measures the tuning \
                rather than AIMD.

                ### Burst (spike profile)

                | Admitter | Rate | p99 (ms) | Completion | Errors | Drain (s) | Fairness |
                | --- | --- | --- | --- | --- | --- | --- |

                """
            for arm in c.burstArms {
                out += "| \(arm.name) | \(f(arm.configuredRate, 0)) | \(f(arm.run.checkoutP99MS)) | "
                out += "\(f(arm.run.completionRate * 100))% | \(f(arm.run.errorRate * 100, 2))% | "
                out += "\(f(arm.timeToDrainSeconds)) | \(f(arm.fairness?.rankCorrelation ?? 0, 3)) |\n"
            }
            out += """

                ### Steady load — the case where the controller loses

                | Admitter | Rate | p99 (ms) | Completion | Errors | Drain (s) |
                | --- | --- | --- | --- | --- | --- |

                """
            for arm in c.steadyArms {
                out += "| \(arm.name) | \(f(arm.configuredRate, 0)) | \(f(arm.run.checkoutP99MS)) | "
                out += "\(f(arm.run.completionRate * 100))% | \(f(arm.run.errorRate * 100, 2))% | "
                out += "\(f(arm.timeToDrainSeconds)) |\n"
            }
            out += """

                Under a steady arrival rate a well-tuned fixed admitter should match or \
                beat the controller, because the controller is still probing upward and \
                backing off. The numbers above are reported whichever way they came out.

                `fixed-at-capacity` is the honest baseline: a fixed rate set exactly at \
                the measured service capacity, which is the strongest fixed setting \
                available and the one the controller actually has to beat. It is also \
                tuned with knowledge nobody has in advance — the capacity was measured \
                beforehand, on this hardware, at this shard count. The controller's \
                claim is not that it beats a perfectly tuned fixed rate on a load that \
                never changes; it is that it finds a workable rate without being told \
                one.

                """
        }

        if let fr = suite.fairness {
            out += """

                ## Fairness, defined as rank correlation

                Spearman rank correlation between arrival order and admission order. \
                1.0 is perfect FIFO; ~0.0 is a lottery.

                | Policy | Rank correlation | Admissions/s | Mean wait (s) | p99 wait (s) | Little's Law predicted wait (s) |
                | --- | --- | --- | --- | --- | --- |

                """
            for arm in fr.arms {
                let r = arm.report
                out += "| \(arm.policy) | **\(f(r?.rankCorrelation ?? 0, 3))** | "
                out += "\(f(arm.admissionThroughput)) | \(f(r?.waitMean ?? 0, 2)) | "
                out += "\(f(r?.waitP99 ?? 0, 2)) | \(f(r?.predictedMeanWait ?? 0, 2)) |\n"
            }
            out += """

                Retries that changed a customer's queue position: \
                **\(fr.retriesEverImprovedPosition ? "yes — this is a bug" : "none")**. \
                A customer who refreshes holds the position they got on arrival, \
                enforced by the (launch, customer) primary key on the queue table: the \
                conflict branch of the join statement touches only a retry counter.

                The last column predicts mean wait from queue length over admission \
                rate and is printed next to the measured mean so the two can be \
                compared rather than asserted.

                \(prose(fr.profileNote))

                The admit-statement columns are the policy's real cost. Comparing \
                policies on admissions per second would hide it, because that number is \
                set by the configured admission rate rather than by what the ordering \
                cost to compute.

                """
        }

        if let t = suite.tailFragmentation {
            out += """

                ## The cost of sharding: tail fragmentation

                The last units of \(t.units) spread over \(t.shardCount) shards. The \
                measured wave is deliberately smaller than the stock remaining, so \
                stock cannot run out and every failure is fragmentation rather than a \
                sell-out — the two are indistinguishable once availability reaches zero.

                | Shard collapse | Units left | Shards holding them | Requests in wave | Failed while stock remained | Spurious sold-out |
                | --- | --- | --- | --- | --- | --- |

                """
            for arm in [t.withoutCollapse, t.withCollapse].compactMap({ $0 }) {
                out += "| \(arm.collapseEnabled ? "on" : "off") | ~\(arm.unitsAvailableBeforeWave) | "
                out += "\(arm.shardsHoldingStockBeforeWave)/\(t.shardCount) | \(arm.requestsInWave) | "
                out += "**\(arm.fragmentedFailures) (\(f(arm.fragmentedFailureRate * 100))%)** | "
                out += "\(arm.spuriousSoldOut) |\n"
            }
            out += "\n\(prose(t.summary))\n"
        }

        if let i = suite.idempotency {
            out += """

                ## Failure handling: duplicate requests

                | Metric | Value |
                | --- | --- |
                | Customers, each sending two concurrent checkouts with one key | \(i.customers) |
                | Completed purchases | \(i.purchases) |
                | Distinct order ids | \(i.distinctOrderIDs) |
                | **Duplicate orders per idempotency key** | **\(i.duplicateOrders)** |
                | Responses replayed from the idempotency record | \(i.replayedResponses) |

                \(prose(i.summary))

                """
        }

        if let h = suite.commitHorizon {
            out += """

                ## Strict FIFO is a tunable, not a guarantee

                `seq` is taken from a Postgres sequence at INSERT, but transactions \
                commit in their own order. An admitter polling the waiting set cannot \
                see a lower-numbered entry that has not committed yet, so it admits what \
                it can see — and arrival order and admission order end up crossed with \
                nobody having retried or jumped the queue. Ordering on a pre-commit \
                sequence number is not sufficient for FIFO.

                Admitting only entries older than a commit horizon fixes it, provided \
                the horizon exceeds how long a join takes to commit under load. \
                \(h.customers) customers arriving at once, with a short control interval \
                so admit ticks land inside the window as often as possible:

                | Commit horizon (ms) | FIFO inversions | Admissions | Inversion rate | Rank correlation | Mean wait (s) |
                | --- | --- | --- | --- | --- | --- |

                """
            for p in h.points {
                out += "| \(f(p.horizonMS, 0)) | **\(p.fifoInversions)** | \(p.admissionsChecked) | "
                out += "\(f(p.inversionRate * 100, 2))% | \(f(p.rankCorrelation, 5)) | "
                out += "\(f(p.meanWaitSeconds, 2)) |\n"
            }
            out += "\n\(prose(h.explanation))\n"

            // The point of counting inversions rather than only correlating ranks.
            if let worst = h.points.max(by: { $0.fifoInversions < $1.fifoInversions }),
                worst.fifoInversions > 0
            {
                out += """

                    Note the rank correlation column against the inversion column: \
                    \(worst.fifoInversions) genuinely out-of-order admissions still \
                    scored \(f(worst.rankCorrelation, 5)). Rank correlation is dominated \
                    by the bulk of the distribution and is nearly blind to a handful of \
                    inversions, which is why they are counted directly as well. Reporting \
                    only the correlation would have hidden this entirely.

                    """
            }
        }

        if let s = suite.sweepStrategy {
            out += """

                ## Expiry sweep under backlog

                \(s.outstandingReservations) reservations abandoned, \
                \(s.expiredBacklog) of them still outstanding when their TTL fired.

                | Metric | Value |
                | --- | --- |
                | Strategy | \(s.strategy) |
                | Batches driven by the harness | \(s.batchesObserved) |
                | Batch duration, mean | \(f(s.batchMeanMS)) ms |
                | Batch duration, p99 | \(f(s.batchP99MS)) ms |
                | **Time for the pool to come back in full** | **\(f(s.secondsToFullRecovery, 2)) s** |
                | Units recovered | \(s.unitsRecovered) |

                \(prose(s.note))

                """
        }

        out += """

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

            Every run is seeded; the seed is recorded in `results.json` next to each \
            arm, and re-running with the same seed replays the same arrivals, the same \
            duplicate requests and the same abandonments.

            Simulated customers are not simultaneous sockets. Each result reports \
            offered arrival rate, peak concurrent connections and completed purchases \
            as three separate numbers, because on one laptop they are three different \
            things.

            """
        return out
    }
}
