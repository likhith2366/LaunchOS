import AsyncHTTPClient
import Foundation
import LaunchCore
import NIOCore

/// One checkout attempt. A customer sending a duplicate makes two of these.
struct CheckoutAttempt: Sendable {
    var purchased = false
    var soldOut = false
    var fragmented = false
    var rejected = false
    var errored = false
    var replayedResponse = false
    var latencyMS: Double?
    var probes = 0
    var orderID: UUID?
    var detail: String?
}

/// What happened to one customer.
struct CustomerResult: Sendable {
    var customerID: String
    var admitted: Bool
    var purchased: Bool
    var soldOut: Bool
    var fragmented: Bool
    var abandoned: Bool
    var rejected: Bool
    /// HTTP or transport failure. Distinct from being turned away, which is the system
    /// working.
    var errored: Bool
    var replayedResponse: Bool
    var checkoutLatencyMS: Double?
    var queueWaitSeconds: Double?
    var endToEndMS: Double
    var probes: Int
    var orderID: UUID?
    var pollCount: Int
    var errorDetail: String?
}

/// Aggregate outcome of one run, with the three numbers the spec insists are reported
/// separately: offered arrival rate, peak concurrent connections, and completed
/// purchases. A hundred thousand simulated customers is not a hundred thousand
/// simultaneous sockets, and conflating them would make every other number suspect.
struct RunOutcome: Sendable, Codable {
    var label: String
    var customers: Int
    var arrivalRatePerSecond: Double
    var peakConcurrentConnections: Int
    var completedPurchases: Int
    var admitted: Int
    var rejected: Int
    var soldOut: Int
    var fragmented: Int
    var abandoned: Int
    var errors: Int
    var replayedResponses: Int
    var wallClockSeconds: Double
    var throughputPerSecond: Double
    var checkoutP50MS: Double
    var checkoutP95MS: Double
    var checkoutP99MS: Double
    var checkoutMaxMS: Double
    var checkoutMeanMS: Double
    var endToEndP99MS: Double
    var meanProbes: Double
    var maxProbes: Int
    /// Admitted customers who finished checkout. The controller is meant to protect
    /// this, not just the server's latency.
    var completionRate: Double
    var errorRate: Double
    /// How far behind its own schedule the load generator ran, in milliseconds.
    ///
    /// Zero means every customer started when the arrival process said they should. A
    /// large figure means the generator, not the server, was the limit — and that the
    /// offered arrival rate reported above is the rate that was asked for rather than
    /// the rate that was delivered. Reported because a benchmark that silently fell
    /// behind is a benchmark measuring its own harness.
    var arrivalLagP50MS: Double
    var arrivalLagP99MS: Double
    var arrivalLagMaxMS: Double
    var inventory: InventorySummary
    var errorSamples: [String]
}

struct InventorySummary: Sendable, Codable {
    var totalUnits: Int
    var available: Int
    var reserved: Int
    var sold: Int
    var accountedFor: Int
    var invariantHolds: Bool
    var nonEmptyShards: Int
    var collapsed: Bool

    init(_ info: InventoryInfo) {
        self.totalUnits = info.totalUnits
        self.available = info.available
        self.reserved = info.reserved
        self.sold = info.sold
        self.accountedFor = info.accountedFor
        self.invariantHolds = info.invariantHolds
        self.nonEmptyShards = info.nonEmptyShards
        self.collapsed = info.collapsed
    }
}

/// Tracks concurrent in-flight requests so the run can report the real peak rather than
/// the nominal customer count, and so load can be capped to what the machine can
/// actually hold open.
actor ConnectionGate {
    private let limit: Int
    private var inFlight = 0
    private(set) var peak = 0
    private var waiters: [CheckedContinuation<Void, Never>] = []

    init(limit: Int) { self.limit = limit }

    func acquire() async {
        if inFlight < limit {
            inFlight += 1
            peak = max(peak, inFlight)
            return
        }
        await withCheckedContinuation { waiters.append($0) }
        inFlight += 1
        peak = max(peak, inFlight)
    }

    func release() {
        inFlight -= 1
        if let next = waiters.first {
            waiters.removeFirst()
            next.resume()
        }
    }
}

struct RunConfig: Sendable {
    var label: String
    var sku: String
    var workload: WorkloadConfig
    /// Cap on simultaneous open connections from the simulator.
    var connectionLimit: Int
    /// Whether customers go through the waiting room. False is the no-queue baseline.
    var useQueue: Bool
    /// How long to wait for admission before giving up on a customer.
    var admissionTimeout: Duration
    /// Drive admission ticks from the simulator's clock instead of the server's timer.
    var driveAdmission: Bool
    var admissionTickInterval: Duration
    /// Poll interval while waiting in the queue.
    var pollInterval: Duration
}

/// Drives one measured run.
struct Runner: Sendable {
    let client: LaunchClient
    let config: RunConfig

    func run() async throws -> (RunOutcome, [CustomerResult]) {
        let workload = Workload(config: config.workload)
        let gate = ConnectionGate(limit: config.connectionLimit)
        let collector = ResultCollector()
        let started = ContinuousClock.now

        // Admission ticks are driven from here when requested, so a run's admission
        // schedule is tied to the run's own clock rather than to a server-side timer
        // that may have drifted while the previous experiment was setting up.
        let tickTask: Task<Void, Never>? = config.driveAdmission
            ? Task {
                let interval = UInt64(config.admissionTickInterval.seconds * 1_000_000_000)
                while !Task.isCancelled {
                    _ = try? await client.admissionTick(sku: config.sku)
                    try? await Task.sleep(nanoseconds: interval)
                }
            }
            : nil

        // A bounded pool of workers, not one task per customer.
        //
        // One task each is simpler and works fine at six thousand. At a million it is
        // a million live tasks, each holding a continuation and sleeping on its own
        // arrival time, and the process spends its memory and its scheduler on
        // bookkeeping rather than on load. The pool keeps live tasks at `workers` and
        // pulls customers from a shared cursor in arrival order.
        //
        // The cost is that the generator can fall behind: if every worker is busy when
        // a customer was due, that customer starts late. That is real — it is what
        // happens to any load generator pushed past its own capacity — and it is
        // dangerous to leave unmeasured, because a run that fell ten seconds behind
        // schedule did not offer the arrival rate it claims to have offered. So the lag
        // is recorded and reported, and a run whose lag is large is a run whose offered
        // rate is the generator's limit rather than the configured one.
        let cursor = ArrivalCursor(total: workload.count)
        let lag = LagCollector()
        let workers = max(1, min(config.connectionLimit, workload.count))
        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<workers {
                group.addTask {
                    while let index = await cursor.next() {
                        let script = workload.script(at: index)
                        // Measured against the run's own start, so arrival times are
                        // absolute rather than accumulating per-customer drift.
                        let due = started.advanced(by: .seconds(script.arrivalOffset))
                        let now = ContinuousClock.now
                        if due > now {
                            try? await Task.sleep(for: due - now)
                            await lag.record(0)
                        } else {
                            await lag.record(now.duration(to: due).milliseconds * -1)
                        }
                        let result = await runCustomer(script, gate: gate)
                        await collector.add(result)
                    }
                }
            }
            await group.waitForAll()
        }
        let arrivalLag = await lag.samples()

        tickTask?.cancel()
        let wallClock = started.duration(to: .now).seconds

        // One last tick so anyone admitted by the final batch is counted, then a final
        // sweep so reserved-but-abandoned units are back in the pool before the
        // invariant is checked. Without the sweep the invariant still holds, but
        // `available` understates what a real operator would see a minute later.
        if config.driveAdmission { _ = try? await client.admissionTick(sku: config.sku) }

        let results = await collector.all()
        let inventory = try await client.inventory(sku: config.sku, assertInvariant: true)
        let outcome = summarize(
            results: results,
            workload: workload,
            arrivalLag: arrivalLag,
            peak: await gate.peak,
            wallClock: wallClock,
            inventory: inventory
        )
        return (outcome, results)
    }

    /// One customer's whole journey: join, wait, check out.
    private func runCustomer(_ script: CustomerScript, gate: ConnectionGate) async -> CustomerResult {
        var result = CustomerResult(
            customerID: script.customerID, admitted: false, purchased: false, soldOut: false,
            fragmented: false, abandoned: false, rejected: false, errored: false,
            replayedResponse: false, checkoutLatencyMS: nil, queueWaitSeconds: nil,
            endToEndMS: 0, probes: 0, orderID: nil, pollCount: 0, errorDetail: nil
        )
        let started = ContinuousClock.now
        defer { _ = started }

        do {
            if config.useQueue {
                await gate.acquire()
                let join = try await client.joinQueue(
                    sku: config.sku, customerID: script.customerID, salt: script.salt
                )
                await gate.release()
                guard let joined = join.value else {
                    result.errored = true
                    result.errorDetail = "join \(join.status): \(join.body.prefix(120))"
                    result.endToEndMS = started.duration(to: .now).milliseconds
                    return result
                }
                var status = joined

                // A customer who refreshes must land on the position they already hold.
                // The audit endpoint checks this across the whole run; this is the
                // client side of the same property.
                if script.rejoinsQueue {
                    await gate.acquire()
                    let rejoin = try await client.joinQueue(
                        sku: config.sku, customerID: script.customerID, salt: script.salt
                    )
                    await gate.release()
                    if let rejoined = rejoin.value, rejoined.arrivalSeq != joined.arrivalSeq {
                        result.errored = true
                        result.errorDetail =
                            "rejoin changed arrival seq \(joined.arrivalSeq) -> \(rejoined.arrivalSeq)"
                    }
                }

                let deadline = ContinuousClock.now.advanced(by: config.admissionTimeout)
                let pollNS = UInt64(config.pollInterval.seconds * 1_000_000_000)
                var polls = 0
                while !status.admitted, ContinuousClock.now < deadline {
                    try? await Task.sleep(nanoseconds: pollNS)
                    await gate.acquire()
                    let poll = try await client.queueStatus(
                        sku: config.sku, customerID: script.customerID, salt: script.salt
                    )
                    await gate.release()
                    polls += 1
                    if let value = poll.value { status = value }
                }
                // Extra polls beyond what is needed, to put real load on the most
                // frequent endpoint in a launch.
                for _ in 0..<script.extraPolls where status.admitted {
                    await gate.acquire()
                    _ = try? await client.queueStatus(
                        sku: config.sku, customerID: script.customerID, salt: script.salt
                    )
                    await gate.release()
                    polls += 1
                }
                result.pollCount = polls
                result.queueWaitSeconds = status.waitSeconds
                result.admitted = status.admitted
                guard status.admitted else {
                    result.rejected = true
                    result.endToEndMS = started.duration(to: .now).milliseconds
                    return result
                }
            } else {
                result.admitted = true
            }

            if script.abandons {
                result.abandoned = true
                result.endToEndMS = started.duration(to: .now).milliseconds
                return result
            }

            if script.thinkTimeMS > 0 {
                try? await Task.sleep(nanoseconds: UInt64(script.thinkTimeMS * 1_000_000))
            }

            // The duplicate case: two concurrent checkouts with the same key. Exactly
            // one order must exist afterwards, and the second response must either
            // replay the first or say the first is still running.
            if script.sendsDuplicate {
                async let first = attemptCheckout(script, gate: gate)
                async let second = attemptCheckout(script, gate: gate)
                let (a, b) = await (first, second)
                let winner = a.purchased ? a : b
                let other = a.purchased ? b : a
                result.apply(winner)
                if other.purchased, winner.purchased, other.orderID != winner.orderID {
                    result.errored = true
                    result.errorDetail = "duplicate key produced two order ids"
                }
                if other.replayedResponse { result.replayedResponse = true }
            } else {
                result.apply(await attemptCheckout(script, gate: gate))
            }
        } catch {
            result.errored = true
            result.errorDetail = "\(error)"
        }
        result.endToEndMS = started.duration(to: .now).milliseconds
        return result
    }

    private func attemptCheckout(_ script: CustomerScript, gate: ConnectionGate) async -> CheckoutAttempt {
        var attempt = CheckoutAttempt()
        await gate.acquire()
        defer { Task { await gate.release() } }
        do {
            let reply = try await client.checkout(
                sku: config.sku, customerID: script.customerID,
                idempotencyKey: script.idempotencyKey, salt: script.salt
            )
            attempt.latencyMS = reply.latencyMS
            if let value = reply.value {
                attempt.probes = value.probes
                attempt.replayedResponse = value.replayed
                switch value.outcome {
                case "purchased":
                    attempt.purchased = true
                    attempt.orderID = value.orderID
                case "soldOut":
                    attempt.soldOut = true
                case "fragmented":
                    attempt.fragmented = true
                default:
                    attempt.errored = true
                    attempt.detail = "unknown outcome \(value.outcome)"
                }
            } else if reply.status == 429 {
                // Turned away at the door. The system working as designed, not an error.
                attempt.rejected = true
            } else if reply.status == 409 {
                // A duplicate arrived while the original was still running.
                attempt.detail = "in-flight duplicate"
            } else {
                attempt.errored = true
                attempt.detail = "checkout \(reply.status): \(reply.body.prefix(120))"
            }
        } catch {
            attempt.errored = true
            attempt.detail = "\(error)"
        }
        return attempt
    }

    private func summarize(
        results: [CustomerResult],
        workload: Workload,
        arrivalLag: Samples,
        peak: Int,
        wallClock: Double,
        inventory: InventoryInfo
    ) -> RunOutcome {
        var checkout = Samples()
        var endToEnd = Samples()
        var probeTotal = 0
        var probeMax = 0
        var probeCount = 0
        for r in results {
            if let l = r.checkoutLatencyMS { checkout.add(l) }
            endToEnd.add(r.endToEndMS)
            if r.probes > 0 {
                probeTotal += r.probes
                probeMax = max(probeMax, r.probes)
                probeCount += 1
            }
        }
        let purchases = results.count { $0.purchased }
        let admitted = results.count { $0.admitted }
        let errors = results.count { $0.errored }
        let samples = Array(results.compactMap(\.errorDetail).prefix(5))
        // Abandoned customers never attempted checkout, so counting them against the
        // completion rate would make the controller look worse the more realistic the
        // workload got.
        let attempted = admitted - results.count { $0.abandoned }
        return RunOutcome(
            label: config.label,
            customers: results.count,
            arrivalRatePerSecond: workload.arrivalRate,
            peakConcurrentConnections: peak,
            completedPurchases: purchases,
            admitted: admitted,
            rejected: results.count { $0.rejected },
            soldOut: results.count { $0.soldOut },
            fragmented: results.count { $0.fragmented },
            abandoned: results.count { $0.abandoned },
            errors: errors,
            replayedResponses: results.count { $0.replayedResponse },
            wallClockSeconds: wallClock,
            throughputPerSecond: wallClock > 0 ? Double(purchases) / wallClock : 0,
            checkoutP50MS: checkout.p50,
            checkoutP95MS: checkout.p95,
            checkoutP99MS: checkout.p99,
            checkoutMaxMS: checkout.max,
            checkoutMeanMS: checkout.mean,
            endToEndP99MS: endToEnd.p99,
            meanProbes: probeCount > 0 ? Double(probeTotal) / Double(probeCount) : 0,
            maxProbes: probeMax,
            completionRate: attempted > 0 ? Double(purchases) / Double(attempted) : 0,
            errorRate: results.isEmpty ? 0 : Double(errors) / Double(results.count),
            arrivalLagP50MS: arrivalLag.p50,
            arrivalLagP99MS: arrivalLag.p99,
            arrivalLagMaxMS: arrivalLag.max,
            inventory: InventorySummary(inventory),
            errorSamples: samples
        )
    }
}

extension CustomerResult {
    mutating func apply(_ attempt: CheckoutAttempt) {
        purchased = purchased || attempt.purchased
        soldOut = soldOut || attempt.soldOut
        fragmented = fragmented || attempt.fragmented
        rejected = rejected || attempt.rejected
        errored = errored || attempt.errored
        replayedResponse = replayedResponse || attempt.replayedResponse
        if let l = attempt.latencyMS { checkoutLatencyMS = max(checkoutLatencyMS ?? 0, l) }
        probes = max(probes, attempt.probes)
        if orderID == nil { orderID = attempt.orderID }
        if errorDetail == nil { errorDetail = attempt.detail }
    }
}

actor ResultCollector {
    private var results: [CustomerResult] = []
    func add(_ r: CustomerResult) { results.append(r) }
    func all() -> [CustomerResult] { results }
}


/// Hands out customer indexes in arrival order to a fixed pool of workers.
actor ArrivalCursor {
    private var next_ = 0
    private let total: Int

    init(total: Int) { self.total = total }

    func next() -> Int? {
        guard next_ < total else { return nil }
        defer { next_ += 1 }
        return next_
    }
}

/// Collects how late each customer started against the arrival process.
actor LagCollector {
    private var lag = Samples()

    func record(_ milliseconds: Double) {
        lag.add(max(0, milliseconds))
    }

    func samples() -> Samples { lag }
}
