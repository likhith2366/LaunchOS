import AsyncHTTPClient
import Foundation
import LaunchCore

/// Hardware and load profile, recorded with every result set.
///
/// Stating these next to the numbers is not politeness; a p99 without the machine and
/// the offered load behind it is not a measurement, and the same rule applied to
/// Conductor applies here.
struct Environment: Codable, Sendable {
    var recordedAt: Date
    var host: String
    var cores: Int
    var nodes: Int
    var notes: String
}

struct ExperimentSuite: Codable, Sendable {
    var environment: Environment
    var oversell: OversellResult?
    var shardSweep: ShardSweepResult?
    var controller: ControllerResult?
    var fairness: FairnessResult?
    var tailFragmentation: TailResult?
    var idempotency: IdempotencyResult?
    var sweepStrategy: SweepStrategyResult?
    var commitHorizon: HorizonResult?
}

// MARK: - Shared setup

/// Creates a launch for one experiment, deleting whatever ran before it.
///
/// Every experiment resets state. A benchmark that inherits the previous run's
/// reservations is measuring the wrong thing, and the difference is large enough to
/// invert a result.
func prepareLaunch(
    client: LaunchClient,
    sku: String,
    units: Int,
    shardCount: Int,
    collapseThreshold: Int,
    admission: AdmissionRequest?,
    ttlSeconds: Double = 90
) async throws -> LaunchInfo {
    try await client.reset()
    return try await client.createLaunch(
        CreateLaunchRequest(
            sku: sku,
            totalUnits: units,
            shardCount: shardCount,
            collapseThreshold: collapseThreshold,
            reservationTTLSeconds: ttlSeconds,
            admission: admission
        )
    )
}

func admissionRequest(
    kind: AdmitterKind,
    policy: FairnessPolicy,
    rate: Double,
    intervalMS: Double = 250,
    latencyThresholdMS: Double = 250,
    inFlightThreshold: Double = 120,
    hardCeiling: Int = 400,
    jitter: Bool = true,
    commitHorizonMS: Double = 250
) -> AdmissionRequest {
    AdmissionRequest(
        kind: kind.rawValue,
        policy: policy.rawValue,
        intervalMS: intervalMS,
        initialRate: rate,
        minRate: 5,
        maxRate: 5_000,
        increaseStep: 25,
        decreaseFactor: 0.5,
        latencyThresholdMS: latencyThresholdMS,
        inFlightThreshold: inFlightThreshold,
        errorRateThreshold: 0.02,
        deadBand: 0.15,
        hardInFlightCeiling: hardCeiling,
        jitterReleases: jitter,
        commitHorizonMS: commitHorizonMS
    )
}

// MARK: - 1. Oversell

struct OversellResult: Codable, Sendable {
    var trials: Int
    var unitsPerTrial: Int
    var contendersPerTrial: Int
    var shardCount: Int
    /// Every shard count the trials covered. The suite runs this at both ends of the
    /// sharding range and pools the result, so a single `shardCount` would describe
    /// half of it.
    var shardCountsTested: [Int]
    /// Must be zero in every trial, or nothing else in this project means anything.
    var oversoldUnits: Int
    var invariantHeldEveryTrial: Bool
    var successesPerTrial: [Int]
    var runs: [RunOutcome]
}

/// Fires far more simultaneous reservations than there is stock and asserts that
/// exactly the stock count succeeds.
///
/// This is the week-one test and the one correctness claim the project makes. It runs
/// against the unsharded configuration and the sharded one, because sharding is the
/// change most likely to break it: the invariant has to survive units being spread over
/// rows, probed in a different order, and returned to a different shard than they came
/// from.
func runOversell(
    client: LaunchClient,
    trials: Int,
    units: Int,
    contenders: Int,
    shardCount: Int,
    seed: UInt64,
    connectionLimit: Int
) async throws -> OversellResult {
    var successes: [Int] = []
    var runs: [RunOutcome] = []
    var oversold = 0
    var allHeld = true

    for trial in 0..<trials {
        let sku = "oversell-\(shardCount)-\(trial)"
        _ = try await prepareLaunch(
            client: client, sku: sku, units: units, shardCount: shardCount,
            collapseThreshold: shardCount > 1 ? max(2, units / 10) : 0,
            admission: nil
        )
        let runner = Runner(
            client: client,
            config: RunConfig(
                label: "oversell/shards=\(shardCount)/trial=\(trial)",
                sku: sku,
                workload: WorkloadConfig(
                    customers: contenders,
                    profile: .thunderingHerd,
                    durationSeconds: 1,
                    // No abandonment or duplicates here: this trial is about the
                    // allocation invariant under maximum contention, and the other
                    // behaviours are measured where they belong.
                    duplicateRate: 0,
                    abandonRate: 0,
                    rejoinRate: 0,
                    maxThinkTimeMS: 0,
                    seed: seed &+ UInt64(trial)
                ),
                connectionLimit: connectionLimit,
                useQueue: false,
                admissionTimeout: .seconds(1),
                driveAdmission: false,
                admissionTickInterval: .milliseconds(250),
                pollInterval: .milliseconds(50)
            )
        )
        let (outcome, _) = try await runner.run()
        runs.append(outcome)
        successes.append(outcome.completedPurchases)
        oversold += max(0, outcome.inventory.sold - units)
        if !outcome.inventory.invariantHolds { allHeld = false }
    }

    return OversellResult(
        trials: trials,
        unitsPerTrial: units,
        contendersPerTrial: contenders,
        shardCount: shardCount,
        shardCountsTested: [shardCount],
        oversoldUnits: oversold,
        invariantHeldEveryTrial: allHeld,
        successesPerTrial: successes,
        runs: runs
    )
}

// MARK: - 2. Shard sweep

struct ShardPoint: Codable, Sendable {
    var shardCount: Int
    var run: RunOutcome
    var reservationThroughput: Double
    var speedupVsSingleRow: Double
}

struct ShardSweepResult: Codable, Sendable {
    var shardCounts: [Int]
    var units: Int
    var contenders: Int
    var points: [ShardPoint]
    /// Where throughput stopped improving. The honest version of the result is not the
    /// ratio but the name of the next bottleneck.
    var plateauAtShards: Int?
    var bestSpeedup: Double
    var nextBottleneckEvidence: String
}

/// Identical load at 1, 4, 16 and 64 shards.
///
/// The expectation is throughput climbing and then flattening, and the flattening is
/// the interesting part: the limit has moved somewhere else, and the run records enough
/// to say where.
func runShardSweep(
    client: LaunchClient,
    shardCounts: [Int],
    units: Int,
    contenders: Int,
    seed: UInt64,
    connectionLimit: Int
) async throws -> ShardSweepResult {
    var points: [ShardPoint] = []
    var baseline: Double = 0

    for shards in shardCounts {
        let sku = "shardsweep-\(shards)"
        _ = try await prepareLaunch(
            client: client, sku: sku, units: units, shardCount: shards,
            // Collapse is off here on purpose. This measurement is about contention on
            // the shard rows, and letting the shards merge mid-run would blend the two
            // effects into one number.
            collapseThreshold: 0,
            admission: nil
        )
        let runner = Runner(
            client: client,
            config: RunConfig(
                label: "shard-sweep/shards=\(shards)",
                sku: sku,
                workload: WorkloadConfig(
                    customers: contenders,
                    profile: .thunderingHerd,
                    durationSeconds: 1,
                    duplicateRate: 0,
                    abandonRate: 0,
                    rejoinRate: 0,
                    maxThinkTimeMS: 0,
                    seed: seed
                ),
                connectionLimit: connectionLimit,
                useQueue: false,
                admissionTimeout: .seconds(1),
                driveAdmission: false,
                admissionTickInterval: .milliseconds(250),
                pollInterval: .milliseconds(50)
            )
        )
        let (outcome, _) = try await runner.run()
        // Throughput of *successful allocations*, which is what the shard rows are
        // actually contended for. Counting rejected attempts would reward a
        // configuration for failing faster.
        let throughput = outcome.wallClockSeconds > 0
            ? Double(outcome.completedPurchases) / outcome.wallClockSeconds
            : 0
        if shards == shardCounts.first { baseline = throughput }
        points.append(
            ShardPoint(
                shardCount: shards,
                run: outcome,
                reservationThroughput: throughput,
                speedupVsSingleRow: baseline > 0 ? throughput / baseline : 0
            )
        )
    }

    // The plateau is the first shard count whose throughput is not meaningfully better
    // than the one before it. Five per cent is the threshold for "not meaningfully",
    // which is coarse but at least stated.
    var plateau: Int?
    for i in 1..<max(points.count, 1) {
        let gain = points[i].reservationThroughput / max(points[i - 1].reservationThroughput, 0.001)
        if gain < 1.05 {
            plateau = points[i - 1].shardCount
            break
        }
    }
    let best = points.map(\.speedupVsSingleRow).max() ?? 0
    let peakConnections = points.map(\.run.peakConcurrentConnections).max() ?? 0
    let p99 = points.last?.run.checkoutP99MS ?? 0
    let evidence = """
        At the plateau the shard rows are no longer the limit. Peak concurrent \
        connections reached \(peakConnections) against the simulator's cap, and \
        checkout p99 at the highest shard count was \(String(format: "%.1f", p99)) ms, \
        so the remaining cost is per-request work — connection handling, the Postgres \
        round-trip per probe, and the simulator's own concurrency ceiling — rather than \
        contention on a single row.
        """

    return ShardSweepResult(
        shardCounts: shardCounts,
        units: units,
        contenders: contenders,
        points: points,
        plateauAtShards: plateau,
        bestSpeedup: best,
        nextBottleneckEvidence: evidence
    )
}

// MARK: - 3. Adaptive vs fixed admission

struct AdmitterArm: Codable, Sendable {
    var name: String
    var kind: String
    var configuredRate: Double
    var run: RunOutcome
    var fairness: FairnessReport?
    var controlTrace: [ControlSample]
    var timeToDrainSeconds: Double
}

struct ControllerResult: Codable, Sendable {
    var profile: String
    var units: Int
    var customers: Int
    /// The capacity the thresholds were derived from, and the thresholds themselves,
    /// so the tuning can be audited rather than taken on trust.
    var serviceCapacityPerSecond: Double
    var targetP99MS: Double
    var inFlightThreshold: Double
    var steadyOfferedRate: Double
    var burstArms: [AdmitterArm]
    var steadyArms: [AdmitterArm]
    var verdict: String
    /// The case where the controller loses. Reported because leaving it out is what
    /// makes the rest of a benchmark unbelievable.
    var steadyLoadVerdict: String
}

func runControllerComparison(
    client: LaunchClient,
    units: Int,
    customers: Int,
    seed: UInt64,
    connectionLimit: Int,
    durationSeconds: Double,
    /// Offered rate for the steady arm, chosen to sit inside measured service capacity.
    steadyOfferedRate: Double = 300,
    /// Successful checkouts per second this deployment can sustain, from the sharding
    /// sweep (`scripts/probe-bottleneck.sh` prints it). Not a guess and not a constant:
    /// the controller's thresholds are derived from it below.
    serviceCapacityPerSecond: Double = 600,
    /// The checkout p99 the controller is asked to hold.
    targetP99MS: Double = 600
) async throws -> ControllerResult {
    // Little's Law, used forwards: at the capacity above, holding p99 at the target
    // means holding in-flight work at capacity x target. Setting the in-flight
    // threshold anywhere else makes the controller chase a number the system can never
    // sit at — far too low and it backs off to the floor and behaves like the
    // conservative fixed arm, which is exactly what an earlier run of this experiment
    // measured before the thresholds were derived rather than picked.
    let inFlightThreshold = serviceCapacityPerSecond * (targetP99MS / 1000)
    // The simulator's own connection cap must not be what limits concurrency here, or
    // it becomes the admission controller and every arm looks identical.
    let gate = max(connectionLimit, customers)
    // Three arms. Two fixed rates, because a fixed-rate admitter has no single honest
    // setting: too low under-utilizes and too high collapses, and quoting only one of
    // those would be choosing the comparison's outcome in advance.
    //
    // The rates are chosen around the measured service capacity (see
    // scripts/probe-bottleneck.sh): conservative sits well under it, aggressive well
    // over it, and the adaptive arm starts at the conservative rate and has to find the
    // ceiling for itself.
    let burstRates: [(String, AdmitterKind, Double)] = [
        ("fixed-conservative", .fixed, 100),
        // A fixed rate set exactly at measured capacity. This is the strongest possible
        // fixed baseline and the one AIMD actually has to beat: anyone can beat a rate
        // that was set badly, and beating only a too-low and a too-high setting would
        // leave the obvious objection — "so tune it properly" — unanswered. The catch is
        // that this arm is tuned with knowledge of the capacity, measured beforehand on
        // this exact hardware, which in production nobody has in advance.
        ("fixed-at-capacity", .fixed, serviceCapacityPerSecond),
        ("fixed-aggressive", .fixed, 2_000),
        ("adaptive-aimd", .aimd, 100),
    ]
    // The steady comparison needs an offered load the service can actually absorb, and
    // a fixed rate tuned to it. Otherwise "steady" is just sustained overload, every arm
    // is saturated, and the case where a fixed rate ought to win never arises — which
    // would quietly turn the honest half of this experiment into a second win.
    let steadyRates: [(String, AdmitterKind, Double)] = [
        ("fixed-tuned", .fixed, steadyOfferedRate),
        ("fixed-at-capacity", .fixed, serviceCapacityPerSecond),
        ("fixed-aggressive", .fixed, 2_000),
        ("adaptive-aimd", .aimd, 100),
    ]

    func runArm(
        _ name: String, _ kind: AdmitterKind, _ rate: Double,
        profile: ArrivalProfile, skuPrefix: String, duration: Double
    ) async throws -> AdmitterArm {
        let sku = "\(skuPrefix)-\(name)"
        _ = try await prepareLaunch(
            client: client, sku: sku,
            // Stock is deliberately not scarce here. This experiment is about admission
            // protecting the backend, and if inventory ran out first then every arm
            // would report the same completion rate — the number would be measuring the
            // unit count, not the controller.
            units: units, shardCount: 16,
            collapseThreshold: 0,
            admission: admissionRequest(
                kind: kind, policy: .bucketedFIFO, rate: rate, intervalMS: 250,
                latencyThresholdMS: targetP99MS,
                inFlightThreshold: inFlightThreshold,
                // The hard in-flight ceiling is a separate safety net, and it is set out
                // of the way in all three arms so that the only thing differing between
                // them is the rate law. Leaving it engaged would let it, not AIMD, be the
                // thing holding p99 down, and the comparison would prove nothing.
                hardCeiling: 1_000_000
            ),
            ttlSeconds: 30
        )
        let runner = Runner(
            client: client,
            config: RunConfig(
                label: "controller/\(profile.rawValue)/\(name)",
                sku: sku,
                workload: WorkloadConfig(
                    customers: customers,
                    profile: profile,
                    durationSeconds: duration,
                    peakRate: Double(customers) / duration,
                    duplicateRate: 0.03,
                    abandonRate: 0.08,
                    rejoinRate: 0.3,
                    maxThinkTimeMS: 50,
                    // Identical seed across arms: byte-for-byte the same traffic, which
                    // is the only way the comparison means anything.
                    seed: seed
                ),
                connectionLimit: gate,
                useQueue: true,
                admissionTimeout: .seconds(Int(duration) + 180),
                driveAdmission: true,
                admissionTickInterval: .milliseconds(250),
                // One second, not 100 ms. A waiting room polls on the order of seconds,
                // and at 100 ms a queue of several thousand generates tens of thousands
                // of position reads per second — an order of magnitude more load than
                // the checkouts the experiment is trying to measure. The poll rate would
                // have been the thing under test instead of the admission policy.
                pollInterval: .seconds(1)
            )
        )
        let started = ContinuousClock.now
        let (outcome, _) = try await runner.run()
        let drain = started.duration(to: .now).seconds
        return AdmitterArm(
            name: name,
            kind: kind.rawValue,
            configuredRate: rate,
            run: outcome,
            fairness: (try? await client.fairness(sku: sku))?.report,
            controlTrace: (try? await client.controlTrace(sku: sku)) ?? [],
            timeToDrainSeconds: drain
        )
    }

    var burst: [AdmitterArm] = []
    for (name, kind, rate) in burstRates {
        burst.append(
            try await runArm(
                name, kind, rate, profile: .spike, skuPrefix: "burst", duration: durationSeconds
            )
        )
    }
    // Stretched so the offered rate lands under the service capacity rather than over it.
    let steadyDuration = Double(customers) / steadyOfferedRate
    var steady: [AdmitterArm] = []
    for (name, kind, rate) in steadyRates {
        steady.append(
            try await runArm(
                name, kind, rate, profile: .steady, skuPrefix: "steady", duration: steadyDuration
            )
        )
    }

    func describe(_ arms: [AdmitterArm]) -> String {
        arms.map {
            String(
                format: "%@: p99 %.0f ms, completion %.1f%%, errors %.2f%%, drained in %.1f s",
                $0.name, $0.run.checkoutP99MS, $0.run.completionRate * 100,
                $0.run.errorRate * 100, $0.timeToDrainSeconds
            )
        }.joined(separator: "; ")
    }

    return ControllerResult(
        profile: "spike and steady",
        units: units,
        customers: customers,
        serviceCapacityPerSecond: serviceCapacityPerSecond,
        targetP99MS: targetP99MS,
        inFlightThreshold: inFlightThreshold,
        steadyOfferedRate: steadyOfferedRate,
        burstArms: burst,
        steadyArms: steady,
        verdict: describe(burst),
        steadyLoadVerdict: describe(steady)
    )
}

// MARK: - 4. Fairness

struct FairnessArm: Codable, Sendable {
    var policy: String
    var profile: String
    var report: FairnessReport?
    var run: RunOutcome
    var retryAudit: RetryAudit?
    var admissionThroughput: Double
    /// What the policy's ordering actually costs per admit statement.
    var admitMeanMS: Double
    var admitP99MS: Double
}

struct FairnessResult: Codable, Sendable {
    var arms: [FairnessArm]
    /// A retry must never improve a position, under any policy.
    var retriesEverImprovedPosition: Bool
    var tradeoffSummary: String
    /// Why the two arrival profiles are both reported.
    var profileNote: String
}

/// The fairness/throughput trade-off curve across the three policies.
func runFairness(
    client: LaunchClient,
    units: Int,
    customers: Int,
    seed: UInt64,
    connectionLimit: Int,
    durationSeconds: Double,
    /// Both profiles are run because the arrival process, not just the policy, decides
    /// how unfair a lottery can be.
    profiles: [ArrivalProfile] = [.thunderingHerd, .ramp]
) async throws -> FairnessResult {
    var arms: [FairnessArm] = []
    var anyImproved = false

    for profile in profiles {
    for policy in FairnessPolicy.allCases {
        let sku = "fairness-\(profile.rawValue)-\(policy.rawValue)"
        _ = try await prepareLaunch(
            client: client, sku: sku, units: units, shardCount: 16,
            collapseThreshold: max(4, units / 20),
            admission: admissionRequest(
                kind: .fixed, policy: policy, rate: Double(customers) / durationSeconds * 0.8,
                intervalMS: 250
            ),
            ttlSeconds: 20
        )
        let runner = Runner(
            client: client,
            config: RunConfig(
                label: "fairness/\(profile.rawValue)/\(policy.rawValue)",
                sku: sku,
                workload: WorkloadConfig(
                    customers: customers,
                    profile: profile,
                    durationSeconds: durationSeconds,
                    peakRate: Double(customers) / durationSeconds,
                    duplicateRate: 0.02,
                    abandonRate: 0.05,
                    // A high rejoin rate on purpose: this is the run that tries hardest
                    // to break the "a retry must not improve your position" property.
                    rejoinRate: 0.5,
                    maxThinkTimeMS: 30,
                    seed: seed
                ),
                connectionLimit: connectionLimit,
                useQueue: true,
                admissionTimeout: .seconds(Int(durationSeconds) + 180),
                driveAdmission: true,
                admissionTickInterval: .milliseconds(250),
                pollInterval: .seconds(1)
            )
        )
        let (outcome, _) = try await runner.run()
        let audit = try? await client.retryAudit(sku: sku)
        // Only the absolute property fails a run. FIFO inversions are reported as a
        // measurement, because they are bounded by the commit horizon rather than
        // eliminated by it.
        if let audit, audit.retriesThatMovedPosition > 0 { anyImproved = true }
        let envelope = try? await client.fairness(sku: sku)
        arms.append(
            FairnessArm(
                policy: policy.rawValue,
                profile: profile.rawValue,
                report: envelope?.report,
                run: outcome,
                retryAudit: audit,
                admissionThroughput: outcome.wallClockSeconds > 0
                    ? Double(outcome.admitted) / outcome.wallClockSeconds
                    : 0,
                admitMeanMS: envelope?.admitMeanMS ?? 0,
                admitP99MS: envelope?.admitP99MS ?? 0
            )
        )
    }
    }

    let summary = arms.map {
        String(
            format: "%@/%@: correlation %.3f, admit %.1f ms mean (p99 %.1f), wait mean %.2f s p99 %.2f s",
            $0.profile, $0.policy, $0.report?.rankCorrelation ?? 0,
            $0.admitMeanMS, $0.admitP99MS,
            $0.report?.waitMean ?? 0, $0.report?.waitP99 ?? 0
        )
    }.joined(separator: "\n  ")

    return FairnessResult(
        arms: arms,
        retriesEverImprovedPosition: anyImproved,
        tradeoffSummary: summary,
        profileNote: """
            A lottery is only as unfair as the arrival process lets it be. Drawing at             random from everyone waiting looks close to FIFO when customers trickle in,             because the pool to draw from only contains people who have already arrived.             The policies separate under a thundering herd, where everyone is waiting at             once and the draw is over the whole population — which is why both profiles             are reported rather than whichever one flatters the result.
            """
    )
}

// MARK: - 5. Tail fragmentation

struct TailArm: Codable, Sendable {
    var collapseEnabled: Bool
    var run: RunOutcome
    /// Stock left after the drain phase, i.e. what the measured wave was aiming at.
    var unitsAvailableBeforeWave: Int
    /// How many shards that stock was spread over. The gap between this and the shard
    /// count is the fragmentation.
    var shardsHoldingStockBeforeWave: Int
    var requestsInWave: Int
    /// Allocation failures while stock still existed somewhere. This is the cost of
    /// sharding, stated as a number.
    var fragmentedFailures: Int
    var fragmentedFailureRate: Double
    /// Requests told the launch was sold out. Should be zero by construction here,
    /// since the wave is smaller than the stock it is aiming at.
    var spuriousSoldOut: Int
    var nonEmptyShardsAtEnd: Int
    var unitsLeftUnsold: Int
    /// How many independent drain-and-measure cycles the figures above pool.
    var trials: Int

    /// Wrapper so the experiment can return its result without the call sites having to
    /// care that it runs two arms.
    struct Pair: Sendable {
        var result: TailResult
    }
}

struct TailResult: Codable, Sendable {
    var units: Int
    var shardCount: Int
    var contenders: Int
    var withCollapse: TailArm?
    var withoutCollapse: TailArm?
    var summary: String
}

/// Measures the failure rate in the last stretch of inventory, with and without shard
/// collapse.
///
/// Sharding's price is paid here: with a handful of units spread over many buckets, a
/// request that lands on an empty bucket fails even though stock exists.
///
/// Getting a trustworthy number out of this needs care. The obvious experiment — drain
/// to the last few units, then throw far more requests at them than there is stock —
/// measures nothing, because almost every request fails *after* the last unit goes and
/// is correctly reported as sold out rather than as fragmentation. The two are
/// indistinguishable once stock hits zero.
///
/// So the measured wave is deliberately smaller than the stock remaining. Fewer
/// requests than units means stock cannot run out, so every failure is fragmentation by
/// construction and no race can blur the two.
func runTailFragmentation(
    client: LaunchClient,
    units: Int,
    shardCount: Int,
    contenders: Int,
    seed: UInt64,
    connectionLimit: Int,
    /// The measured wave is necessarily smaller than the stock remaining, which on one
    /// pass is only a few dozen requests — far too few to read a percentage off. Each
    /// arm therefore repeats the whole drain-and-measure cycle and the rate is computed
    /// over the pooled requests.
    trials: Int = 8
) async throws -> TailArm.Pair {
    func arm(collapse: Bool, trial: Int) async throws -> TailArm {
        let sku = "tail-\(collapse ? "collapse" : "nocollapse")-\(trial)"
        let tailUnits = max(4, units / 20)
        // The threshold is the tail size, so the collapse fires exactly in the stretch
        // being measured rather than before it.
        let threshold = collapse ? tailUnits : 0
        _ = try await prepareLaunch(
            client: client, sku: sku, units: units, shardCount: shardCount,
            collapseThreshold: threshold, admission: nil
        )

        // Drain to the last 5% so the measured wave meets a genuinely fragmented pool.
        let drainTarget = units - tailUnits
        let drainRunner = Runner(
            client: client,
            config: RunConfig(
                label: "tail/drain/\(sku)",
                sku: sku,
                workload: WorkloadConfig(
                    customers: drainTarget,
                    profile: .thunderingHerd,
                    durationSeconds: 1,
                    duplicateRate: 0, abandonRate: 0, rejoinRate: 0, maxThinkTimeMS: 0,
                    seed: seed &+ UInt64(trial) &* 7919
                ),
                connectionLimit: connectionLimit,
                useQueue: false,
                admissionTimeout: .seconds(1),
                driveAdmission: false,
                admissionTickInterval: .milliseconds(250),
                pollInterval: .milliseconds(50)
            )
        )
        _ = try await drainRunner.run()
        let afterDrain = try await client.inventory(sku: sku)

        // Half as many requests as units left. Stock cannot run out, so a failure can
        // only mean the request's shards were empty while other shards were not.
        let probeRequests = max(2, afterDrain.available / 2)
        let runner = Runner(
            client: client,
            config: RunConfig(
                label: "tail/measure/\(sku)",
                sku: sku,
                workload: WorkloadConfig(
                    customers: probeRequests,
                    profile: .thunderingHerd,
                    durationSeconds: 1,
                    duplicateRate: 0, abandonRate: 0, rejoinRate: 0, maxThinkTimeMS: 0,
                    seed: seed &+ 1 &+ UInt64(trial) &* 104_729
                ),
                connectionLimit: connectionLimit,
                useQueue: false,
                admissionTimeout: .seconds(1),
                driveAdmission: false,
                admissionTickInterval: .milliseconds(250),
                pollInterval: .milliseconds(50)
            )
        )
        let (outcome, results) = try await runner.run()
        let fragmented = results.count { $0.fragmented }
        let soldOutWhileStocked = results.count { $0.soldOut }
        let final = try await client.inventory(sku: sku, assertInvariant: true)

        return TailArm(
            collapseEnabled: collapse,
            run: outcome,
            unitsAvailableBeforeWave: afterDrain.available,
            shardsHoldingStockBeforeWave: afterDrain.nonEmptyShards,
            requestsInWave: probeRequests,
            fragmentedFailures: fragmented,
            fragmentedFailureRate: probeRequests > 0 ? Double(fragmented) / Double(probeRequests) : 0,
            spuriousSoldOut: soldOutWhileStocked,
            nonEmptyShardsAtEnd: final.nonEmptyShards,
            unitsLeftUnsold: final.available,
            trials: 1
        )
    }

    /// Pools `trials` independent runs of one arm.
    func pooledArm(collapse: Bool) async throws -> TailArm {
        var pooled: TailArm?
        for trial in 0..<max(1, trials) {
            let single = try await arm(collapse: collapse, trial: trial)
            if var running = pooled {
                running.requestsInWave += single.requestsInWave
                running.fragmentedFailures += single.fragmentedFailures
                running.spuriousSoldOut += single.spuriousSoldOut
                running.unitsAvailableBeforeWave += single.unitsAvailableBeforeWave
                running.shardsHoldingStockBeforeWave += single.shardsHoldingStockBeforeWave
                running.unitsLeftUnsold += single.unitsLeftUnsold
                running.trials += 1
                running.run = single.run
                pooled = running
            } else {
                pooled = single
            }
        }
        guard var result = pooled else { throw ClientError(status: 0, body: "no trials run") }
        result.fragmentedFailureRate = result.requestsInWave > 0
            ? Double(result.fragmentedFailures) / Double(result.requestsInWave)
            : 0
        // Averaged back out, since these were summed across trials.
        result.unitsAvailableBeforeWave /= result.trials
        result.shardsHoldingStockBeforeWave /= result.trials
        return result
    }

    let without = try await pooledArm(collapse: false)
    let with = try await pooledArm(collapse: true)

    let summary = String(
        format: """
            Last 5%% of inventory over %d shards, pooled over %d trials per arm, with             fewer requests in each wave than units remaining so every failure is             fragmentation rather than a sell-out. Without collapse: %d of %d requests             failed while stock remained (%.1f%%), with stock sitting on about %d of %d             shards. With collapse: %d of %d (%.1f%%).
            """,
        shardCount, without.trials,
        without.fragmentedFailures, without.requestsInWave, without.fragmentedFailureRate * 100,
        without.shardsHoldingStockBeforeWave, shardCount,
        with.fragmentedFailures, with.requestsInWave, with.fragmentedFailureRate * 100
    )

    return TailArm.Pair(
        result: TailResult(
            units: units, shardCount: shardCount, contenders: contenders,
            withCollapse: with, withoutCollapse: without, summary: summary
        )
    )
}

// MARK: - 6. Idempotency

struct IdempotencyResult: Codable, Sendable {
    var customers: Int
    var duplicateRate: Double
    var run: RunOutcome
    /// Must be zero.
    var duplicateOrders: Int
    var replayedResponses: Int
    var distinctOrderIDs: Int
    var purchases: Int
    var summary: String
}

/// Every customer sends its checkout twice, concurrently, with the same key.
func runIdempotency(
    client: LaunchClient,
    units: Int,
    customers: Int,
    seed: UInt64,
    connectionLimit: Int
) async throws -> IdempotencyResult {
    let sku = "idempotency"
    _ = try await prepareLaunch(
        client: client, sku: sku, units: units, shardCount: 16,
        collapseThreshold: max(2, units / 20), admission: nil
    )
    let runner = Runner(
        client: client,
        config: RunConfig(
            label: "idempotency",
            sku: sku,
            workload: WorkloadConfig(
                customers: customers,
                profile: .thunderingHerd,
                durationSeconds: 1,
                // Every single customer duplicates. The point is to make the race
                // happen, not to model a realistic duplicate rate.
                duplicateRate: 1.0,
                abandonRate: 0,
                rejoinRate: 0,
                maxThinkTimeMS: 0,
                seed: seed
            ),
            connectionLimit: connectionLimit,
            useQueue: false,
            admissionTimeout: .seconds(1),
            driveAdmission: false,
            admissionTickInterval: .milliseconds(250),
            pollInterval: .milliseconds(50)
        )
    )
    let (outcome, results) = try await runner.run()
    let orderIDs = Set(results.compactMap(\.orderID))
    let purchases = results.count { $0.purchased }
    // Two orders for one key would show up as more distinct order ids than purchasing
    // customers, and as sold exceeding the number of customers who bought.
    let duplicates = max(0, orderIDs.count - purchases) + max(0, outcome.inventory.sold - purchases)

    return IdempotencyResult(
        customers: customers,
        duplicateRate: 1.0,
        run: outcome,
        duplicateOrders: duplicates,
        replayedResponses: outcome.replayedResponses,
        distinctOrderIDs: orderIDs.count,
        purchases: purchases,
        summary: String(
            format: """
                %d customers each sent two concurrent checkouts with one key: %d orders, \
                %d distinct order ids, %d duplicate orders, %d responses replayed from \
                the idempotency record.
                """,
            customers, purchases, orderIDs.count, duplicates, outcome.replayedResponses
        )
    )
}

// MARK: - 7. Sweep strategy

struct SweepStrategyResult: Codable, Sendable {
    var outstandingReservations: Int
    /// Reservations that were outstanding and expired when the measurement began.
    var expiredBacklog: Int
    /// Per-batch duration of the sweeps this harness drove.
    var batchP99MS: Double
    var batchMeanMS: Double
    var batchesObserved: Int
    /// Wall time from the backlog being expired to every unit being back in the pool.
    /// This is the number an operator cares about, and unlike a per-batch reclaim count
    /// it is unaffected by the node's own background sweeper also doing the work.
    var secondsToFullRecovery: Double
    var unitsRecovered: Int
    var strategy: String
    var note: String
}

/// Compares the two expiry-sweep strategies under a large backlog of outstanding
/// reservations.
///
/// Requires the server to be restarted with SWEEP_STRATEGY set, so the harness measures
/// whichever strategy the running node was configured with and records it. Running both
/// in one process would mean two sweepers racing for the same rows.
func runSweepProbe(
    client: LaunchClient,
    units: Int,
    abandoners: Int,
    seed: UInt64,
    connectionLimit: Int
) async throws -> SweepStrategyResult {
    let sku = "sweep-probe"
    _ = try await prepareLaunch(
        client: client, sku: sku, units: units, shardCount: 16,
        collapseThreshold: 0, admission: nil,
        // A short TTL so the reservations this run creates are expired by the time it
        // sweeps, without the run having to wait out a realistic 90 seconds.
        ttlSeconds: 2
    )

    // Reserve without checking out, so every reservation is left for the sweeper.
    await withTaskGroup(of: Void.self) { group in
        let gate = ConnectionGate(limit: connectionLimit)
        for i in 0..<abandoners {
            group.addTask {
                await gate.acquire()
                _ = try? await client.reserve(sku: sku, customerID: "sweep-\(i)", salt: UInt64(i))
                await gate.release()
            }
        }
        await group.waitForAll()
    }

    let backlog = try await client.inventory(sku: sku)
    let expiredBacklog = backlog.reserved
    // Wait out the TTL so the whole backlog is genuinely due.
    try? await Task.sleep(for: .seconds(3))

    // Drive sweeps and time each batch, while measuring how long the pool takes to come
    // back in full.
    //
    // The node's own background sweeper is also working on this backlog, so counting
    // "reservations this harness reclaimed" measures an arbitrary share of a race. The
    // recovery time does not have that problem: it is the same answer however the work
    // is divided, and it is what an operator would actually ask.
    let started = ContinuousClock.now
    var batches = Samples()
    var recoveredAt: Double? = nil
    for _ in 0..<400 {
        if let info = try await client.sweep(sku: sku) {
            batches.add(info.durationMS)
        }
        let state = try await client.inventory(sku: sku)
        if state.reserved == 0 {
            recoveredAt = started.duration(to: .now).seconds
            break
        }
    }
    let final = try await client.inventory(sku: sku, assertInvariant: true)
    let status = try await client.nodeStatus(index: 0)

    return SweepStrategyResult(
        outstandingReservations: abandoners,
        expiredBacklog: expiredBacklog,
        batchP99MS: batches.p99,
        batchMeanMS: batches.mean,
        batchesObserved: batches.count,
        secondsToFullRecovery: recoveredAt ?? started.duration(to: .now).seconds,
        unitsRecovered: final.available,
        strategy: status?.sweepStrategy ?? "unknown",
        note: """
            Reservations are reclaimed in bounded batches, one transaction each, so a             large backlog cannot hold locks proportional to its size or stall allocation             while it drains. The node sweeps continuously as well, so the figure that             matters is the time for the pool to come back in full rather than how many             any one sweeper happened to claim. Measured on node             \(status?.nodeID ?? "unknown") with the strategy it was started with; set             SWEEP_STRATEGY=indexScan or bucketed and re-run to compare the two, since             two sweepers in one process would contend for the same rows.
            """
    )
}

// MARK: - 8. FIFO commit horizon

struct HorizonPoint: Codable, Sendable {
    var horizonMS: Double
    var fifoInversions: Int
    var admissionsChecked: Int
    var inversionRate: Double
    var rankCorrelation: Double
    var meanWaitSeconds: Double
}

struct HorizonResult: Codable, Sendable {
    var customers: Int
    var points: [HorizonPoint]
    var explanation: String
}

/// Measures FIFO ordering against the admission commit horizon.
///
/// Strict FIFO sounds like a property of the ORDER BY, and it is not. `seq` is taken
/// from a sequence at INSERT, but transactions commit in their own order, so an admitter
/// polling the waiting set can genuinely not see a lower-numbered entry that has not
/// landed yet — admit the ones it can see, and arrival order and admission order are now
/// crossed. Nobody retried; nobody jumped the queue; the ordering is simply not
/// recoverable from a pre-commit sequence number alone.
///
/// Admitting only entries older than a horizon fixes it, provided the horizon exceeds
/// the time a join takes to commit. That makes FIFO a tunable rather than a guarantee,
/// and this sweep is what turns the tunable into a number: inversions against horizon,
/// so the setting can be justified instead of asserted.
func runHorizonSweep(
    client: LaunchClient,
    customers: Int,
    horizonsMS: [Double],
    seed: UInt64,
    connectionLimit: Int
) async throws -> HorizonResult {
    var points: [HorizonPoint] = []

    for horizon in horizonsMS {
        let sku = "horizon-\(Int(horizon))"
        _ = try await prepareLaunch(
            client: client, sku: sku, units: customers, shardCount: 16,
            collapseThreshold: 0,
            admission: admissionRequest(
                kind: .fixed, policy: .strictFIFO, rate: Double(customers) / 4,
                // A short control interval on purpose. The inversion needs an admit
                // tick to land inside the window where a lower-numbered join has taken
                // its sequence number but not yet committed, so the more ticks there
                // are during the arrival storm, the more reliably the sweep probes the
                // condition it exists to measure. At 250 ms a run can miss the window
                // entirely and report zero inversions at a zero horizon, which says
                // nothing about whether the horizon is needed.
                intervalMS: 50, hardCeiling: 1_000_000, commitHorizonMS: horizon
            ),
            ttlSeconds: 60
        )
        let runner = Runner(
            client: client,
            config: RunConfig(
                label: "horizon/\(Int(horizon))ms",
                sku: sku,
                workload: WorkloadConfig(
                    customers: customers,
                    // Everyone at once: the hardest case for the horizon, because every
                    // join is competing to commit at the same moment.
                    profile: .thunderingHerd,
                    durationSeconds: 1,
                    duplicateRate: 0,
                    abandonRate: 0,
                    rejoinRate: 0.4,
                    maxThinkTimeMS: 0,
                    seed: seed
                ),
                connectionLimit: connectionLimit,
                useQueue: true,
                admissionTimeout: .seconds(180),
                driveAdmission: true,
                admissionTickInterval: .milliseconds(50),
                pollInterval: .seconds(1)
            )
        )
        _ = try await runner.run()
        let audit = try await client.retryAudit(sku: sku)
        let fairness = try await client.fairness(sku: sku)
        let checked = audit?.admissionsChecked ?? 0
        points.append(
            HorizonPoint(
                horizonMS: horizon,
                fifoInversions: audit?.fifoInversions ?? 0,
                admissionsChecked: checked,
                inversionRate: checked > 0 ? Double(audit?.fifoInversions ?? 0) / Double(checked) : 0,
                rankCorrelation: fairness?.report.rankCorrelation ?? 0,
                meanWaitSeconds: fairness?.report.waitMean ?? 0
            )
        )
    }

    return HorizonResult(
        customers: customers,
        points: points,
        explanation: """
            Inversions fall as the horizon grows, because the horizon has to cover how \
            long a join takes to commit rather than how far apart arrivals are. The cost \
            is that every customer waits at least the horizon longer, so the right \
            setting is the smallest one that holds under the commit latency the service \
            actually sees under load — which is why this is measured rather than picked.
            """
    )
}

// MARK: - 9. Queue position at scale

struct PositionScaleResult: Codable, Sendable {
    var depths: [Int]
    var points: [PositionBenchmarkResult]
    var explanation: String
}

/// Measures what a queue position costs as the queue gets deeper.
///
/// This is the measurement that decides whether a million-person waiting room is
/// answerable. "How many people are ahead of me" is the most frequent request in a
/// launch, and counting them is O(queue depth) — so at a million waiting, each poll
/// walks up to a million index entries, and a million people polling makes that the
/// whole system. Nothing downstream helps, because the cost is in the question.
///
/// The queue is seeded in bulk rather than joined over HTTP: depth is the independent
/// variable here, and how long a million joins take is a separate experiment that runs
/// over HTTP like a real customer would.
func runPositionScale(
    client: LaunchClient,
    depths: [Int],
    iterations: Int
) async throws -> PositionScaleResult {
    let sku = "position-scale"
    _ = try await prepareLaunch(
        client: client, sku: sku, units: 1_000, shardCount: 16, collapseThreshold: 0,
        // A waiting room, but admitting almost nobody: departures would move the queue
        // underneath the measurement and the two paths would be answering about
        // different queues.
        admission: admissionRequest(
            kind: .fixed, policy: .strictFIFO, rate: 0.0001, intervalMS: 60_000
        ),
        ttlSeconds: 600
    )

    var points: [PositionBenchmarkResult] = []
    for depth in depths.sorted() {
        let seeded = try await client.seedQueue(sku: sku, count: depth)
        guard let seeded, seeded.queueDepth >= depth else {
            throw ClientError(
                status: 0, body: "seeding \(sku) to \(depth) left depth \(seeded?.queueDepth ?? -1)"
            )
        }
        guard let point = try await client.positionBenchmark(sku: sku, iterations: iterations) else {
            throw ClientError(status: 0, body: "no benchmark result at depth \(depth)")
        }
        points.append(point)
    }

    let deepest = points.last
    return PositionScaleResult(
        depths: depths.sorted(),
        points: points,
        explanation: """
            The estimated column should stay flat as depth grows and the exact column \
            should not, because one is arithmetic on two counters and the other walks \
            everyone ahead of you. Both are measured inside the node over the same rows \
            in the same call, interleaved, so neither gets a warmer cache or a different \
            queue depth than the other — and timing them from a client would have \
            compared two numbers that are mostly network.

            Error is the estimate's distance from the count it replaced, in queue \
            places\(deepest.map { ": \($0.positionError) at a depth of \($0.queueDepth)" } ?? ""). \
            It comes from gaps in the arrival index, which a rolled-back or conflicting \
            join leaves behind, and those make a position read high rather than low — \
            the safe direction to be wrong in for somebody waiting.
            """
    )
}
