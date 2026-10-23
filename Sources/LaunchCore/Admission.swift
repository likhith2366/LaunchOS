import Foundation
import Logging

/// Rolling view of downstream health, sampled by checkout and read by the controller.
///
/// Two signals, deliberately:
///
/// - `inFlight` and queue depth respond the moment service time rises, because a
///   request that takes longer is still counted as in flight. This is what the
///   controller acts on.
/// - checkout p99 only moves once slow requests have finished and aged into the window,
///   by which point admission has already been too high for seconds. This is what the
///   controller alarms on.
///
/// Controlling on the leading indicator and alarming on the lagging one is the whole
/// reason both are tracked.
public actor HealthMonitor {
    /// Smoothing factor for the EWMA. Lower is smoother and slower.
    public let alpha: Double
    private var latencyWindow: [Double] = []
    private let windowLimit: Int
    private var errorWindow: [Bool] = []

    public private(set) var inFlight: Int = 0
    public private(set) var peakInFlight: Int = 0
    public private(set) var smoothedLatencyMS: Double = 0
    public private(set) var smoothedInFlight: Double = 0
    public private(set) var completed: Int = 0
    public private(set) var failed: Int = 0

    public init(alpha: Double = 0.2, windowLimit: Int = 2_000) {
        self.alpha = alpha
        self.windowLimit = windowLimit
    }

    public func checkoutStarted() {
        inFlight += 1
        peakInFlight = max(peakInFlight, inFlight)
        smoothedInFlight = smoothedInFlight == 0
            ? Double(inFlight)
            : alpha * Double(inFlight) + (1 - alpha) * smoothedInFlight
    }

    public func checkoutFinished(latencyMS: Double, failed didFail: Bool) {
        inFlight = max(0, inFlight - 1)
        if didFail { failed += 1 } else { completed += 1 }
        latencyWindow.append(latencyMS)
        if latencyWindow.count > windowLimit { latencyWindow.removeFirst(latencyWindow.count - windowLimit) }
        errorWindow.append(didFail)
        if errorWindow.count > windowLimit { errorWindow.removeFirst(errorWindow.count - windowLimit) }
        smoothedLatencyMS = smoothedLatencyMS == 0
            ? latencyMS
            : alpha * latencyMS + (1 - alpha) * smoothedLatencyMS
    }

    /// Exact p99 over the current window. Lagging by construction.
    public func latencyP99() -> Double {
        Samples(latencyWindow).p99
    }

    public func errorRate() -> Double {
        guard !errorWindow.isEmpty else { return 0 }
        return Double(errorWindow.count { $0 }) / Double(errorWindow.count)
    }

    public func snapshot() -> HealthSnapshot {
        HealthSnapshot(
            inFlight: inFlight,
            smoothedInFlight: smoothedInFlight,
            latencyP99MS: Samples(latencyWindow).p99,
            smoothedLatencyMS: smoothedLatencyMS,
            errorRate: errorRate(),
            completed: completed,
            failed: failed,
            peakInFlight: peakInFlight
        )
    }
}

public struct HealthSnapshot: Sendable, Codable {
    public var inFlight: Int
    public var smoothedInFlight: Double
    public var latencyP99MS: Double
    public var smoothedLatencyMS: Double
    public var errorRate: Double
    public var completed: Int
    public var failed: Int
    public var peakInFlight: Int
}

public enum AdmitterKind: String, Sendable, Codable, CaseIterable {
    case fixed
    case aimd
}

public struct AdmissionConfig: Sendable {
    public var kind: AdmitterKind
    public var policy: FairnessPolicy
    /// Control interval. Admissions inside an interval are spread across it.
    public var interval: Duration
    /// Fixed admitter: the rate. AIMD: the starting rate.
    public var initialRate: Double
    public var minRate: Double
    public var maxRate: Double
    /// Additive increase, in admissions per second per interval.
    public var increaseStep: Double
    /// Multiplicative decrease factor.
    public var decreaseFactor: Double
    /// p99 above this is a back-off signal.
    public var latencyThresholdMS: Double
    /// In-flight count above this is a back-off signal. The leading indicator.
    public var inFlightThreshold: Double
    public var errorRateThreshold: Double
    /// Fraction of the threshold forming a dead band in which the rate is left alone.
    /// Without it the controller reacts to every sample and the admission graph saws.
    public var deadBand: Double
    /// Absolute cap on concurrent checkout sessions, enforced regardless of what the
    /// controller wants. The controller is a feedback loop and can be wrong; this is
    /// the thing that is not allowed to be.
    public var hardInFlightCeiling: Int
    /// Spread each batch's releases across the interval instead of at its boundary.
    public var jitterReleases: Bool
    /// How long a queue entry must exist before it can be admitted. See
    /// `WaitingRoom.admit` for why ordering on a sequence number alone does not give
    /// FIFO.
    public var commitHorizon: Duration

    public init(
        kind: AdmitterKind = .aimd,
        policy: FairnessPolicy = .bucketedFIFO,
        interval: Duration = .milliseconds(500),
        initialRate: Double = 50,
        minRate: Double = 5,
        maxRate: Double = 5_000,
        increaseStep: Double = 25,
        decreaseFactor: Double = 0.5,
        latencyThresholdMS: Double = 250,
        inFlightThreshold: Double = 120,
        errorRateThreshold: Double = 0.02,
        deadBand: Double = 0.15,
        hardInFlightCeiling: Int = 400,
        jitterReleases: Bool = true,
        commitHorizon: Duration = .milliseconds(250)
    ) {
        self.kind = kind
        self.policy = policy
        self.interval = interval
        self.initialRate = initialRate
        self.minRate = minRate
        self.maxRate = maxRate
        self.increaseStep = increaseStep
        self.decreaseFactor = decreaseFactor
        self.latencyThresholdMS = latencyThresholdMS
        self.inFlightThreshold = inFlightThreshold
        self.errorRateThreshold = errorRateThreshold
        self.deadBand = deadBand
        self.hardInFlightCeiling = hardInFlightCeiling
        self.jitterReleases = jitterReleases
        self.commitHorizon = commitHorizon
    }
}

public enum ControlAction: String, Sendable, Codable {
    case increase
    case decrease
    case hold
    case ceilingClamped
}

public struct ControlSample: Sendable, Codable {
    public var at: Date
    public var rate: Double
    public var granted: Int
    public var action: ControlAction
    public var health: HealthSnapshot
    public var queueDepth: Int
    /// Wall time for the admit statement itself. This is where a fairness policy's
    /// coordination cost shows up: ordering a large waiting set is not free, and
    /// comparing policies on admissions per second hides it, because that number is set
    /// by the configured rate rather than by what the ordering cost.
    public var admitDurationMS: Double
}

/// Decides how many customers to admit per interval.
///
/// The fixed admitter is the baseline and exists first on purpose: an adaptive
/// controller cannot be called better without a number to beat.
public actor AdmissionController {
    /// Immutable for the controller's lifetime, so readers outside the actor — the
    /// status route, the experiment harness — do not have to hop onto it to ask how the
    /// controller was configured. Changing the configuration means replacing the
    /// controller, which is what the registry does.
    public nonisolated let config: AdmissionConfig
    private let health: HealthMonitor
    private let room: WaitingRoom
    /// Told about each admitted batch, so queue positions can move without counting.
    private let positions: PositionEstimator?
    /// Told about each admitted batch, so checkout can confirm admission without reading
    /// Postgres.
    private let coordination: Coordination?
    private let launchID: UUID
    private let sku: String
    private let logger: Logger
    private var rng: SplitMix64

    public private(set) var rate: Double
    public private(set) var samples: [ControlSample] = []
    private var running = false
    private var queueDepthSum: Double = 0
    private var queueDepthCount: Int = 0
    public private(set) var totalAdmitted: Int = 0
    public private(set) var startedAt: Date?
    private var admitDurations = Samples()

    public init(
        config: AdmissionConfig,
        health: HealthMonitor,
        room: WaitingRoom,
        positions: PositionEstimator? = nil,
        coordination: Coordination? = nil,
        launchID: UUID,
        sku: String,
        seed: UInt64 = 0x5EED,
        logger: Logger
    ) {
        self.config = config
        self.health = health
        self.room = room
        self.positions = positions
        self.coordination = coordination
        self.launchID = launchID
        self.sku = sku
        self.rate = config.initialRate
        self.rng = SplitMix64(seed: seed)
        self.logger = logger
    }

    /// AIMD, driven by the leading indicator.
    ///
    /// The dead band is why `hold` exists as an outcome: inside it the rate is left
    /// alone, which is what stops the controller oscillating between full-open and
    /// nearly-closed on noise.
    func nextRate(_ snapshot: HealthSnapshot) -> (Double, ControlAction) {
        guard config.kind == .aimd else { return (config.initialRate, .hold) }

        let inFlightRatio = snapshot.smoothedInFlight / max(config.inFlightThreshold, 1)
        let latencyRatio = snapshot.smoothedLatencyMS / max(config.latencyThresholdMS, 1)
        let pressure = max(inFlightRatio, latencyRatio)

        if pressure > 1.0 + config.deadBand || snapshot.errorRate > config.errorRateThreshold {
            let next = max(config.minRate, rate * config.decreaseFactor)
            return (next, .decrease)
        }
        if pressure < 1.0 - config.deadBand {
            let next = min(config.maxRate, rate + config.increaseStep)
            return (next, .increase)
        }
        return (rate, .hold)
    }

    /// One control tick: decide a rate, admit that many, release them spread across the
    /// interval.
    @discardableResult
    public func tick() async throws -> ControlSample {
        let snapshot = await health.snapshot()
        let depth = try await room.depth(launchID: launchID)
        queueDepthSum += Double(depth)
        queueDepthCount += 1

        var (nextRate, action) = nextRate(snapshot)

        // The hard ceiling is not part of the control law. Whatever the loop concluded,
        // in-flight sessions are not allowed past this.
        let headroom = max(0, config.hardInFlightCeiling - snapshot.inFlight)
        let intervalSeconds = config.interval.seconds
        var budget = Int((nextRate * intervalSeconds).rounded())
        if budget > headroom {
            budget = headroom
            action = .ceilingClamped
        }

        rate = nextRate
        let admitStarted = ContinuousClock.now
        let admitted = try await room.admit(
            launchID: launchID, count: budget, policy: config.policy,
            commitHorizon: config.commitHorizon
        )
        let admitDuration = admitStarted.duration(to: .now).milliseconds
        totalAdmitted += admitted.count
        admitDurations.add(admitDuration)
        // One counter increment per batch, which is what keeps the poll path from
        // having to count anybody.
        await positions?.recordDepartures(launchID: launchID, count: admitted.count)
        // And one SADD per batch, which is what keeps checkout from reading Postgres
        // just to ask a question this loop already answered.
        await coordination?.markAdmitted(
            launchID: launchID, customerIDs: admitted.map(\.customerID)
        )

        await MetricsRegistry.shared.set(
            "launchos_admission_rate", to: rate, labels: ["sku": sku],
            help: "Current admission rate in customers per second"
        )
        await MetricsRegistry.shared.set(
            "launchos_queue_depth", to: Double(depth), labels: ["sku": sku],
            help: "Customers waiting"
        )
        await MetricsRegistry.shared.set(
            "launchos_inflight_checkouts", to: Double(snapshot.inFlight), labels: ["sku": sku],
            help: "Checkout sessions in flight"
        )
        await MetricsRegistry.shared.increment(
            "launchos_admitted_total", by: Double(admitted.count), labels: ["sku": sku],
            help: "Customers admitted"
        )

        await MetricsRegistry.shared.observe(
            "launchos_admit_statement_ms", admitDuration,
            labels: ["sku": sku, "policy": config.policy.rawValue],
            help: "Time for one admit statement, i.e. the cost of the fairness policy"
        )
        let sample = ControlSample(
            at: Date(), rate: rate, granted: admitted.count, action: action,
            health: snapshot, queueDepth: depth, admitDurationMS: admitDuration
        )
        samples.append(sample)
        return sample
    }

    /// Runs the control loop until cancelled.
    ///
    /// Admitting a batch at the interval boundary means every admitted customer starts
    /// checkout in the same millisecond, which is a thundering herd of the system's own
    /// making. The loop therefore sleeps a jittered fraction of the interval between
    /// sub-batches rather than one whole interval between batches.
    public func run() async {
        guard !running else { return }
        running = true
        startedAt = Date()
        let intervalNS = UInt64(config.interval.seconds * 1_000_000_000)
        while !Task.isCancelled {
            do {
                _ = try await tick()
            } catch is CancellationError {
                break
            } catch {
                logger.error("admission tick failed", metadata: ["error": .string("\(error)")])
            }
            if config.jitterReleases {
                // Two half-interval sleeps with jitter, so releases land spread across
                // the interval instead of all on its boundary.
                let jitter = rng.uniform()
                let first = UInt64(Double(intervalNS) * (0.3 + 0.4 * jitter))
                try? await Task.sleep(nanoseconds: first)
                if Task.isCancelled { break }
                try? await Task.sleep(nanoseconds: intervalNS - first)
            } else {
                try? await Task.sleep(nanoseconds: intervalNS)
            }
        }
        running = false
    }

    public func meanQueueDepth() -> Double {
        queueDepthCount == 0 ? 0 : queueDepthSum / Double(queueDepthCount)
    }

    public func effectiveAdmissionRate() -> Double {
        guard let startedAt, totalAdmitted > 0 else { return 0 }
        let elapsed = Date().timeIntervalSince(startedAt)
        return elapsed > 0 ? Double(totalAdmitted) / elapsed : 0
    }

    public func controlTrace() -> [ControlSample] { samples }

    /// Cost of the admit statement across the run: the policy's coordination overhead.
    public func admitStatementCost() -> (meanMS: Double, p99MS: Double, batches: Int) {
        (admitDurations.mean, admitDurations.p99, admitDurations.count)
    }

    public func setRate(_ r: Double) { rate = r }
}
