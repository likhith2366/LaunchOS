import Foundation
import Logging

/// One depth's worth of the position measurement.
public struct PositionBenchmarkResult: Sendable, Codable {
    public var queueDepth: Int
    public var iterations: Int
    /// Constant-time estimate, measured inside the node.
    public var estimatedMedianMS: Double
    public var estimatedP99MS: Double
    /// The count it replaced, over the same rows at the same moment.
    public var exactMedianMS: Double
    public var exactP99MS: Double
    public var speedup: Double
    public var estimatedPosition: Int
    public var exactPosition: Int
    /// Distance between the two, in queue places.
    public var positionError: Int
    public var positionErrorPercent: Double

    public init(
        queueDepth: Int,
        iterations: Int,
        estimated: Samples,
        exact: Samples,
        estimatedPosition: Int,
        exactPosition: Int
    ) {
        self.queueDepth = queueDepth
        self.iterations = iterations
        self.estimatedMedianMS = estimated.p50
        self.estimatedP99MS = estimated.p99
        self.exactMedianMS = exact.p50
        self.exactP99MS = exact.p99
        self.speedup = estimated.p50 > 0 ? exact.p50 / estimated.p50 : 0
        self.estimatedPosition = estimatedPosition
        self.exactPosition = exactPosition
        self.positionError = abs(estimatedPosition - exactPosition)
        self.positionErrorPercent =
            exactPosition > 0 ? Double(abs(estimatedPosition - exactPosition)) / Double(exactPosition) * 100 : 0
    }
}

/// Times the two ways of answering "how many people are ahead of me", inside the node.
///
/// Measured server-side on purpose. The work being compared is on the order of a
/// millisecond, and an HTTP round-trip plus a `curl` process start on this host is
/// several times that — a client-side benchmark shows two near-identical numbers and
/// proves nothing. Timing it here isolates the thing that actually differs.
///
/// Both paths run against the same rows, interleaved, in the same call, so neither gets
/// a warmer cache or a different queue depth than the other.
public struct PositionBenchmark: Sendable {
    let room: WaitingRoom
    let estimator: PositionEstimator
    let logger: Logger

    public init(room: WaitingRoom, estimator: PositionEstimator, logger: Logger) {
        self.room = room
        self.estimator = estimator
        self.logger = logger
    }

    /// - Parameter customerID: whose position to ask for. The caller should pick the
    ///   customer at the back of the queue: the worst case for the count, and no harder
    ///   for the estimate.
    public func run(
        launchID: UUID,
        customerID: String,
        iterations: Int
    ) async throws -> PositionBenchmarkResult {
        guard let entry = try await room.entry(launchID: launchID, customerID: customerID) else {
            throw LaunchError.notAdmitted(customerID)
        }
        let depth = try await room.depth(launchID: launchID)

        var estimated = Samples()
        var exact = Samples()
        var lastEstimated = 0
        var lastExact = 0

        // One warm-up of each, so neither figure carries first-call costs that the other
        // has already paid — the estimator's baseline read, and the planner's first look
        // at the count.
        _ = try await estimator.position(
            launchID: launchID, seq: entry.seq, arrivalIndex: entry.arrivalIndex
        )
        _ = try await room.position(launchID: launchID, seq: entry.seq)

        for _ in 0..<max(1, iterations) {
            var clock = ContinuousClock.now
            let estimate = try await estimator.position(
                launchID: launchID, seq: entry.seq, arrivalIndex: entry.arrivalIndex
            )
            estimated.add(clock.duration(to: .now).milliseconds)
            lastEstimated = estimate.position

            clock = .now
            lastExact = try await room.position(launchID: launchID, seq: entry.seq)
            exact.add(clock.duration(to: .now).milliseconds)
        }

        return PositionBenchmarkResult(
            queueDepth: depth,
            iterations: iterations,
            estimated: estimated,
            exact: exact,
            estimatedPosition: lastEstimated,
            exactPosition: lastExact
        )
    }
}
