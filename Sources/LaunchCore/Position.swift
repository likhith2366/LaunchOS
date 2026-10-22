import Foundation
import Logging

/// How a queue position was arrived at, so a caller can tell an estimate from a count.
public enum PositionSource: String, Sendable, Codable {
    /// Counted in Postgres: exact, and O(the number of people ahead of you).
    case exactCount
    /// Derived from counters: O(1), and an estimate.
    case estimated
    /// Served from the Redis position cache.
    case cached
}

public struct QueuePosition: Sendable, Codable {
    public var position: Int
    public var source: PositionSource

    public init(position: Int, source: PositionSource) {
        self.position = position
        self.source = source
    }
}

/// Computes queue positions in constant time.
///
/// ## Why the obvious query does not reach a million
///
/// "How many people are ahead of me" is naturally
/// `SELECT count(*) ... WHERE seq < mine`, and that query is the single thing that stops
/// this system scaling. It is O(the number of people ahead of you), and it is also the
/// most frequent request in a launch: a million people waiting, each polling every few
/// seconds, is a million index range scans per poll interval, each walking up to a
/// million entries. Nothing downstream helps, because the cost is in the question.
///
/// ## What replaces it
///
/// Two numbers, neither of which depends on queue depth:
///
/// - `arrival_index`, the customer's dense 1-based place in this launch's arrival order,
///   handed out at join time by a sequence belonging to that launch and stored on their
///   row.
/// - `departed`, how many customers have left the waiting room. Kept in Redis, and
///   incremented once per admitted *batch* rather than once per customer.
///
/// Then `position ≈ (arrival index − 1) − departed`.
///
/// The per-launch sequence matters more than it looks. The first version of this derived
/// the arrival index from `seq`, the globally shared queue sequence, as
/// `(my seq − the launch's first seq)`. That is dense only while one launch is taking
/// joins: with several, their numbers interleave and the estimate drifts without bound.
/// The test suite, which runs many launches at once, reported position 30 for the fourth
/// customer in a queue of sixty. A sequence per launch is dense by construction, and
/// sequences cost no row lock to draw from.
///
/// ## What that costs in accuracy
///
/// Worth stating plainly, because this is an estimate shown to a customer as a number.
/// Two sources of error:
///
/// 1. **Index gaps.** Sequences are non-transactional, so a rolled-back join — or a
///    retry whose insert hit the conflict branch — leaves a permanent hole in the index.
///    This makes the estimate read high, never low.
/// 2. **Out-of-order departures.** Subtracting a total assumes everyone who left was
///    ahead of you. Under strict FIFO that is true. Under a lottery it is not — some of
///    them were behind you — so the estimate reads low near the front of the queue and
///    high near the back.
///
/// The first error is in the safe direction: being told you are further back than you
/// are disappoints nobody who then gets in sooner. Neither is assumed to be small —
/// `launch-sim position-scale` measures the estimate against the exact count across
/// queue depths, and the exact count is always available on request, so the error is a
/// number rather than a hope.
///
/// This is also what real waiting rooms do: they show an approximate position, or only
/// an estimated wait, because the exact rank is both expensive to compute and stale by
/// the time it reaches the browser.
public actor PositionEstimator {
    private struct Baseline {
        var departed: Int
        var refreshedAt: ContinuousClock.Instant
    }

    private let room: WaitingRoom
    private let coordination: Coordination
    private let logger: Logger
    /// How stale the reconciled baseline may be. The Redis counter moves between
    /// refreshes; this bounds how far the baseline can drift from the table.
    private let refreshInterval: Duration
    private var baselines: [UUID: Baseline] = [:]

    public init(
        room: WaitingRoom,
        coordination: Coordination,
        logger: Logger,
        refreshInterval: Duration = .seconds(5)
    ) {
        self.room = room
        self.coordination = coordination
        self.logger = logger
        self.refreshInterval = refreshInterval
    }

    /// Records that `count` customers left the waiting room, so positions move without
    /// anyone having to count the table.
    public func recordDepartures(launchID: UUID, count: Int) async {
        guard count > 0 else { return }
        await coordination.addDeparted(launchID: launchID, count: count)
        if var baseline = baselines[launchID] {
            baseline.departed += count
            baselines[launchID] = baseline
        }
    }

    /// Position for one customer, in constant time.
    ///
    /// Falls back to the exact count when the customer has no arrival index, meaning a
    /// launch created before its arrival sequence existed. A wrong answer is worse than a
    /// slow one, and that case does not arise for launches this version created.
    public func position(
        launchID: UUID,
        seq: Int64,
        arrivalIndex: Int64
    ) async throws -> QueuePosition {
        guard arrivalIndex > 0 else {
            return QueuePosition(
                position: try await room.position(launchID: launchID, seq: seq),
                source: .exactCount
            )
        }
        let baseline = try await baseline(for: launchID)
        // Redis wins over the local figure when it answers, because it carries every
        // node's increments and the local one carries only this node's.
        let departed = await coordination.departed(launchID: launchID) ?? baseline.departed
        // The index is 1-based, so subtracting one gives how many arrived before this
        // customer.
        let ahead = Int(arrivalIndex - 1) - departed
        return QueuePosition(position: max(0, ahead), source: .estimated)
    }

    private func baseline(for launchID: UUID) async throws -> Baseline {
        if let baseline = baselines[launchID],
            baseline.refreshedAt.duration(to: .now) < refreshInterval
        {
            return baseline
        }
        let departed = try await room.departedCount(launchID: launchID)
        // Pushing the counted figure back is how drift from a lost increment is
        // corrected, and it is why the counter is allowed to be approximate at all.
        await coordination.setDeparted(launchID: launchID, to: departed)
        let baseline = Baseline(departed: departed, refreshedAt: .now)
        baselines[launchID] = baseline
        return baseline
    }

    public func forget(launchID: UUID) {
        baselines[launchID] = nil
    }

    public func forgetAll() {
        baselines.removeAll()
    }
}
