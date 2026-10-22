import Foundation
import Logging

public struct SweeperConfig: Sendable {
    public var strategy: SweepStrategy
    public var batchSize: Int
    public var interval: Duration

    public init(
        strategy: SweepStrategy = .bucketed,
        batchSize: Int = 500,
        interval: Duration = .milliseconds(500)
    ) {
        self.strategy = strategy
        self.batchSize = batchSize
        self.interval = interval
    }
}

/// Returns abandoned reservations to the pool.
///
/// This is what stops a launch looking sold out while a thousand customers sit on
/// reservations they never completed, and it is also the safety net for the crash
/// window: a unit reserved by a worker that died has no order to complete it, and the
/// only thing that ever brings it back is expiry.
///
/// It sweeps in bounded batches because the alternative — one query that reclaims
/// everything expired — holds locks proportional to the backlog and, with a hundred
/// thousand reservations outstanding, stalls allocation while it runs.
public actor Sweeper {
    private let store: Store
    private let config: SweeperConfig
    private let logger: Logger
    public private(set) var totalReclaimed = 0
    public private(set) var sweeps = 0
    public private(set) var durations = Samples()

    public init(store: Store, config: SweeperConfig = SweeperConfig(), logger: Logger) {
        self.store = store
        self.config = config
        self.logger = logger
    }

    @discardableResult
    public func sweepOnce(launchID: UUID? = nil) async throws -> SweepResult {
        let result = try await store.sweepExpired(
            strategy: config.strategy, batchSize: config.batchSize, launchID: launchID
        )
        sweeps += 1
        totalReclaimed += result.reclaimed
        durations.add(result.durationMS)
        if result.reclaimed > 0 {
            await MetricsRegistry.shared.increment(
                "launchos_reservations_expired_total", by: Double(result.reclaimed),
                help: "Reservations reclaimed by the expiry sweeper"
            )
            await MetricsRegistry.shared.observe(
                "launchos_sweep_duration_ms", result.durationMS,
                help: "Time for one expiry sweep batch"
            )
        }
        return result
    }

    public func run(launchID: UUID? = nil) async {
        let intervalNS = UInt64(config.interval.seconds * 1_000_000_000)
        while !Task.isCancelled {
            do {
                let result = try await sweepOnce(launchID: launchID)
                // A full batch means there is more waiting; go straight round again
                // rather than sleeping while the backlog grows.
                if result.reclaimed >= config.batchSize { continue }
            } catch {
                logger.error("sweep failed", metadata: ["error": .string("\(error)")])
            }
            try? await Task.sleep(nanoseconds: intervalNS)
        }
    }

    public func stats() -> (sweeps: Int, reclaimed: Int, p99MS: Double, meanMS: Double) {
        (sweeps, totalReclaimed, durations.p99, durations.mean)
    }
}

/// Reclaims reservations held by processes that are no longer alive, without waiting for
/// each reservation's TTL.
///
/// Expiry alone is already correct, so this is purely about how long a unit stays stuck:
/// a 90-second TTL means 90 seconds of a dead worker's inventory being invisible. Node
/// liveness comes from Redis heartbeats, which is safe here because being wrong only
/// costs an early reclaim of a reservation whose owner cannot commit it anyway — the
/// commit is still conditional on the reservation being held and unexpired.
public struct CrashRecovery: Sendable {
    let store: Store
    let coordination: Coordination
    let logger: Logger

    public init(store: Store, coordination: Coordination, logger: Logger) {
        self.store = store
        self.coordination = coordination
        self.logger = logger
    }

    public func recoverOrphans(launchID: UUID) async throws -> Int {
        let live = await coordination.liveNodes()
        // No heartbeats at all means Redis is down, not that every node is dead.
        // Reclaiming everything here would be catastrophic, so do nothing and let TTL
        // expiry handle it.
        guard !live.isEmpty else { return 0 }
        let orphans = try await store.orphanedReservations(excludingNodes: live, launchID: launchID)
        var reclaimed = 0
        for id in orphans {
            if try await store.releaseReservation(reservationID: id, as: .expired) { reclaimed += 1 }
        }
        if reclaimed > 0 {
            logger.warning(
                "reclaimed orphaned reservations",
                metadata: ["count": .stringConvertible(reclaimed), "live_nodes": .stringConvertible(live.count)]
            )
            await MetricsRegistry.shared.increment(
                "launchos_orphans_reclaimed_total", by: Double(reclaimed),
                help: "Reservations reclaimed from nodes that stopped heartbeating"
            )
        }
        return reclaimed
    }
}
