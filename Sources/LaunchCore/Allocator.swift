import Foundation
import Logging

/// Caches the immutable parts of a launch's configuration, plus the one mutable bit
/// (`collapsed`) that the hot path has to know about.
///
/// Deliberately not where allocation happens: an actor serializes its callers, so
/// putting `reserve` on one would reintroduce by hand exactly the serialization that
/// sharding exists to remove. The cache is only touched for a dictionary read.
actor LaunchConfigCache {
    private var configs: [UUID: Launch] = [:]

    func cached(_ id: UUID) -> Launch? { configs[id] }
    func store(_ launch: Launch) { configs[launch.id] = launch }
    func invalidate(_ id: UUID) { configs[id] = nil }
}

public struct AllocatorConfig: Sendable {
    /// How many shards a single request will try before giving up. The cap is what keeps
    /// the end of a launch from turning into a probe storm: without it, every request
    /// would scan all N shards once stock runs low, turning one failed allocation into N
    /// round-trips.
    public var maxProbes: Int
    /// Whether to merge shards back into one row when stock runs low.
    public var collapseEnabled: Bool

    public init(maxProbes: Int = 4, collapseEnabled: Bool = true) {
        self.maxProbes = maxProbes
        self.collapseEnabled = collapseEnabled
    }
}

/// Allocates units from sharded inventory.
///
/// The whole design exists to avoid one row that every request has to lock. Units are
/// split across `shardCount` rows; a request picks a starting shard from a hash of its
/// customer id and probes forward from there, so load spreads evenly without any
/// coordination between requests.
public final class Allocator: Sendable {
    let store: Store
    let nodeID: String
    /// Public so the node can report how it was configured; the probe limit is part of
    /// what a measurement has to state.
    public let config: AllocatorConfig
    let logger: Logger
    private let cache = LaunchConfigCache()

    public init(store: Store, nodeID: String, config: AllocatorConfig = AllocatorConfig(), logger: Logger) {
        self.store = store
        self.nodeID = nodeID
        self.config = config
        self.logger = logger
    }

    func launchConfig(_ id: UUID, refresh: Bool = false) async throws -> Launch {
        if !refresh, let cached = await cache.cached(id) { return cached }
        // Loaded outside the cache actor on purpose: awaiting a database round-trip
        // while holding the actor would serialize every concurrent caller behind it.
        let launch = try await store.launch(id: id)
        await cache.store(launch)
        return launch
    }

    /// Deterministic probe sequence over the shards.
    ///
    /// `start + i * step` with `step` coprime to `shardCount` visits every shard exactly
    /// once, so the sequence terminates and a bounded sweep never retries a shard it has
    /// already tried. Both are derived from a stable hash of the customer id, so the same
    /// customer probes the same shards on a retry — which keeps a retrying client from
    /// wandering onto a different shard and succeeding where its first attempt failed,
    /// and makes a failed run reproducible.
    public static func probeOrder(customerID: String, shardCount: Int, limit: Int) -> [Int] {
        guard shardCount > 1 else { return [0] }
        let h = stableHash(customerID)
        let start = Int(h % UInt64(shardCount))
        var step = Int((h >> 32) % UInt64(shardCount - 1)) + 1
        while gcd(step, shardCount) != 1 { step = step % (shardCount - 1) + 1 }
        let count = min(limit, shardCount)
        return (0..<count).map { (start + $0 * step) % shardCount }
    }

    /// Reserves `units` for a customer, or reports that the launch is sold out.
    ///
    /// Each probe is a single statement that conditionally decrements one shard and
    /// inserts the reservation in the same commit, so a probe either takes a unit and
    /// records who holds it or writes nothing at all.
    public func reserve(
        launchID: UUID,
        customerID: String,
        units: Int = 1
    ) async throws -> AllocationOutcome {
        var launch = try await launchConfig(launchID)
        var totalProbes = 0
        var collapsedHere = false

        for attempt in 0..<2 {
            // The whole probe set goes to the database as one statement, which takes the
            // first shard in this order that has stock. Four shards tried one statement
            // at a time cost four round-trips whether the first succeeded or none did,
            // and on the sold-out path — most requests, late in a launch — that was the
            // entire per-request cost.
            let shards = launch.collapsed
                ? [0]
                : Self.probeOrder(
                    customerID: customerID, shardCount: launch.shardCount, limit: config.maxProbes
                )
            totalProbes += shards.count
            if let reservation = try await store.probe(
                launchID: launchID, shardIDs: shards, customerID: customerID, units: units,
                ownerNode: nodeID, ttl: launch.reservationTTL
            ) {
                await MetricsRegistry.shared.observe(
                    "launchos_allocation_probes", Double(shards.count),
                    labels: ["sku": launch.sku],
                    help: "Shards offered per allocation attempt"
                )
                return AllocationOutcome(
                    reservation: reservation, probes: totalProbes,
                    collapsedDuringCall: collapsedHere
                )
            }

            // Every shard this request was willing to try came back empty. That is either
            // a genuine sell-out or tail fragmentation: stock exists, but not on the
            // shards this customer probes. The two need different answers, so find out
            // which it is before reporting failure.
            guard attempt == 0 else { break }
            // Remaining stock and the collapsed flag come back together, because the
            // decision below needs both and reading them separately let the two answers
            // come from different instants.
            let (remaining, alreadyCollapsed) = try await store.availability(launchID: launchID)
            if remaining == 0 { break }

            if alreadyCollapsed {
                // Another request collapsed it while this one was probing; retry against
                // the merged row.
                launch.collapsed = true
                await cache.store(launch)
                continue
            }

            if config.collapseEnabled, launch.collapseThreshold > 0, remaining <= launch.collapseThreshold {
                if try await store.collapseShards(launchID: launchID) {
                    collapsedHere = true
                    await MetricsRegistry.shared.increment(
                        "launchos_shard_collapses_total", labels: ["sku": launch.sku],
                        help: "Times a launch's shards were merged back into one row"
                    )
                    logger.info(
                        "collapsed shards",
                        metadata: [
                            "sku": .string(launch.sku), "remaining": .stringConvertible(remaining),
                        ]
                    )
                }
                launch.collapsed = true
                await cache.store(launch)
                continue
            }

            // Stock exists above the collapse threshold but not where this customer
            // looked. Reported as a distinct outcome rather than folded into sold-out,
            // because this is the cost of sharding and the measurement needs to see it.
            await MetricsRegistry.shared.increment(
                "launchos_fragmentation_failures_total", labels: ["sku": launch.sku],
                help: "Allocations that failed while stock remained on shards outside the probe set"
            )
            return AllocationOutcome(
                reservation: nil, probes: totalProbes, collapsedDuringCall: collapsedHere,
                remainingWhenFailed: remaining
            )
        }

        await MetricsRegistry.shared.increment(
            "launchos_soldout_total", labels: ["sku": launch.sku],
            help: "Allocations that failed with no stock left anywhere"
        )
        return AllocationOutcome(
            reservation: nil, probes: totalProbes, collapsedDuringCall: collapsedHere,
            remainingWhenFailed: 0
        )
    }

    public func cancel(reservationID: UUID) async throws -> Bool {
        try await store.releaseReservation(reservationID: reservationID, as: .cancelled)
    }

    public func invalidateCache(launchID: UUID) async {
        await cache.invalidate(launchID)
    }
}

func gcd(_ a: Int, _ b: Int) -> Int {
    var (a, b) = (abs(a), abs(b))
    while b != 0 { (a, b) = (b, a % b) }
    return a
}
