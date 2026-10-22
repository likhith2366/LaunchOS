import Foundation
import Logging
import NIOCore
import NIOPosix
// RediStack predates strict concurrency checking and does not mark its types Sendable,
// so its Sendable diagnostics are downgraded here rather than silenced project-wide.
@preconcurrency import RediStack

public struct RedisConfig: Sendable {
    public var host: String
    public var port: Int
    public var enabled: Bool

    public init(host: String = "127.0.0.1", port: Int = 6379, enabled: Bool = true) {
        self.host = host
        self.port = port
        self.enabled = enabled
    }

    public static func fromEnvironment() -> RedisConfig {
        func env(_ k: String, _ d: String) -> String { ProcessInfo.processInfo.environment[k] ?? d }
        return RedisConfig(
            host: env("REDIS_HOST", "127.0.0.1"),
            port: Int(env("REDIS_PORT", "6379")) ?? 6379,
            enabled: env("REDIS_ENABLED", "1") != "0"
        )
    }
}

/// Redis: fast, shared, and explicitly not the source of truth.
///
/// Queue positions are cached here so the "where am I in line?" poll — which a waiting
/// customer sends every second or two and which is by far the most frequent request
/// during a launch — does not reach Postgres. Every value is derived and every read is
/// allowed to miss or to be stale, because the durable copy in Postgres is what any
/// decision is made from.
///
/// The test that matters for this component is killing Redis mid-launch: positions go
/// cold, latency rises, and queue order is unchanged.
public final class Coordination: Sendable {
    /// `RedisConnectionPool` is thread-safe — it serializes all of its own state onto
    /// its bound event loop — but is not annotated as such, so the guarantee has to be
    /// asserted here rather than checked by the compiler.
    private nonisolated(unsafe) let pool: RedisConnectionPool?
    private let group: EventLoopGroup
    private let ownsGroup: Bool
    let logger: Logger

    public init(config: RedisConfig, logger: Logger, eventLoopGroup: EventLoopGroup? = nil) {
        self.logger = logger
        if let eventLoopGroup {
            self.group = eventLoopGroup
            self.ownsGroup = false
        } else {
            self.group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
            self.ownsGroup = true
        }
        guard config.enabled else {
            self.pool = nil
            return
        }
        let addresses: [SocketAddress]
        do {
            addresses = [try SocketAddress.makeAddressResolvingHost(config.host, port: config.port)]
        } catch {
            logger.warning("redis address unresolvable, running without cache")
            self.pool = nil
            return
        }
        self.pool = RedisConnectionPool(
            configuration: .init(
                initialServerConnectionAddresses: addresses,
                maximumConnectionCount: .maximumActiveConnections(8),
                connectionFactoryConfiguration: .init(connectionDefaultLogger: logger)
            ),
            boundEventLoop: group.next()
        )
        pool?.activate()
    }

    public var isEnabled: Bool { pool != nil }

    public func shutdown() async {
        pool?.close()
        if ownsGroup {
            try? await group.shutdownGracefully()
        }
    }

    private func positionKey(_ launchID: UUID, _ customerID: String) -> RedisKey {
        RedisKey("launchos:pos:\(launchID.uuidString):\(customerID)")
    }

    /// Caches a position. Failures are swallowed on purpose: a cache that is down must
    /// slow the system, not break it.
    public func cachePosition(launchID: UUID, customerID: String, position: Int, ttl: Duration = .seconds(5)) async {
        guard let pool else { return }
        do {
            try await pool.set(
                positionKey(launchID, customerID),
                to: String(position),
                onCondition: .none,
                expiration: .seconds(Int(max(1, ttl.seconds)))
            ).get()
        } catch {
            await MetricsRegistry.shared.increment(
                "launchos_redis_errors_total", help: "Redis operations that failed and were ignored"
            )
        }
    }

    public func cachedPosition(launchID: UUID, customerID: String) async -> Int? {
        guard let pool else { return nil }
        do {
            let value = try await pool.get(positionKey(launchID, customerID), as: String.self).get()
            if let value, let n = Int(value) {
                await MetricsRegistry.shared.increment(
                    "launchos_position_cache_hits_total", help: "Position polls served from Redis"
                )
                return n
            }
            await MetricsRegistry.shared.increment(
                "launchos_position_cache_misses_total", help: "Position polls that fell through to Postgres"
            )
            return nil
        } catch {
            await MetricsRegistry.shared.increment("launchos_redis_errors_total")
            return nil
        }
    }

    /// Cross-node in-flight checkout count, so the hard ceiling can be enforced over the
    /// cluster rather than per process. Advisory: a lost increment makes the ceiling
    /// slightly wrong, which is why it backs a ceiling and not an invariant.
    public func adjustInFlight(launchID: UUID, by delta: Int) async -> Int? {
        guard let pool else { return nil }
        let key = RedisKey("launchos:inflight:\(launchID.uuidString)")
        do {
            let value = delta >= 0
                ? try await pool.increment(key, by: delta).get()
                : try await pool.decrement(key, by: -delta).get()
            return value
        } catch {
            await MetricsRegistry.shared.increment("launchos_redis_errors_total")
            return nil
        }
    }

    public func inFlight(launchID: UUID) async -> Int? {
        guard let pool else { return nil }
        do {
            let v = try await pool.get(RedisKey("launchos:inflight:\(launchID.uuidString)"), as: String.self).get()
            return v.flatMap { Int($0) }
        } catch {
            return nil
        }
    }

    // MARK: - Queue departure counter

    /// Customers who have left the waiting room for a launch, across all nodes.
    ///
    /// This is what makes a queue position O(1) instead of a count over everyone ahead
    /// of you. It lives in Redis because it has to be shared — each node admits its own
    /// batches, so a per-process counter would undercount by however much the other
    /// nodes did — and because it is incremented once per admitted *batch* rather than
    /// once per customer.
    ///
    /// It is allowed to drift. A lost increment makes positions read slightly high,
    /// which is the safe direction to be wrong in for someone waiting, and
    /// `reconcileDeparted` corrects it from Postgres on a slow timer.
    private func departedKey(_ launchID: UUID) -> RedisKey {
        RedisKey("launchos:departed:\(launchID.uuidString)")
    }

    @discardableResult
    public func addDeparted(launchID: UUID, count: Int) async -> Int? {
        guard let pool, count > 0 else { return nil }
        do {
            return try await pool.increment(departedKey(launchID), by: count).get()
        } catch {
            await MetricsRegistry.shared.increment("launchos_redis_errors_total")
            return nil
        }
    }

    public func departed(launchID: UUID) async -> Int? {
        guard let pool else { return nil }
        do {
            let value = try await pool.get(departedKey(launchID), as: String.self).get()
            return value.flatMap { Int($0) }
        } catch {
            await MetricsRegistry.shared.increment("launchos_redis_errors_total")
            return nil
        }
    }

    /// Overwrites the counter with a figure counted in Postgres.
    ///
    /// Called on a slow timer rather than on the request path: the counter is an
    /// optimisation, and the authoritative answer is always a count of the table.
    public func setDeparted(launchID: UUID, to value: Int) async {
        guard let pool else { return }
        do {
            try await pool.set(departedKey(launchID), to: String(value)).get()
        } catch {
            await MetricsRegistry.shared.increment("launchos_redis_errors_total")
        }
    }

    /// Registers this node as alive, so recovery can tell which reservations belong to
    /// a process that is gone.
    public func heartbeat(nodeID: String, ttl: Duration = .seconds(6)) async {
        guard let pool else { return }
        do {
            try await pool.set(
                RedisKey("launchos:node:\(nodeID)"), to: "1",
                onCondition: .none, expiration: .seconds(Int(max(1, ttl.seconds)))
            ).get()
        } catch {
            await MetricsRegistry.shared.increment("launchos_redis_errors_total")
        }
    }

    /// Nodes that have heartbeated recently.
    ///
    /// SCAN is a cursor, not a query: one call examines a slice of the keyspace and
    /// returns whatever matched in that slice, so it has to be iterated until the cursor
    /// comes back to zero. A single pass over a keyspace holding a few thousand position
    /// keys routinely matched nothing at all.
    ///
    /// That is not a cosmetic bug. `CrashRecovery` reclaims reservations whose owner is
    /// *not* in this list, so a partially iterated result would make it reclaim units
    /// that a live node is still holding — the customer loses a reservation they were
    /// about to complete. (It could not oversell, because the commit is conditional on
    /// the reservation still being held, but losing it is bad enough.)
    public func liveNodes() async -> [String] {
        guard let pool else { return [] }
        let prefix = "launchos:node:"
        var cursor = 0
        var found: Set<String> = []
        // Bounded: a cursor that never returns to zero must not spin forever.
        for _ in 0..<64 {
            do {
                let (next, keys) = try await pool.scan(
                    startingFrom: cursor, matching: "\(prefix)*", count: 250
                ).get()
                for key in keys where key.hasPrefix(prefix) {
                    found.insert(String(key.dropFirst(prefix.count)))
                }
                if next == 0 { break }
                cursor = next
            } catch {
                await MetricsRegistry.shared.increment("launchos_redis_errors_total")
                // A partial answer is worse than none here, for the reason above.
                return []
            }
        }
        return found.sorted()
    }
}
