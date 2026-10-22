import Foundation
import Logging

public struct ShardLease: Sendable, Equatable {
    public var shardID: Int
    public var fenceToken: Int64
    public var expiresAt: Date
}

/// How several allocator processes avoid double-allocating the same unit.
public enum MultiNodeMode: String, Sendable, CaseIterable, Codable {
    /// Every node issues conditional updates against the same shard rows. Correct
    /// without any coordination, because the database serializes the contended rows —
    /// which is also exactly why it is the slower of the two.
    case postgresArbitrated
    /// Each node leases a disjoint subset of shards and allocates only from those, so
    /// two nodes never touch the same row. Less contention, at the price of reclaim
    /// latency when a node dies holding a lease.
    case leasedShards
}

/// Lease-based shard ownership.
///
/// A lease is a row with an owner, an expiry and a fence token. The token comes from a
/// sequence and is re-issued only when ownership changes, so a node that was paused long
/// enough for its lease to expire and be taken can be told its token is stale instead of
/// continuing to act as the owner.
public struct LeaseManager: Sendable {
    let store: Store
    let nodeID: String
    let logger: Logger
    public let ttl: Duration

    public init(store: Store, nodeID: String, logger: Logger, ttl: Duration = .seconds(5)) {
        self.store = store
        self.nodeID = nodeID
        self.logger = logger
        self.ttl = ttl
    }

    /// Takes or renews the lease on one shard.
    ///
    /// The WHERE clause on the conflict branch is the whole mechanism: an update only
    /// happens if the lease has expired or this node already holds it, so a live owner
    /// is never displaced. A new fence token is issued only on a change of owner,
    /// because renewing is not a change of ownership and should not invalidate work the
    /// owner already started.
    public func acquire(launchID: UUID, shardID: Int) async throws -> ShardLease? {
        let expires = Date().addingTimeInterval(ttl.seconds)
        let rows = try await store.client.query(
            """
            INSERT INTO shard_leases (launch_id, shard_id, owner_node, fence_token, expires_at)
            VALUES (\(launchID), \(shardID), \(nodeID), nextval('fence_seq'), \(expires))
            ON CONFLICT (launch_id, shard_id) DO UPDATE
               SET owner_node = \(nodeID),
                   expires_at = \(expires),
                   fence_token = CASE
                       WHEN shard_leases.owner_node = \(nodeID) THEN shard_leases.fence_token
                       ELSE nextval('fence_seq')
                   END
             WHERE shard_leases.expires_at < now() OR shard_leases.owner_node = \(nodeID)
            RETURNING fence_token, expires_at
            """,
            logger: logger
        )
        for try await (token, expiresAt) in rows.decode((Int64, Date).self) {
            return ShardLease(shardID: shardID, fenceToken: token, expiresAt: expiresAt)
        }
        return nil
    }

    /// Claims this node's share of the shards.
    ///
    /// Each node starts scanning at a different offset so that `n` nodes starting at the
    /// same moment do not all contend for shard 0 and then walk the list in lockstep.
    public func acquireShare(launchID: UUID, shardCount: Int, nodeIndex: Int, nodeCount: Int) async throws -> [ShardLease] {
        var held: [ShardLease] = []
        let offset = nodeCount > 0 ? (nodeIndex * shardCount) / max(nodeCount, 1) : 0
        for i in 0..<shardCount {
            let shard = (offset + i) % shardCount
            if let lease = try await acquire(launchID: launchID, shardID: shard) {
                held.append(lease)
            }
            // Stop once this node holds roughly its fair share, leaving the rest for
            // the others. A node only grabs everything if nobody else is alive to take it.
            if nodeCount > 1, held.count >= Int((Double(shardCount) / Double(nodeCount)).rounded(.up)) {
                break
            }
        }
        return held
    }

    public func renew(launchID: UUID, leases: [ShardLease]) async throws -> [ShardLease] {
        var renewed: [ShardLease] = []
        for lease in leases {
            if let r = try await acquire(launchID: launchID, shardID: lease.shardID) {
                // A changed token means the lease lapsed and someone else took it in
                // between; this node no longer owns the shard.
                if r.fenceToken == lease.fenceToken {
                    renewed.append(r)
                } else {
                    logger.warning(
                        "lost shard lease",
                        metadata: [
                            "shard": .stringConvertible(lease.shardID),
                            "held_token": .stringConvertible(lease.fenceToken),
                            "now_token": .stringConvertible(r.fenceToken),
                        ]
                    )
                }
            }
        }
        return renewed
    }

    public func release(launchID: UUID, shardID: Int) async throws {
        _ = try await store.client.query(
            """
            DELETE FROM shard_leases
             WHERE launch_id = \(launchID) AND shard_id = \(shardID) AND owner_node = \(nodeID)
            """,
            logger: logger
        )
    }

    public func releaseAll(launchID: UUID) async throws {
        _ = try await store.client.query(
            "DELETE FROM shard_leases WHERE launch_id = \(launchID) AND owner_node = \(nodeID)",
            logger: logger
        )
    }

    /// Shards whose lease has lapsed. Measuring the gap between a node's death and this
    /// returning its shards is the reclaim latency that lease-based ownership costs.
    public func expiredShards(launchID: UUID) async throws -> [Int] {
        let rows = try await store.client.query(
            """
            SELECT shard_id FROM shard_leases
             WHERE launch_id = \(launchID) AND expires_at < now() ORDER BY shard_id
            """,
            logger: logger
        )
        var out: [Int] = []
        for try await s in rows.decode(Int.self) { out.append(s) }
        return out
    }

    public func currentOwners(launchID: UUID) async throws -> [Int: String] {
        let rows = try await store.client.query(
            """
            SELECT shard_id, owner_node FROM shard_leases
             WHERE launch_id = \(launchID) AND expires_at >= now()
            """,
            logger: logger
        )
        var out: [Int: String] = [:]
        for try await (shard, owner) in rows.decode((Int, String).self) { out[shard] = owner }
        return out
    }
}

/// Keeps a node's lease set current in the background and exposes the shards it may
/// allocate from.
public actor LeaseHolder {
    private let manager: LeaseManager
    private let launchID: UUID
    private let shardCount: Int
    private let nodeIndex: Int
    private let nodeCount: Int
    private let logger: Logger
    public private(set) var leases: [ShardLease] = []
    public private(set) var reclaimEvents: Int = 0

    public init(
        manager: LeaseManager, launchID: UUID, shardCount: Int,
        nodeIndex: Int, nodeCount: Int, logger: Logger
    ) {
        self.manager = manager
        self.launchID = launchID
        self.shardCount = shardCount
        self.nodeIndex = nodeIndex
        self.nodeCount = nodeCount
        self.logger = logger
    }

    public var ownedShards: [Int] { leases.map(\.shardID).sorted() }

    public func refresh() async throws {
        if leases.isEmpty {
            leases = try await manager.acquireShare(
                launchID: launchID, shardCount: shardCount,
                nodeIndex: nodeIndex, nodeCount: nodeCount
            )
        } else {
            let before = leases.count
            leases = try await manager.renew(launchID: launchID, leases: leases)
            if leases.count < before { reclaimEvents += 1 }
        }
        // Pick up shards a dead node left behind.
        for shard in try await manager.expiredShards(launchID: launchID) {
            guard !leases.contains(where: { $0.shardID == shard }) else { continue }
            if let lease = try await manager.acquire(launchID: launchID, shardID: shard) {
                logger.info("reclaimed shard", metadata: ["shard": .stringConvertible(shard)])
                leases.append(lease)
                reclaimEvents += 1
            }
        }
    }

    /// Renews on a cadence well inside the TTL, so a single slow renewal does not cost
    /// the node its shards.
    public func run() async {
        let period = UInt64(manager.ttl.seconds * 1_000_000_000 / 3)
        while !Task.isCancelled {
            do { try await refresh() } catch {
                logger.error("lease refresh failed", metadata: ["error": .string("\(error)")])
            }
            try? await Task.sleep(nanoseconds: period)
        }
    }
}
