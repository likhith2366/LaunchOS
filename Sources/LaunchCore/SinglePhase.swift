import Foundation
import Logging
import PostgresNIO

/// What a single-phase sale did.
public struct SinglePhaseOutcome: Sendable {
    public enum Result: Sendable {
        /// The unit was taken, the order written, and the key answered.
        case purchased(shardID: Int)
        /// No shard in the probe set had stock.
        case noStock
    }

    public var result: Result
    public var probes: Int
}

extension Store {
    /// Takes a unit, records the order, and answers the idempotency key — in one
    /// statement, for a sale with no payment step.
    ///
    /// ## Why this exists alongside the two-phase path
    ///
    /// The two-phase flow — reserve, hold, commit when payment authorises, let expiry
    /// reclaim the unit if it never does — is the reason reservations and the sweeper
    /// exist, and it is not going anywhere. It costs three commits per sale: claim the
    /// key, take the unit, record the order. Once row contention is gone, those three
    /// WAL flushes are the dominant per-sale cost.
    ///
    /// When nothing happens between reserving and committing — an immediate sale, a free
    /// drop, a claim against an entitlement already held — the middle commit separates
    /// nothing. This merges the last two, so a sale costs two commits instead of three.
    ///
    /// The caller still claims the key in its own statement first. That ordering is not
    /// negotiable: something has to serialise on the key before a unit can move, or two
    /// concurrent duplicates both take stock on their way to discovering they are
    /// duplicates.
    ///
    /// ## The guarantee this preserves
    ///
    /// The order and the key's completed state — including the response body a retry
    /// reads back — are written by this one statement, so they commit together. There is
    /// no instant at which the order exists and the record a retry would find does not.
    /// That is the same guarantee the two-phase commit makes, for the same reason.
    ///
    /// ## Why the key is updated here rather than inserted here
    ///
    /// An earlier version of this did the whole thing in one statement: insert the key,
    /// probe, write the order, then update the key to completed. The update matched
    /// nothing, because sub-statements of a single statement cannot see each other's
    /// effects — the row the insert had just created was invisible to the update in the
    /// same statement. Every sale succeeded and left its key `in_flight` forever, so
    /// every retry was told the original was still running.
    ///
    /// That is the same mistake that lost every available unit during shard collapse,
    /// arrived at from a different direction, which is why the rule is worth stating as
    /// a rule: **within one statement, never write a row that another part of the same
    /// statement also writes.** Here the key is inserted by the caller's earlier
    /// statement, so updating it in this one is an ordinary cross-statement update.
    ///
    /// Inventory moves `available → sold` directly rather than through `reserved`, which
    /// keeps `available + reserved + sold = total` true at every instant an observer
    /// could look — the only thing the invariant asks.
    func takeAndRecord(
        launchID: UUID,
        shardIDs: [Int],
        customerID: String,
        units: Int,
        ownerNode: String,
        ttl: Duration,
        idempotencyKey: String,
        responseJSON: String?,
        orderID: UUID,
        reservationID: UUID,
        skipLocked: Bool = true
    ) async throws -> SinglePhaseOutcome {
        guard !shardIDs.isEmpty else {
            return SinglePhaseOutcome(result: .noStock, probes: 0)
        }
        let now = Date()
        let expiresAt = now.addingTimeInterval(ttl.seconds)
        let bucket = expiryBucket(for: expiresAt)

        let rows = try await client.query(
            """
            WITH candidate AS (
                SELECT shard_id FROM inventory_shards
                 WHERE launch_id = \(launchID)
                   AND shard_id = ANY(\(shardIDs))
                   AND available >= \(units)
                 ORDER BY array_position(\(shardIDs), shard_id)
                 LIMIT 1
                   FOR UPDATE \(unescaped: skipLocked ? "SKIP LOCKED" : "")
            ),
            taken AS (
                UPDATE inventory_shards i
                   SET available = i.available - \(units), sold = i.sold + \(units)
                  FROM candidate c
                 WHERE i.launch_id = \(launchID) AND i.shard_id = c.shard_id
                   AND i.available >= \(units)
             RETURNING i.shard_id
            ),
            reservation AS (
                INSERT INTO reservations
                    (id, launch_id, shard_id, customer_id, units, state, owner_node,
                     probes, expires_at, expiry_bucket, resolved_at)
                SELECT \(reservationID), \(launchID), taken.shard_id, \(customerID),
                       \(units), 'committed', \(ownerNode), \(shardIDs.count),
                       \(expiresAt), \(bucket), \(now)
                  FROM taken
            RETURNING id, shard_id
            ),
            recorded AS (
                INSERT INTO orders
                    (id, launch_id, customer_id, reservation_id, units, created_at)
                SELECT \(orderID), \(launchID), \(customerID), \(reservationID), \(units),
                       \(now)
                  FROM reservation
            RETURNING id
            ),
            answered AS (
                UPDATE idempotency_keys
                   SET state = 'completed', response = \(responseJSON)::jsonb,
                       updated_at = now()
                 WHERE key = \(idempotencyKey) AND EXISTS (SELECT 1 FROM recorded)
            RETURNING key
            )
            SELECT (SELECT count(*) FROM recorded)::bigint,
                   (SELECT count(*) FROM answered)::bigint,
                   (SELECT shard_id FROM reservation)
            """,
            logger: logger
        )

        for try await (recorded, answered, shardID) in rows.decode((Int64, Int64, Int?).self) {
            guard recorded == 1, let shardID else {
                return SinglePhaseOutcome(result: .noStock, probes: shardIDs.count)
            }
            // The order exists, so the key must have been answered in the same commit.
            // If it was not, a retry would be told the original is still running while
            // its order sits there, which is the failure this whole mechanism exists to
            // prevent — so it is checked rather than assumed.
            guard answered == 1 else {
                throw LaunchError.invariantViolated(
                    "single-phase sale wrote an order for \(idempotencyKey) without "
                        + "answering the key; a retry would never find its order"
                )
            }
            return SinglePhaseOutcome(result: .purchased(shardID: shardID), probes: shardIDs.count)
        }
        return SinglePhaseOutcome(result: .noStock, probes: shardIDs.count)
    }
}
