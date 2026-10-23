import Foundation
import Logging
import PostgresNIO

public struct StoreConfig: Sendable {
    public var host: String
    public var port: Int
    public var username: String
    public var password: String
    public var database: String
    public var maxConnections: Int

    public init(
        host: String = "127.0.0.1",
        port: Int = 5432,
        username: String = "launchos",
        password: String = "launchos",
        database: String = "launchos",
        maxConnections: Int = 24
    ) {
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.maxConnections = maxConnections
    }

    public static func fromEnvironment() -> StoreConfig {
        func env(_ k: String, _ d: String) -> String { ProcessInfo.processInfo.environment[k] ?? d }
        return StoreConfig(
            host: env("POSTGRES_HOST", "127.0.0.1"),
            port: Int(env("POSTGRES_PORT", "5432")) ?? 5432,
            username: env("POSTGRES_USER", "launchos"),
            password: env("POSTGRES_PASSWORD", "launchos"),
            database: env("POSTGRES_DB", "launchos"),
            maxConnections: Int(env("POSTGRES_POOL", "24")) ?? 24
        )
    }

    var clientConfiguration: PostgresClient.Configuration {
        var c = PostgresClient.Configuration(
            host: host, port: port, username: username, password: password, database: database,
            tls: .disable
        )
        c.options.maximumConnections = maxConnections
        return c
    }
}

/// How the expiry sweeper finds work. Both are implemented because the difference
/// between them is one of the measurements: an index scan over a large held-reservation
/// table degrades as the table grows, a bucketed read does not.
public enum SweepStrategy: String, Sendable, CaseIterable {
    case indexScan
    case bucketed
}

public struct SweepResult: Sendable {
    public var reclaimed: Int
    public var unitsReturned: Int
    public var bucketsVisited: Int
    public var durationMS: Double
}

/// Everything durable. Postgres is the arbiter of correctness; every invariant in the
/// project is enforced by a constraint or a conditional update in this file.
public final class Store: Sendable {
    public let client: PostgresClient
    public let logger: Logger

    public init(config: StoreConfig, logger: Logger) {
        self.client = PostgresClient(configuration: config.clientConfiguration)
        self.logger = logger
    }

    /// The client needs its own task for the lifetime of the process.
    public func run() async {
        await client.run()
    }

    /// The name of a launch's arrival sequence.
    ///
    /// Derived from the launch id and never supplied by a caller, because it is
    /// interpolated into DDL where no parameter binding exists. Hyphens are stripped so
    /// the result is a bare identifier needing no quoting, and a UUID's 32 hex digits
    /// plus the prefix sit well inside Postgres's 63-character identifier limit.
    static func arrivalSequenceName(for launchID: UUID) -> String {
        "queue_arrival_" + launchID.uuidString.replacingOccurrences(of: "-", with: "").lowercased()
    }

    // MARK: - Schema

    public func migrate() async throws {
        for statement in Self.schemaStatements {
            do {
                _ = try await client.query(PostgresQuery(unsafeSQL: statement), logger: logger)
            } catch {
                // PSQLError's default description deliberately hides its detail to avoid
                // leaking data into logs. During migration that detail is the only thing
                // that matters, and a schema statement carries no user data, so it is
                // reflected into the message along with the statement that failed.
                let first = statement.split(whereSeparator: \.isNewline)
                    .first.map(String.init) ?? statement
                throw LaunchError.invariantViolated(
                    "migration failed on `\(first.trimmingCharacters(in: .whitespaces))`: "
                        + String(reflecting: error)
                )
            }
        }
    }

    public func reset() async throws {
        for t in ["idempotency_keys", "orders", "reservations", "queue_entries", "shard_leases", "inventory_shards", "launches"] {
            _ = try? await client.query(PostgresQuery(unsafeSQL: "TRUNCATE TABLE \(t) CASCADE"), logger: logger)
        }
        for s in ["queue_seq", "admission_seq", "fence_seq"] {
            _ = try? await client.query(PostgresQuery(unsafeSQL: "ALTER SEQUENCE \(s) RESTART WITH 1"), logger: logger)
        }
        // Per-launch arrival sequences are objects, not rows, so TRUNCATE CASCADE leaves
        // them behind. Left alone they accumulate for the lifetime of the database.
        let leftover = try? await client.query(
            """
            SELECT sequencename FROM pg_sequences
             WHERE schemaname = current_schema() AND sequencename LIKE 'queue_arrival_%'
            """,
            logger: logger
        )
        if let leftover {
            for try await name in leftover.decode(String.self) {
                _ = try? await client.query(
                    PostgresQuery(unsafeSQL: "DROP SEQUENCE IF EXISTS \(name)"), logger: logger
                )
            }
        }
    }

    // MARK: - Launches

    /// Creates the launch and splits its units across `shardCount` rows. The remainder
    /// is spread one unit at a time over the low shards rather than dumped on shard 0,
    /// because a lopsided split makes the sharding measurement look better than it is.
    /// Creates the launch and returns it as stored, including the name of the arrival
    /// sequence this call created. Callers need that name to hand out arrival indexes,
    /// and deriving it twice in two places is how they drift apart.
    @discardableResult
    public func createLaunch(_ launch: Launch) async throws -> Launch {
        let ttlMS = Int64(launch.reservationTTL.milliseconds)
        // One arrival sequence per launch, so a customer's position in that launch is a
        // dense index and can be answered by arithmetic. The name is derived from the
        // launch id rather than supplied, because it goes into DDL.
        let sequenceName = Self.arrivalSequenceName(for: launch.id)
        _ = try await client.query(
            PostgresQuery(unsafeSQL: "CREATE SEQUENCE IF NOT EXISTS \(sequenceName)"),
            logger: logger
        )
        _ = try await client.query(
            """
            INSERT INTO launches
                (id, sku, total_units, shard_count, collapse_threshold, collapsed,
                 requires_admission, queue_sequence, reservation_ttl_ms)
            VALUES (\(launch.id), \(launch.sku), \(launch.totalUnits), \(launch.shardCount),
                    \(launch.collapseThreshold), \(launch.collapsed),
                    \(launch.requiresAdmission), \(sequenceName), \(ttlMS))
            """,
            logger: logger
        )
        var stored = launch
        stored.queueSequence = sequenceName

        let base = launch.totalUnits / launch.shardCount
        let remainder = launch.totalUnits % launch.shardCount
        for shard in 0..<launch.shardCount {
            let units = base + (shard < remainder ? 1 : 0)
            _ = try await client.query(
                """
                INSERT INTO inventory_shards (launch_id, shard_id, available, reserved, sold)
                VALUES (\(launch.id), \(shard), \(units), 0, 0)
                """,
                logger: logger
            )
        }
        return stored
    }

    public func launch(sku: String) async throws -> Launch {
        let rows = try await client.query(
            """
            SELECT id, sku, total_units, shard_count, collapse_threshold, collapsed,
                   requires_admission, queue_sequence, reservation_ttl_ms
            FROM launches WHERE sku = \(sku)
            """,
            logger: logger
        )
        for try await (id, sku, total, shards, threshold, collapsed, requiresAdmission, sequence, ttl)
            in rows.decode((UUID, String, Int, Int, Int, Bool, Bool, String?, Int64).self)
        {
            return Launch(
                id: id, sku: sku, totalUnits: total, shardCount: shards,
                collapseThreshold: threshold, collapsed: collapsed,
                requiresAdmission: requiresAdmission, queueSequence: sequence,
                reservationTTL: .milliseconds(ttl)
            )
        }
        throw LaunchError.launchNotFound(sku)
    }

    public func launch(id: UUID) async throws -> Launch {
        let rows = try await client.query(
            """
            SELECT id, sku, total_units, shard_count, collapse_threshold, collapsed,
                   requires_admission, queue_sequence, reservation_ttl_ms
            FROM launches WHERE id = \(id)
            """,
            logger: logger
        )
        for try await (id, sku, total, shards, threshold, collapsed, requiresAdmission, sequence, ttl)
            in rows.decode((UUID, String, Int, Int, Int, Bool, Bool, String?, Int64).self)
        {
            return Launch(
                id: id, sku: sku, totalUnits: total, shardCount: shards,
                collapseThreshold: threshold, collapsed: collapsed,
                requiresAdmission: requiresAdmission, queueSequence: sequence,
                reservationTTL: .milliseconds(ttl)
            )
        }
        throw LaunchError.launchNotFound(id.uuidString)
    }

    /// Every launch, for the self-check and the operator view.
    public func allLaunches() async throws -> [Launch] {
        let rows = try await client.query(
            """
            SELECT id, sku, total_units, shard_count, collapse_threshold, collapsed,
                   requires_admission, queue_sequence, reservation_ttl_ms
            FROM launches ORDER BY created_at
            """,
            logger: logger
        )
        var out: [Launch] = []
        for try await (id, sku, total, shards, threshold, collapsed, requiresAdmission, sequence, ttl)
            in rows.decode((UUID, String, Int, Int, Int, Bool, Bool, String?, Int64).self)
        {
            out.append(
                Launch(
                    id: id, sku: sku, totalUnits: total, shardCount: shards,
                    collapseThreshold: threshold, collapsed: collapsed,
                    requiresAdmission: requiresAdmission, queueSequence: sequence,
                    reservationTTL: .milliseconds(ttl)
                )
            )
        }
        return out
    }

    public func inventoryState(launchID: UUID) async throws -> InventoryState {
        let launch = try await launch(id: launchID)
        var shards: [InventoryShard] = []
        let rows = try await client.query(
            """
            SELECT shard_id, available, reserved, sold FROM inventory_shards
            WHERE launch_id = \(launchID) ORDER BY shard_id
            """,
            logger: logger
        )
        for try await (shard, available, reserved, sold) in rows.decode((Int, Int, Int, Int).self) {
            shards.append(InventoryShard(shardID: shard, available: available, reserved: reserved, sold: sold))
        }
        return InventoryState(
            launchID: launchID, totalUnits: launch.totalUnits, shards: shards, collapsed: launch.collapsed
        )
    }

    // MARK: - Allocation

    /// One probe: a single conditional update on one shard, with the reservation insert
    /// fused into the same statement so there is no window in which a unit is taken from
    /// a shard but no reservation exists to account for it.
    ///
    /// If the UPDATE matches no row the CTE is empty, the INSERT inserts nothing, and the
    /// statement returns zero rows. Nothing is written, so a failed probe costs one
    /// round-trip and no rollback.
    /// Tries a whole probe set in one statement, taking the first shard in probe order
    /// that has stock.
    ///
    /// This replaced a version that issued one statement per shard. Probing four shards
    /// sequentially costs four round-trips whether it succeeds on the first or fails on
    /// all four, and once the commit path had been collapsed to a single statement, those
    /// probes were the largest remaining per-request cost — the whole budget on the
    /// sold-out path, which is most requests in a launch.
    ///
    /// `ORDER BY array_position(...)` preserves the caller's probe order exactly, so the
    /// deterministic per-customer sequence is unchanged; it is now evaluated inside the
    /// database instead of by round-tripping. `FOR UPDATE SKIP LOCKED` takes the first
    /// candidate that is not already locked by another request, which is what keeps
    /// contention spread instead of queueing several requests behind one hot shard.
    ///
    /// The `available >= units` predicate is repeated on the UPDATE. It is redundant —
    /// the row is locked and the value was read under that lock — but it costs nothing
    /// and it means the no-oversell guarantee does not depend on reasoning about
    /// lock scope.
    ///
    /// One behavioural difference worth stating: the shards are now judged against a
    /// single snapshot rather than four successive ones. For the fragmentation
    /// measurement that is more honest, not less — "none of this customer's shards had
    /// stock" is now a statement about one instant.
    ///
    /// - Parameter skipLocked: whether a shard currently locked by another transaction is
    ///   skipped or waited for. See `Allocator.reserve` for why both are needed.
    func probe(
        launchID: UUID,
        shardIDs: [Int],
        customerID: String,
        units: Int,
        ownerNode: String,
        ttl: Duration,
        skipLocked: Bool = true
    ) async throws -> Reservation? {
        guard !shardIDs.isEmpty else { return nil }
        let reservationID = UUID()
        let expiresAt = Date().addingTimeInterval(ttl.seconds)
        let bucket = expiryBucket(for: expiresAt)
        let probeCount = shardIDs.count
        let rows = try await client.query(
            """
            WITH candidate AS (
                SELECT shard_id FROM inventory_shards
                 WHERE launch_id = \(launchID)
                   AND shard_id = ANY(\(shardIDs))
                   AND available >= \(units)
                 ORDER BY array_position(\(shardIDs), shard_id)
                 LIMIT 1
                   -- A literal chosen by the caller's Bool, never anything from a
                   -- request; `unescaped` is the only way to vary a lock clause and it
                   -- must never carry input.
                   FOR UPDATE \(unescaped: skipLocked ? "SKIP LOCKED" : "")
            ),
            taken AS (
                UPDATE inventory_shards i
                   SET available = i.available - \(units), reserved = i.reserved + \(units)
                  FROM candidate c
                 WHERE i.launch_id = \(launchID) AND i.shard_id = c.shard_id
                   AND i.available >= \(units)
             RETURNING i.shard_id
            )
            INSERT INTO reservations
                (id, launch_id, shard_id, customer_id, units, state, owner_node, probes,
                 expires_at, expiry_bucket)
            SELECT \(reservationID), \(launchID), taken.shard_id, \(customerID), \(units),
                   'held', \(ownerNode), \(probeCount), \(expiresAt), \(bucket)
              FROM taken
            RETURNING shard_id
            """,
            logger: logger
        )
        for try await shardID in rows.decode(Int.self) {
            return Reservation(
                id: reservationID, launchID: launchID, shardID: shardID, customerID: customerID,
                units: units, state: .held, expiresAt: expiresAt, ownerNode: ownerNode,
                probes: probeCount
            )
        }
        return nil
    }

    /// Sum of available units across all shards. Used to decide whether to collapse.
    public func availableUnits(launchID: UUID) async throws -> Int {
        try await availability(launchID: launchID).available
    }

    /// Remaining stock and whether the launch has already been collapsed, in one
    /// statement.
    ///
    /// The allocator needs both at the same moment and on the same path — every failed
    /// allocation has to decide whether this is a sell-out, a collapse trigger, or
    /// fragmentation — and reading them separately both cost a round-trip and allowed
    /// the two answers to come from different instants.
    func availability(launchID: UUID) async throws -> (available: Int, collapsed: Bool) {
        let rows = try await client.query(
            """
            SELECT COALESCE(SUM(i.available), 0)::bigint, l.collapsed
              FROM launches l LEFT JOIN inventory_shards i ON i.launch_id = l.id
             WHERE l.id = \(launchID)
             GROUP BY l.collapsed
            """,
            logger: logger
        )
        for try await (total, collapsed) in rows.decode((Int64, Bool).self) {
            return (Int(total), collapsed)
        }
        throw LaunchError.launchNotFound(launchID.uuidString)
    }

    /// Merges every shard's remaining availability into shard 0 and marks the launch
    /// collapsed, under a transaction that locks the shard rows in a fixed order.
    ///
    /// Contention stops being the problem once only a handful of units are left — at that
    /// point nearly every request is going to fail anyway — so trading parallelism for a
    /// guarantee that a request never fails while stock exists is the right way round.
    ///
    /// Returns false if another node collapsed it first.
    public func collapseShards(launchID: UUID) async throws -> Bool {
        try await client.withConnection { conn in
            _ = try await conn.query("BEGIN", logger: self.logger)
            do {
                // Re-check under lock so two nodes racing to collapse do it once.
                let check = try await conn.query(
                    "SELECT collapsed FROM launches WHERE id = \(launchID) FOR UPDATE", logger: self.logger
                )
                var alreadyCollapsed = true
                for try await c in check.decode(Bool.self) { alreadyCollapsed = c }
                if alreadyCollapsed {
                    _ = try await conn.query("COMMIT", logger: self.logger)
                    return false
                }
                // Three explicit steps rather than one statement with a data-modifying
                // CTE. An earlier version did the drain and the credit in a single
                // statement; the drain ran and the credit silently did not, which lost
                // every unit that was still available at collapse time. Sub-statements
                // of one statement cannot see each other's effects, so arithmetic that
                // has to read what another part just wrote does not belong in one.
                //
                // Locking every shard in ascending id order first means concurrent
                // allocations block here rather than racing the sum, and fixes the lock
                // order so this can never deadlock against `releaseReservation`.
                let locked = try await conn.query(
                    """
                    SELECT shard_id, available FROM inventory_shards
                     WHERE launch_id = \(launchID) ORDER BY shard_id FOR UPDATE
                    """,
                    logger: self.logger
                )
                var toMove = 0
                for try await (shardID, available) in locked.decode((Int, Int).self) where shardID != 0 {
                    toMove += available
                }
                _ = try await conn.query(
                    """
                    UPDATE inventory_shards SET available = 0
                     WHERE launch_id = \(launchID) AND shard_id <> 0
                    """,
                    logger: self.logger
                )
                _ = try await conn.query(
                    """
                    UPDATE inventory_shards SET available = available + \(toMove)
                     WHERE launch_id = \(launchID) AND shard_id = 0
                    """,
                    logger: self.logger
                )
                _ = try await conn.query(
                    "UPDATE launches SET collapsed = TRUE WHERE id = \(launchID)", logger: self.logger
                )
                _ = try await conn.query("COMMIT", logger: self.logger)
                return true
            } catch {
                _ = try? await conn.query("ROLLBACK", logger: self.logger)
                throw error
            }
        }
    }

    // MARK: - Commit, cancel, expire

    /// Moves a held reservation to committed and records the order, with the idempotency
    /// record written in the same transaction. That shared commit is the guarantee: an
    /// order never exists without the key that would let a retry find it.
    public func commitReservation(
        reservationID: UUID,
        orderID: UUID = UUID(),
        idempotencyKey: String? = nil,
        responseJSON: String? = nil
    ) async throws -> Order {
        let createdAt = Date()
        // One statement, so one round-trip and one implicit transaction.
        //
        // This was six round-trips (BEGIN, three writes, the key update, COMMIT) and the
        // measurements said that was the ceiling: once sharding removed the row
        // contention, throughput went flat while latency rose in proportion to
        // concurrency, which is a saturated service, and the service was spending its
        // time on round-trips per checkout rather than on contention.
        //
        // Collapsing writes into one statement is the thing that caused the worst bug in
        // this project, so the distinction matters. There, the arithmetic needed to read
        // rows the same statement was modifying, and sub-statements cannot see each
        // other's effects. Here, every write reads `claimed`'s RETURNING output — which
        // is exactly what data-modifying CTEs are for — and the four writes touch four
        // different tables, so no two of them can silently overwrite one another.
        //
        // If `claimed` matches nothing the CTE is empty, every dependent write inserts or
        // updates nothing, and the statement returns a zero count. Nothing partial can
        // commit.
        let rows = try await client.query(
            """
            WITH claimed AS (
                UPDATE reservations SET state = 'committed', resolved_at = now()
                 WHERE id = \(reservationID) AND state = 'held' AND expires_at > now()
                RETURNING launch_id, shard_id, customer_id, units
            ),
            moved AS (
                UPDATE inventory_shards i
                   SET reserved = i.reserved - c.units, sold = i.sold + c.units
                  FROM claimed c
                 WHERE i.launch_id = c.launch_id AND i.shard_id = c.shard_id
             RETURNING i.shard_id
            ),
            recorded AS (
                INSERT INTO orders (id, launch_id, customer_id, reservation_id, units, created_at)
                SELECT \(orderID), c.launch_id, c.customer_id, \(reservationID), c.units,
                       \(createdAt)
                  FROM claimed c
             RETURNING id
            ),
            answered AS (
                UPDATE idempotency_keys
                   SET state = 'completed', response = \(responseJSON)::jsonb,
                       updated_at = now()
                 WHERE key = \(idempotencyKey) AND EXISTS (SELECT 1 FROM claimed)
             RETURNING key
            )
            SELECT c.launch_id, c.customer_id, c.units,
                   (SELECT count(*) FROM moved)::bigint,
                   (SELECT count(*) FROM recorded)::bigint
              FROM claimed c
            """,
            logger: logger
        )

        for try await (launchID, customerID, units, shardsUpdated, ordersWritten) in rows.decode(
            (UUID, String, Int, Int64, Int64).self
        ) {
            // These cannot legitimately be zero once the reservation was claimed: the
            // shard row is guaranteed to exist by the foreign key, and the order insert
            // is unconditional on `claimed`. Checking anyway, because this is precisely
            // the failure the earlier CTE bug had — a write that silently did not happen
            // — and it is cheap to assert rather than assume.
            guard shardsUpdated == 1, ordersWritten == 1 else {
                throw LaunchError.invariantViolated(
                    "commit of \(reservationID) updated \(shardsUpdated) shard rows and wrote "
                        + "\(ordersWritten) orders; expected exactly one of each"
                )
            }
            return Order(
                id: orderID, launchID: launchID, customerID: customerID,
                reservationID: reservationID, units: units, createdAt: createdAt
            )
        }

        // Nothing was claimed. Finding out why costs an extra query, which is fine
        // because this is not the happy path.
        try await explainCommitFailure(reservationID: reservationID)
        throw LaunchError.reservationNotFound(reservationID)
    }

    /// Turns "the conditional update matched nothing" into the specific reason, so the
    /// caller can distinguish an expired reservation from an unknown one.
    private func explainCommitFailure(reservationID: UUID) async throws {
        let rows = try await client.query(
            "SELECT state, expires_at <= now() FROM reservations WHERE id = \(reservationID)",
            logger: logger
        )
        for try await (state, isExpired) in rows.decode((String, Bool).self) {
            let s = ReservationState(rawValue: state) ?? .expired
            if s == .held, isExpired { throw LaunchError.reservationExpired(reservationID) }
            throw LaunchError.reservationNotHeld(reservationID, s)
        }
    }

    /// Returns a held reservation's units to the pool. Shared by explicit cancellation
    /// and by the sweeper.
    ///
    /// The destination shard is not always the source shard: after a collapse, every
    /// unit must come back to shard 0, or returning stock would silently un-collapse the
    /// launch and reintroduce the fragmentation the collapse removed.
    public func releaseReservation(reservationID: UUID, as finalState: ReservationState) async throws -> Bool {
        // One statement, so one round-trip. The sweeper does this once per reservation,
        // so at a hundred thousand abandoned reservations the round-trip count here is
        // the difference between a sweep that keeps up and one that does not.
        //
        // The two inventory updates are split by whether the unit is going back to the
        // shard it came from, and this is not a stylistic choice. Two data-modifying
        // CTEs that update the *same row* do not both apply — the second has no effect —
        // so writing this as a naive "decrement reserved here, increment available
        // there" pair would silently lose one of the two halves in the ordinary case
        // where source and destination are the same shard. That is the identical failure
        // mode as the shard-collapse bug. So:
        //
        //   `settled` handles the source row, and folds the available increment into the
        //           same UPDATE when the destination is that row;
        //   `moved`   handles the destination row only when it is a different row.
        //
        // Every case therefore touches each row exactly once.
        let rows = try await client.query(
            """
            WITH released AS (
                UPDATE reservations SET state = \(finalState.rawValue), resolved_at = now()
                 WHERE id = \(reservationID) AND state = 'held'
             RETURNING launch_id, shard_id, units
            ),
            routed AS (
                SELECT r.launch_id, r.shard_id AS src, r.units,
                       CASE WHEN l.collapsed THEN 0 ELSE r.shard_id END AS dst
                  FROM released r JOIN launches l ON l.id = r.launch_id
            ),
            settled AS (
                UPDATE inventory_shards i
                   SET reserved = i.reserved - t.units,
                       available = i.available
                           + CASE WHEN t.dst = t.src THEN t.units ELSE 0 END
                  FROM routed t
                 WHERE i.launch_id = t.launch_id AND i.shard_id = t.src
             RETURNING i.shard_id
            ),
            moved AS (
                UPDATE inventory_shards i
                   SET available = i.available + t.units
                  FROM routed t
                 WHERE i.launch_id = t.launch_id AND i.shard_id = t.dst AND t.dst <> t.src
             RETURNING i.shard_id
            )
            SELECT (SELECT count(*) FROM released)::bigint,
                   (SELECT count(*) FROM settled)::bigint
            """,
            logger: logger
        )

        for try await (releasedCount, settledCount) in rows.decode((Int64, Int64).self) {
            guard releasedCount > 0 else { return false }
            // The reservation was released, so its shard row must have been adjusted.
            guard settledCount == 1 else {
                throw LaunchError.invariantViolated(
                    "release of \(reservationID) adjusted \(settledCount) shard rows; expected one"
                )
            }
            return true
        }
        return false
    }

    // MARK: - Expiry sweep

    public func expiryBucket(for date: Date) -> Int64 {
        Int64(date.timeIntervalSince1970 / 60.0)
    }

    /// Reclaims expired reservations in bounded batches.
    ///
    /// `indexScan` reads the partial index on expires_at; `bucketed` reads whole minute
    /// partitions, which keeps the sweeper's work proportional to what actually expired
    /// rather than to how many reservations are outstanding.
    public func sweepExpired(
        strategy: SweepStrategy,
        batchSize: Int,
        launchID: UUID? = nil
    ) async throws -> SweepResult {
        let start = ContinuousClock.now
        var ids: [UUID] = []
        var buckets = 0

        switch strategy {
        case .indexScan:
            let rows: PostgresRowSequence
            if let launchID {
                rows = try await client.query(
                    """
                    SELECT id FROM reservations
                     WHERE state = 'held' AND expires_at <= now() AND launch_id = \(launchID)
                     ORDER BY expires_at LIMIT \(batchSize)
                    """,
                    logger: logger
                )
            } else {
                rows = try await client.query(
                    """
                    SELECT id FROM reservations
                     WHERE state = 'held' AND expires_at <= now()
                     ORDER BY expires_at LIMIT \(batchSize)
                    """,
                    logger: logger
                )
            }
            for try await id in rows.decode(UUID.self) { ids.append(id) }
            buckets = 1

        case .bucketed:
            // Both queries below are scoped by launch when one is given. An earlier
            // version took `launchID` and then ignored it here, so a scoped sweep
            // happily reclaimed other launches' reservations — harmless for the
            // background sweeper, which wants everything, but it quietly made every
            // measurement and assertion about "how many did this launch reclaim"
            // meaningless.
            let nowBucket = expiryBucket(for: Date())
            let scope = launchID
            let dueRows: PostgresRowSequence
            if let scope {
                dueRows = try await client.query(
                    """
                    SELECT DISTINCT expiry_bucket FROM reservations
                     WHERE state = 'held' AND expiry_bucket <= \(nowBucket)
                       AND launch_id = \(scope)
                     ORDER BY expiry_bucket LIMIT 4
                    """,
                    logger: logger
                )
            } else {
                dueRows = try await client.query(
                    """
                    SELECT DISTINCT expiry_bucket FROM reservations
                     WHERE state = 'held' AND expiry_bucket <= \(nowBucket)
                     ORDER BY expiry_bucket LIMIT 4
                    """,
                    logger: logger
                )
            }
            var dueBuckets: [Int64] = []
            for try await b in dueRows.decode(Int64.self) { dueBuckets.append(b) }
            for bucket in dueBuckets {
                guard ids.count < batchSize else { break }
                buckets += 1
                let remaining = batchSize - ids.count
                let rows: PostgresRowSequence
                if let scope {
                    rows = try await client.query(
                        """
                        SELECT id FROM reservations
                         WHERE state = 'held' AND expiry_bucket = \(bucket)
                           AND expires_at <= now() AND launch_id = \(scope)
                         LIMIT \(remaining)
                        """,
                        logger: logger
                    )
                } else {
                    rows = try await client.query(
                        """
                        SELECT id FROM reservations
                         WHERE state = 'held' AND expiry_bucket = \(bucket)
                           AND expires_at <= now()
                         LIMIT \(remaining)
                        """,
                        logger: logger
                    )
                }
                for try await id in rows.decode(UUID.self) { ids.append(id) }
            }
        }

        var reclaimed = 0
        var units = 0
        for id in ids {
            // One transaction per reservation. Reclaim must be idempotent and
            // independently retryable: a batch-wide transaction would mean one
            // contended row stalls the reclaim of every other unit in the batch.
            if try await releaseReservation(reservationID: id, as: .expired) {
                reclaimed += 1
                units += 1
            }
        }
        let elapsed = start.duration(to: .now)
        return SweepResult(
            reclaimed: reclaimed, unitsReturned: units, bucketsVisited: buckets,
            durationMS: elapsed.milliseconds
        )
    }

    /// Reservations whose owning node has gone away and whose TTL has not yet fired.
    /// This is the crash-window query: the unit is reserved, no order exists, and
    /// nothing is going to complete it.
    public func orphanedReservations(excludingNodes liveNodes: [String], launchID: UUID) async throws -> [UUID] {
        let live = liveNodes.isEmpty ? [""] : liveNodes
        let rows = try await client.query(
            """
            SELECT id FROM reservations
             WHERE state = 'held' AND launch_id = \(launchID)
               AND owner_node <> ALL(\(live))
            """,
            logger: logger
        )
        var out: [UUID] = []
        for try await id in rows.decode(UUID.self) { out.append(id) }
        return out
    }

    // MARK: - Invariant

    /// The correctness assertion the whole project rests on, run against committed state.
    ///
    /// - Parameter deep: also reconcile sold units against the orders table. That check
    ///   is O(the number of orders), so it is off by default — see the note on
    ///   `SelfCheck` for why continuous verification has to cost less than the thing it
    ///   is verifying.
    public func assertInvariant(
        launchID: UUID,
        deep: Bool = true
    ) async throws -> InventoryState {
        let state = try await inventoryState(launchID: launchID)
        guard state.holds else {
            throw LaunchError.invariantViolated(
                "available(\(state.available)) + reserved(\(state.reserved)) + sold(\(state.sold)) "
                    + "= \(state.accountedFor) != total(\(state.totalUnits))"
            )
        }
        guard state.sold <= state.totalUnits else {
            throw LaunchError.invariantViolated(
                "oversold: sold(\(state.sold)) > total(\(state.totalUnits))"
            )
        }

        // Everything above reads `inventory_shards`, which is a handful of rows however
        // large the launch gets. What follows reads every order, so it does not belong on
        // a timer that fires every few seconds.
        guard deep else { return state }

        // Second, independent check: sold units must equal ordered units. The first
        // check can only catch arithmetic drift inside inventory_shards; this one
        // catches a unit that was marked sold without an order behind it.
        let rows = try await client.query(
            "SELECT COALESCE(SUM(units), 0)::bigint FROM orders WHERE launch_id = \(launchID)",
            logger: logger
        )
        var ordered = 0
        for try await n in rows.decode(Int64.self) { ordered = Int(n) }
        guard ordered == state.sold else {
            throw LaunchError.invariantViolated("sold(\(state.sold)) != ordered units(\(ordered))")
        }
        return state
    }

    public func orderCount(launchID: UUID) async throws -> Int {
        let rows = try await client.query(
            "SELECT count(*)::bigint FROM orders WHERE launch_id = \(launchID)", logger: logger
        )
        for try await n in rows.decode(Int64.self) { return Int(n) }
        return 0
    }

    /// Duplicate orders per idempotency key. Must be zero.
    public func duplicateOrderCount(launchID: UUID) async throws -> Int {
        let rows = try await client.query(
            """
            SELECT COALESCE(SUM(c - 1), 0)::bigint FROM (
                SELECT count(*) AS c FROM orders o
                  JOIN idempotency_keys k ON k.customer_id = o.customer_id AND k.launch_id = o.launch_id
                 WHERE o.launch_id = \(launchID)
                 GROUP BY k.key
                HAVING count(*) > 1
            ) dup
            """,
            logger: logger
        )
        for try await n in rows.decode(Int64.self) { return Int(n) }
        return 0
    }

    /// The schema, split into statements.
    ///
    /// This used to be `schemaSQL.split(separator: ";")`, which is wrong in a way that
    /// takes a while to show up: a semicolon inside a `--` comment cuts the statement it
    /// sits in. Adding the word "durability;" to a column comment truncated a
    /// `CREATE TABLE` mid-definition and every query against the database failed with a
    /// syntax error pointing at a character offset.
    ///
    /// So comments are stripped before splitting, and the stripper respects single-quoted
    /// literals so that a `--` inside one is left alone.
    static let schemaStatements: [String] = {
        splitStatements(schemaSQL)
    }()

    static func splitStatements(_ sql: String) -> [String] {
        var stripped = ""
        stripped.reserveCapacity(sql.count)
        var inString = false
        var inComment = false
        var iterator = sql.startIndex

        while iterator < sql.endIndex {
            let character = sql[iterator]
            let next = sql.index(after: iterator)

            if inComment {
                if character.isNewline {
                    inComment = false
                    stripped.append(character)
                }
                iterator = next
                continue
            }

            if inString {
                stripped.append(character)
                // Doubled quotes are an escaped quote inside the literal, not its end.
                if character == "'" {
                    if next < sql.endIndex, sql[next] == "'" {
                        stripped.append("'")
                        iterator = sql.index(after: next)
                        continue
                    }
                    inString = false
                }
                iterator = next
                continue
            }

            if character == "'" {
                inString = true
                stripped.append(character)
                iterator = next
                continue
            }

            if character == "-", next < sql.endIndex, sql[next] == "-" {
                inComment = true
                iterator = sql.index(after: next)
                continue
            }

            stripped.append(character)
            iterator = next
        }

        return stripped
            .split(separator: ";", omittingEmptySubsequences: true)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
    }
}

extension Duration {
    public var milliseconds: Double {
        let c = components
        return Double(c.seconds) * 1000 + Double(c.attoseconds) / 1e15
    }

    public var seconds: Double {
        let c = components
        return Double(c.seconds) + Double(c.attoseconds) / 1e18
    }
}
