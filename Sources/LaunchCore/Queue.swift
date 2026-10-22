import Foundation
import Logging
import PostgresNIO

/// How the next batch of customers is chosen from the waiting pool.
///
/// The three are here to be compared, not because one is obviously right: strict FIFO
/// buys the fairest ordering with a globally ordered scan, lottery gives up ordering
/// entirely for speed, and bucketed FIFO is the compromise.
public enum FairnessPolicy: String, Sendable, CaseIterable, Codable {
    /// Exact arrival order. Requires an ordered scan over the waiting set.
    case strictFIFO
    /// Uniform random choice from everyone waiting. No ordering cost, and no bound on
    /// how long an early arrival can be passed over.
    case lottery
    /// Arrival order between time buckets, random within a bucket. Keeps ordering
    /// roughly intact while letting the database pick any row inside the current bucket.
    case bucketedFIFO

    /// The ORDER BY that implements the policy. This is the entire difference between
    /// the three, and the reason their throughput differs.
    var orderBy: String {
        switch self {
        case .strictFIFO: return "seq"
        case .lottery: return "random()"
        case .bucketedFIFO: return "bucket, random()"
        }
    }
}

public struct QueueJoinResult: Sendable {
    public var entry: QueueEntry
    /// Number of waiting customers ahead of this one.
    public var position: Int
    /// True when this join found an existing row instead of creating one, i.e. a retry.
    public var wasAlreadyQueued: Bool
    public var retries: Int
}

public struct AdmissionRecord: Sendable, Codable {
    public var customerID: String
    public var arrivalSeq: Int64
    public var admissionSeq: Int64
    public var arrivalAt: Date
    public var admittedAt: Date
    public var waitSeconds: Double
}

/// The waiting room.
///
/// Arrival order lives in Postgres, not Redis. A sequence number assigned on insert is
/// what makes the order durable: the service can restart, Redis can be flushed, and the
/// order is still recoverable from the table. Redis caches positions so the common
/// "where am I?" poll does not hit the database, and is never consulted for a decision.
public struct WaitingRoom: Sendable {
    let store: Store
    let logger: Logger
    /// Width of a bucketed-FIFO bucket, in seconds.
    public let bucketWidth: Double

    public init(store: Store, logger: Logger, bucketWidth: Double = 1.0) {
        self.store = store
        self.logger = logger
        self.bucketWidth = bucketWidth
    }

    public func bucket(for date: Date) -> Int64 {
        Int64(date.timeIntervalSince1970 / bucketWidth)
    }

    /// Joins the queue, or returns the position already held.
    ///
    /// `ON CONFLICT ... DO UPDATE` over the (launch_id, customer_id) primary key is what
    /// makes a retry harmless: the second request increments a retry counter and reads
    /// back the row the first one created, leaving `seq` — and therefore the customer's
    /// place in line — untouched. A customer who refreshes twenty times holds the
    /// position they got on arrival.
    /// - Parameter arrivalSequence: the launch's own arrival sequence, which hands out
    ///   the dense index a constant-time position is computed from. Nil falls back to
    ///   zero, and positions for that launch are counted instead.
    public func join(
        launchID: UUID,
        customerID: String,
        arrivalSequence: String? = nil,
        at now: Date = Date()
    ) async throws -> QueueJoinResult {
        let started = ContinuousClock.now
        let b = bucket(for: now)
        // `nextval` is only drawn on an actual insert. On the conflict path the existing
        // row keeps the index it was given, which is the same guarantee as `seq`: a
        // customer who refreshes keeps the arrival they had. The sequence does advance
        // on a conflicting insert, leaving a gap, which is harmless — gaps make a
        // position read high, never low.
        var binds = PostgresBindings()
        binds.append(launchID, context: .default)
        binds.append(customerID, context: .default)
        binds.append(b, context: .default)
        binds.append(now, context: .default)
        if let arrivalSequence { binds.append(arrivalSequence, context: .default) }
        let indexExpression = arrivalSequence == nil ? "0::bigint" : "nextval($5::regclass)"
        let sql = """
            WITH joined AS (
                INSERT INTO queue_entries
                    (launch_id, customer_id, bucket, arrival_at, state, arrival_index)
                VALUES ($1, $2, $3, $4, 'waiting', \(indexExpression))
                ON CONFLICT (launch_id, customer_id)
                DO UPDATE SET retries = queue_entries.retries + 1
                RETURNING seq, arrival_index, bucket, arrival_at, state, admitted_seq,
                          admitted_at, retries
            )
            SELECT joined.seq, joined.arrival_index, joined.bucket, joined.arrival_at,
                   joined.state, joined.admitted_seq, joined.admitted_at, joined.retries
              FROM joined
            """
        let rows = try await store.client.query(
            PostgresQuery(unsafeSQL: sql, binds: binds), logger: logger
        )
        var result: QueueJoinResult?
        for try await (seq, arrivalIndex, bucket, arrivalAt, state, admittedSeq, admittedAt, retries)
            in rows.decode((Int64, Int64, Int64, Date, String, Int64?, Date?, Int).self)
        {
            let entry = QueueEntry(
                launchID: launchID, customerID: customerID, seq: seq,
                arrivalIndex: arrivalIndex, bucket: bucket,
                arrivalAt: arrivalAt, state: QueueEntryState(rawValue: state) ?? .waiting,
                admittedSeq: admittedSeq, admittedAt: admittedAt
            )
            // No position here on purpose. Folding the count into this statement saves a
            // round-trip, which is why it was written that way, but the count itself is
            // O(the number of people already waiting) — so at a million joins it is a
            // million scans of up to a million index entries, and the round-trip was
            // never the expensive part. The caller fills the position in from
            // `PositionEstimator` in constant time.
            result = QueueJoinResult(
                entry: entry, position: 0, wasAlreadyQueued: retries > 0, retries: retries
            )
        }
        guard let result else { throw LaunchError.launchNotFound(launchID.uuidString) }
        await MetricsRegistry.shared.observe(
            "launchos_queue_join_duration_ms", started.duration(to: .now).milliseconds,
            help: "Server-side queue join latency, including database wait"
        )
        return result
    }

    /// Waiting customers ahead of `seq`. Counted from the durable table so the answer
    /// stays right after a Redis flush.
    public func position(launchID: UUID, seq: Int64) async throws -> Int {
        let rows = try await store.client.query(
            """
            SELECT count(*)::bigint FROM queue_entries
             WHERE launch_id = \(launchID) AND state = 'waiting' AND seq < \(seq)
            """,
            logger: logger
        )
        for try await n in rows.decode(Int64.self) { return Int(n) }
        return 0
    }

    /// The seq the launch's queue started at. Immutable once anyone has joined, so it is
    /// read once per launch per node and then cached.
    func firstSeq(launchID: UUID) async throws -> Int64? {
        let rows = try await store.client.query(
            "SELECT min(seq)::bigint FROM queue_entries WHERE launch_id = \(launchID)",
            logger: logger
        )
        for try await value in rows.decode(Int64?.self) { return value }
        return nil
    }

    /// Customers who have left the waiting room, counted in Postgres.
    func departedCount(launchID: UUID) async throws -> Int {
        let rows = try await store.client.query(
            """
            SELECT count(*)::bigint FROM queue_entries
             WHERE launch_id = \(launchID) AND state <> 'waiting'
            """,
            logger: logger
        )
        for try await n in rows.decode(Int64.self) { return Int(n) }
        return 0
    }

    /// Reads a customer's queue entry without computing their position.
    ///
    /// Position is a separate call because it is the expensive half — a count over the
    /// waiting set — and the common case is answered from the Redis cache instead. An
    /// earlier version computed it here unconditionally and then let the caller override
    /// it with the cached value, which meant the cache saved nothing at all and the most
    /// frequent request in a launch paid for a scan every time.
    public func entry(launchID: UUID, customerID: String) async throws -> QueueEntry? {
        let rows = try await store.client.query(
            """
            SELECT seq, arrival_index, bucket, arrival_at, state, admitted_seq, admitted_at
              FROM queue_entries WHERE launch_id = \(launchID) AND customer_id = \(customerID)
            """,
            logger: logger
        )
        for try await (seq, arrivalIndex, bucket, arrivalAt, state, admittedSeq, admittedAt)
            in rows.decode((Int64, Int64, Int64, Date, String, Int64?, Date?).self)
        {
            return QueueEntry(
                launchID: launchID, customerID: customerID, seq: seq,
                arrivalIndex: arrivalIndex, bucket: bucket,
                arrivalAt: arrivalAt, state: QueueEntryState(rawValue: state) ?? .waiting,
                admittedSeq: admittedSeq, admittedAt: admittedAt
            )
        }
        return nil
    }

    public func status(launchID: UUID, customerID: String) async throws -> (QueueEntry, Int)? {
        guard let entry = try await entry(launchID: launchID, customerID: customerID) else {
            return nil
        }
        let pos = entry.state == .waiting
            ? try await position(launchID: launchID, seq: entry.seq)
            : 0
        return (entry, pos)
    }

    public func depth(launchID: UUID) async throws -> Int {
        let rows = try await store.client.query(
            """
            SELECT count(*)::bigint FROM queue_entries
             WHERE launch_id = \(launchID) AND state = 'waiting'
            """,
            logger: logger
        )
        for try await n in rows.decode(Int64.self) { return Int(n) }
        return 0
    }

    /// Admits up to `count` customers under the given policy, in one statement.
    ///
    /// The candidate CTE applies the policy's ORDER BY with a LIMIT and takes
    /// `FOR UPDATE SKIP LOCKED`, so several admitter processes can run concurrently
    /// without handing the same customer to both. `nextval` is called in the `numbered`
    /// CTE, which reads from the already-ordered candidate set, so admission sequence
    /// numbers are handed out in the policy's order.
    /// - Parameter commitHorizon: how long an entry must have existed before it may be
    ///   admitted. See the note below on why ordering by a sequence number is not enough
    ///   on its own.
    public func admit(
        launchID: UUID,
        count: Int,
        policy: FairnessPolicy,
        commitHorizon: Duration = .milliseconds(250)
    ) async throws -> [AdmissionRecord] {
        guard count > 0 else { return [] }
        // Why the horizon exists.
        //
        // `seq` comes from a Postgres sequence, so it is handed out in arrival order —
        // but it is handed out at INSERT, not at COMMIT, and two concurrent joins can
        // commit in the opposite order to the numbers they took. An admitter running in
        // that window sees seq 6 and not seq 5, admits 6, and then admits 5 on the next
        // tick with a higher admission number. Arrival order and admission order are now
        // inverted, through no fault of the policy: strict FIFO measured 0.999 instead of
        // 1.000 and the retry audit reported positions "improving" for customers who had
        // done nothing at all.
        //
        // Ordering on a sequence is therefore not sufficient for FIFO. Admitting only
        // entries that have existed for longer than the horizon fixes it, because any
        // join that took a lower sequence number started earlier and has had at least
        // the horizon to commit. The cost is a fixed, bounded addition to every
        // customer's wait, which at 250 ms is far below the queue waits it applies to.
        let horizonSeconds = commitHorizon.seconds
        let sql = """
            WITH cand AS (
                SELECT customer_id FROM queue_entries
                 WHERE launch_id = $1 AND state = 'waiting'
                   AND arrival_at <= now() - ($3 || ' seconds')::interval
                 ORDER BY \(policy.orderBy)
                 LIMIT $2
                 FOR UPDATE SKIP LOCKED
            ),
            numbered AS (
                SELECT customer_id, nextval('admission_seq') AS aseq FROM cand
            )
            UPDATE queue_entries q
               SET state = 'admitted', admitted_seq = n.aseq, admitted_at = now()
              FROM numbered n
             WHERE q.launch_id = $1 AND q.customer_id = n.customer_id
            RETURNING q.customer_id, q.seq, q.admitted_seq, q.arrival_at, q.admitted_at
            """
        var binds = PostgresBindings()
        binds.append(launchID, context: .default)
        binds.append(Int(count), context: .default)
        binds.append(String(format: "%.3f", horizonSeconds), context: .default)
        let rows = try await store.client.query(
            PostgresQuery(unsafeSQL: sql, binds: binds), logger: logger
        )
        var out: [AdmissionRecord] = []
        for try await (customerID, seq, aseq, arrivalAt, admittedAt) in rows.decode(
            (String, Int64, Int64, Date, Date).self
        ) {
            out.append(
                AdmissionRecord(
                    customerID: customerID, arrivalSeq: seq, admissionSeq: aseq,
                    arrivalAt: arrivalAt, admittedAt: admittedAt,
                    waitSeconds: admittedAt.timeIntervalSince(arrivalAt)
                )
            )
        }
        return out
    }

    public func isAdmitted(launchID: UUID, customerID: String) async throws -> Bool {
        let rows = try await store.client.query(
            """
            SELECT state = 'admitted' FROM queue_entries
             WHERE launch_id = \(launchID) AND customer_id = \(customerID)
            """,
            logger: logger
        )
        for try await admitted in rows.decode(Bool.self) { return admitted }
        return false
    }

    /// Every admission for a launch, for computing fairness after a run.
    public func admissions(launchID: UUID) async throws -> [AdmissionRecord] {
        let rows = try await store.client.query(
            """
            SELECT customer_id, seq, admitted_seq, arrival_at, admitted_at
              FROM queue_entries
             WHERE launch_id = \(launchID) AND state = 'admitted' AND admitted_seq IS NOT NULL
             ORDER BY admitted_seq
            """,
            logger: logger
        )
        var out: [AdmissionRecord] = []
        for try await (customerID, seq, aseq, arrivalAt, admittedAt) in rows.decode(
            (String, Int64, Int64, Date, Date).self
        ) {
            out.append(
                AdmissionRecord(
                    customerID: customerID, arrivalSeq: seq, admissionSeq: aseq,
                    arrivalAt: arrivalAt, admittedAt: admittedAt,
                    waitSeconds: admittedAt.timeIntervalSince(arrivalAt)
                )
            )
        }
        return out
    }

    /// Retry counts, so a run can assert that refreshing never improved a position.
    public func retryAudit(launchID: UUID) async throws -> [(customerID: String, seq: Int64, retries: Int, admittedSeq: Int64?)] {
        let rows = try await store.client.query(
            """
            SELECT customer_id, seq, retries, admitted_seq FROM queue_entries
             WHERE launch_id = \(launchID) ORDER BY seq
            """,
            logger: logger
        )
        var out: [(String, Int64, Int, Int64?)] = []
        for try await row in rows.decode((String, Int64, Int, Int64?).self) { out.append(row) }
        return out
    }
}

extension WaitingRoom {
    /// Counts queue entries whose arrival sequence does not match the order their
    /// position was first established in.
    ///
    /// The join statement updates only `retries` on conflict, so `seq`, `bucket` and
    /// `arrival_at` are written once and never again — a customer who refreshes twenty
    /// times keeps the position they got on arrival. This is the assertion of that: any
    /// row where a retry coincided with a changed bucket or arrival time would mean the
    /// conflict branch had touched something it must not.
    public func retriesThatChangedPosition(launchID: UUID) async throws -> Int {
        let rows = try await store.client.query(
            """
            SELECT count(*)::bigint FROM queue_entries
             WHERE launch_id = \(launchID)
               AND retries > 0
               AND bucket <> floor(extract(epoch FROM arrival_at) / \(bucketWidth))::bigint
            """,
            logger: logger
        )
        for try await n in rows.decode(Int64.self) { return Int(n) }
        return 0
    }
}

/// Fairness and wait-time summary for one run.
public struct FairnessReport: Sendable, Codable {
    public var policy: FairnessPolicy
    public var admitted: Int
    /// Spearman rank correlation between arrival order and admission order.
    /// 1.0 is perfect FIFO, ~0.0 is a lottery.
    public var rankCorrelation: Double
    public var waitP50: Double
    public var waitP95: Double
    public var waitP99: Double
    public var waitMax: Double
    public var waitMean: Double
    /// Mean queue depth over the run, for the Little's Law check.
    public var meanQueueDepth: Double
    public var admissionRate: Double
    /// Little's Law: W = L / λ. Printed next to the measured mean wait so the two can
    /// be compared rather than asserted.
    public var predictedMeanWait: Double

    public init(
        policy: FairnessPolicy, admissions: [AdmissionRecord],
        meanQueueDepth: Double, admissionRate: Double
    ) {
        self.policy = policy
        self.admitted = admissions.count
        let arrival = admissions.map { Double($0.arrivalSeq) }
        let admission = admissions.map { Double($0.admissionSeq) }
        self.rankCorrelation = spearman(arrival, admission)
        let waits = Samples(admissions.map(\.waitSeconds))
        self.waitP50 = waits.p50
        self.waitP95 = waits.p95
        self.waitP99 = waits.p99
        self.waitMax = waits.max
        self.waitMean = waits.mean
        self.meanQueueDepth = meanQueueDepth
        self.admissionRate = admissionRate
        self.predictedMeanWait = admissionRate > 0 ? meanQueueDepth / admissionRate : 0
    }
}
