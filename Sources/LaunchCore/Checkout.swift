import Foundation
import Logging

public struct CheckoutRequest: Sendable, Codable {
    public var launchID: UUID
    public var customerID: String
    public var units: Int
    /// Client-generated. Two requests carrying the same key are the same request.
    public var idempotencyKey: String

    public init(launchID: UUID, customerID: String, units: Int = 1, idempotencyKey: String) {
        self.launchID = launchID
        self.customerID = customerID
        self.units = units
        self.idempotencyKey = idempotencyKey
    }

    /// Identifies the request's content, so a key reused for a *different* request is a
    /// client bug and can be reported as one rather than silently answered with the
    /// wrong order.
    var fingerprint: String {
        "\(launchID.uuidString):\(customerID):\(units)"
    }
}

public struct CheckoutResponse: Sendable, Codable, Equatable {
    public enum Outcome: String, Sendable, Codable {
        case purchased
        case soldOut
        /// Stock exists but not on any shard this customer probed.
        case fragmented
    }

    public var outcome: Outcome
    public var orderID: UUID?
    public var reservationID: UUID?
    public var units: Int
    public var probes: Int
    /// True when this response was replayed from the idempotency record rather than
    /// produced by doing the work again.
    public var replayed: Bool

    public init(
        outcome: Outcome, orderID: UUID? = nil, reservationID: UUID? = nil,
        units: Int = 0, probes: Int = 0, replayed: Bool = false
    ) {
        self.outcome = outcome
        self.orderID = orderID
        self.reservationID = reservationID
        self.units = units
        self.probes = probes
        self.replayed = replayed
    }

    func replaying() -> CheckoutResponse {
        var copy = self
        copy.replayed = true
        return copy
    }
}

enum IdempotencyClaim: Sendable {
    /// This caller owns the key and must do the work.
    case claimed
    /// The work is already done; here is the answer the first caller got.
    case completed(CheckoutResponse)
    /// Another request with this key is running right now.
    case inFlight
    /// The key was used before for a different request.
    case conflict
}

/// Drives one checkout from admission to order, and makes a retry safe.
///
/// The failure being prevented is specific: a request succeeds, its response is lost,
/// the customer retries, and a second order appears for one unit of stock. The defence
/// is a key the client generates, a conditional insert that decides who owns it, and the
/// order and the key's completion sharing a single transaction.
public struct CheckoutCoordinator: Sendable {
    let store: Store
    let allocator: Allocator
    let room: WaitingRoom
    let health: HealthMonitor
    /// Optional, so a coordinator can be built without Redis; the admission check then
    /// always reads Postgres, which is slower and equally correct.
    let coordination: Coordination?
    let nodeID: String
    let logger: Logger
    /// How long to wait for an in-flight duplicate before telling the client to retry.
    /// Blocking briefly absorbs the common case — a client retrying after a few hundred
    /// milliseconds — without holding a connection open for the pathological one.
    public var inFlightWait: Duration
    /// Default for whether admission is enforced, when a caller does not say.
    public var requireAdmission: Bool

    public init(
        store: Store,
        allocator: Allocator,
        room: WaitingRoom,
        health: HealthMonitor,
        coordination: Coordination? = nil,
        nodeID: String,
        logger: Logger,
        inFlightWait: Duration = .milliseconds(250),
        requireAdmission: Bool = true
    ) {
        self.store = store
        self.allocator = allocator
        self.room = room
        self.health = health
        self.coordination = coordination
        self.nodeID = nodeID
        self.logger = logger
        self.inFlightWait = inFlightWait
        self.requireAdmission = requireAdmission
    }

    /// Claims the key, or reports what the existing holder is doing.
    ///
    /// `ON CONFLICT DO NOTHING` plus `RETURNING` is the conditional write: exactly one
    /// concurrent caller gets a row back and therefore owns the work. Everyone else
    /// falls through to reading the existing record.
    func claim(_ request: CheckoutRequest, attempt: Int = 0) async throws -> IdempotencyClaim {
        let inserted = try await store.client.query(
            """
            INSERT INTO idempotency_keys (key, launch_id, customer_id, fingerprint, state, owner_node)
            VALUES (\(request.idempotencyKey), \(request.launchID), \(request.customerID),
                    \(request.fingerprint), 'in_flight', \(nodeID))
            ON CONFLICT (key) DO NOTHING
            RETURNING key
            """,
            logger: logger
        )
        for try await _ in inserted.decode(String.self) { return .claimed }

        let existing = try await store.client.query(
            """
            SELECT state, fingerprint, response::text FROM idempotency_keys
             WHERE key = \(request.idempotencyKey)
            """,
            logger: logger
        )
        for try await (state, fingerprint, response) in existing.decode((String, String, String?).self) {
            guard fingerprint == request.fingerprint else { return .conflict }
            switch state {
            case "completed", "failed":
                if let response, let data = response.data(using: .utf8),
                    let decoded = try? JSONDecoder().decode(CheckoutResponse.self, from: data)
                {
                    return .completed(decoded)
                }
                // A terminal key with no readable response is a bug, not a retry
                // opportunity: replaying the work could produce a second order.
                throw LaunchError.idempotencyConflict(request.idempotencyKey)
            default:
                return .inFlight
            }
        }
        // The row vanished between the two statements: the owner failed and released
        // the key. Retrying the claim is correct, but bounded — a key that keeps
        // appearing and vanishing means something is wrong, and spinning on it forever
        // would turn that into a hung request rather than an error anyone can see.
        guard attempt < 3 else {
            throw LaunchError.idempotencyInFlight(request.idempotencyKey)
        }
        return try await claim(request, attempt: attempt + 1)
    }

    /// `enforceAdmission` is decided per launch by the caller, not per node: a launch
    /// created without a waiting room has no admission to check, which is the week-one
    /// baseline and also how the sharding and idempotency experiments run. Defaulting it
    /// to the node-wide setting would make those runs depend on how the node was started.
    public func checkout(
        _ request: CheckoutRequest,
        enforceAdmission: Bool? = nil
    ) async throws -> CheckoutResponse {
        // The in-flight count is the controller's leading indicator, so it has to be
        // decremented before this call returns. An earlier version recorded the
        // completion from a `defer` in a detached task, which meant that under load the
        // decrements queued behind the work that was causing the load — in-flight read
        // high for longer than it was true, and the controller backed off on a signal
        // that was partly its own measurement lag.
        let started = ContinuousClock.now
        await health.checkoutStarted()
        do {
            let response = try await perform(request, enforceAdmission: enforceAdmission)
            await recordCompletion(started: started, failed: false)
            return response
        } catch {
            await recordCompletion(started: started, failed: Self.isServerFailure(error))
            throw error
        }
    }

    /// Whether an error means the service failed, as opposed to the service working.
    ///
    /// This matters because the error rate feeds the controller. Counting a customer
    /// being turned away at the door, or a duplicate being refused, as a server failure
    /// would make the controller back off hardest exactly when its own admission
    /// control was doing its job — a feedback loop driven by its own success.
    static func isServerFailure(_ error: Error) -> Bool {
        guard let error = error as? LaunchError else { return true }
        switch error {
        case .notAdmitted, .soldOut, .idempotencyInFlight, .idempotencyConflict:
            return false
        case .launchNotFound, .reservationNotFound, .reservationNotHeld, .reservationExpired,
            .invariantViolated:
            return true
        }
    }

    private func recordCompletion(started: ContinuousClock.Instant, failed: Bool) async {
        let elapsed = started.duration(to: .now).milliseconds
        await health.checkoutFinished(latencyMS: elapsed, failed: failed)
        await MetricsRegistry.shared.observe(
            "launchos_checkout_duration_ms", elapsed, help: "Server-side checkout latency"
        )
    }

    private func perform(
        _ request: CheckoutRequest,
        enforceAdmission: Bool?
    ) async throws -> CheckoutResponse {
        if enforceAdmission ?? requireAdmission {
            // Redis first. A positive answer came from the admitter itself, so it is
            // trustworthy; anything else means "ask Postgres" rather than "turn them
            // away", because a cold or unreachable cache must not lock out a customer
            // who was genuinely admitted.
            let cached = await coordination?.isAdmittedCached(
                launchID: request.launchID, customerID: request.customerID
            )
            if cached != true {
                guard
                    try await room.isAdmitted(
                        launchID: request.launchID, customerID: request.customerID
                    )
                else {
                    throw LaunchError.notAdmitted(request.customerID)
                }
            }
        }

        switch try await claim(request) {
        case .completed(let response):
            await MetricsRegistry.shared.increment(
                "launchos_idempotent_replays_total",
                help: "Retries answered from the idempotency record instead of re-executing"
            )
            return response.replaying()

        case .conflict:
            throw LaunchError.idempotencyConflict(request.idempotencyKey)

        case .inFlight:
            // Wait briefly for the owner to finish, then answer from its record. This is
            // the subtle case: returning "still processing" immediately is also correct,
            // but a short block answers most real retries with the real result.
            let deadline = ContinuousClock.now.advanced(by: inFlightWait)
            while ContinuousClock.now < deadline {
                try? await Task.sleep(for: .milliseconds(25))
                if case .completed(let response) = try await claim(request) {
                    await MetricsRegistry.shared.increment("launchos_idempotent_replays_total")
                    return response.replaying()
                }
            }
            await MetricsRegistry.shared.increment(
                "launchos_idempotency_inflight_total",
                help: "Retries that arrived while the original request was still running"
            )
            throw LaunchError.idempotencyInFlight(request.idempotencyKey)

        case .claimed:
            break
        }

        // This caller owns the key. From here on, every exit path has to leave the key
        // in a terminal state with a response attached, or a later retry would block
        // forever on an in-flight record whose owner is gone.
        do {
            let outcome = try await allocator.reserve(
                launchID: request.launchID, customerID: request.customerID, units: request.units
            )
            guard let reservation = outcome.reservation else {
                // Sold out and fragmented are both failures to allocate, but only one of
                // them means the launch is over, so they are recorded separately. The
                // allocator already read the remaining stock to make that distinction, so
                // it comes back on the outcome rather than costing a second query that
                // could also disagree with the first.
                let remaining = outcome.remainingWhenFailed ?? 0
                let response = CheckoutResponse(
                    outcome: remaining > 0 ? .fragmented : .soldOut, probes: outcome.probes
                )
                // A sell-out is a terminal answer, so it is stored against the key: a
                // retry gets the same "sold out" rather than a fresh attempt that might
                // succeed off the back of someone else's cancellation.
                try await finalize(request, state: "completed", response: response)
                return response
            }

            // The crash window is between here and the commit below. If this process
            // dies now the unit is reserved with no order behind it; nothing in this
            // path can fix that, which is why reservation expiry is durable and the
            // sweeper is the safety net rather than an optimization.
            //
            // The order id is generated here rather than inside the transaction so the
            // response body can be built first and written in that same commit. That is
            // what makes the guarantee airtight: there is no instant at which the order
            // exists but the record a retry would read from does not.
            let orderID = UUID()
            let response = CheckoutResponse(
                outcome: .purchased, orderID: orderID, reservationID: reservation.id,
                units: request.units, probes: outcome.probes
            )
            let json = String(data: try JSONEncoder().encode(response), encoding: .utf8)
            let order = try await store.commitReservation(
                reservationID: reservation.id,
                orderID: orderID,
                idempotencyKey: request.idempotencyKey,
                responseJSON: json
            )
            assert(order.id == orderID)
            await MetricsRegistry.shared.increment(
                "launchos_purchases_total", help: "Completed purchases"
            )
            return response
        } catch {
            // Release the key so the customer can retry, rather than leaving a
            // permanent in-flight record that would lock them out of their own order.
            try? await releaseClaim(request)
            throw error
        }
    }

    private func finalize(_ request: CheckoutRequest, state: String, response: CheckoutResponse) async throws {
        let json = String(data: try JSONEncoder().encode(response), encoding: .utf8)
        _ = try await store.client.query(
            """
            UPDATE idempotency_keys
               SET state = \(state), response = \(json)::jsonb, updated_at = now()
             WHERE key = \(request.idempotencyKey)
            """,
            logger: logger
        )
    }

    private func releaseClaim(_ request: CheckoutRequest) async throws {
        _ = try await store.client.query(
            """
            DELETE FROM idempotency_keys
             WHERE key = \(request.idempotencyKey) AND state = 'in_flight' AND owner_node = \(nodeID)
            """,
            logger: logger
        )
    }

    /// Deletes idempotency records past their retention window.
    ///
    /// Nothing else ever removes a completed key, so without this the table grows for
    /// the lifetime of the deployment — and it is the largest table in the system,
    /// because every record carries the response body it exists to replay. Measured
    /// mid-run at a million customers: 216 MB for 386,000 keys, about 560 bytes each,
    /// against 101 MB for the orders they describe.
    ///
    /// The window is what decides how long a retry can still be answered from the
    /// record rather than re-executed. Past it, a repeat of the same request is treated
    /// as new — which is why the window has to outlive any client's retry behaviour by
    /// a wide margin, and why a day is the usual choice rather than an hour.
    ///
    /// Deleted in bounded batches for the same reason the expiry sweeper is: a single
    /// statement covering a week of keys holds locks proportional to the backlog.
    @discardableResult
    public func expireIdempotencyRecords(
        olderThan retention: Duration,
        batchSize: Int = 5_000
    ) async throws -> Int {
        let cutoff = Date().addingTimeInterval(-retention.seconds)
        let rows = try await store.client.query(
            """
            DELETE FROM idempotency_keys
             WHERE key IN (
                 SELECT key FROM idempotency_keys
                  WHERE state <> 'in_flight' AND created_at < \(cutoff)
                  LIMIT \(batchSize)
             )
            RETURNING key
            """,
            logger: logger
        )
        var deleted = 0
        for try await _ in rows.decode(String.self) { deleted += 1 }
        if deleted > 0 {
            await MetricsRegistry.shared.increment(
                "launchos_idempotency_records_expired_total", by: Double(deleted),
                help: "Idempotency records deleted past their retention window"
            )
        }
        return deleted
    }

    /// Clears in-flight keys left behind by a process that died mid-checkout.
    ///
    /// Without this, a customer whose request was being handled by the dead worker can
    /// never retry: their key stays in-flight forever and every retry is rejected.
    public func recoverAbandonedClaims(olderThan age: Duration) async throws -> Int {
        let cutoff = Date().addingTimeInterval(-age.seconds)
        let rows = try await store.client.query(
            """
            DELETE FROM idempotency_keys
             WHERE state = 'in_flight' AND updated_at < \(cutoff)
            RETURNING key
            """,
            logger: logger
        )
        var n = 0
        for try await _ in rows.decode(String.self) { n += 1 }
        if n > 0 {
            logger.warning("released abandoned idempotency claims", metadata: ["count": .stringConvertible(n)])
            await MetricsRegistry.shared.increment(
                "launchos_abandoned_claims_released_total", by: Double(n),
                help: "In-flight idempotency keys released after their owner died"
            )
        }
        return n
    }
}
