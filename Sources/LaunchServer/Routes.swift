import Foundation
import LaunchCore
import Vapor

extension LaunchError: AbortError {
    public var status: HTTPResponseStatus {
        switch self {
        case .launchNotFound, .reservationNotFound: return .notFound
        case .soldOut: return .conflict
        case .reservationNotHeld, .reservationExpired: return .conflict
        case .notAdmitted: return .tooManyRequests
        // 409 rather than 429: the request is not being rate-limited, it is a duplicate
        // of one still running, and the client should retry the same key.
        case .idempotencyInFlight: return .conflict
        case .idempotencyConflict: return .unprocessableEntity
        case .invariantViolated: return .internalServerError
        }
    }

    public var reason: String {
        switch self {
        case .launchNotFound(let sku): return "no launch for \(sku)"
        case .soldOut: return "sold out"
        case .reservationNotFound(let id): return "no reservation \(id)"
        case .reservationNotHeld(let id, let state): return "reservation \(id) is \(state.rawValue)"
        case .reservationExpired(let id): return "reservation \(id) expired"
        case .notAdmitted(let c): return "\(c) has not been admitted"
        case .idempotencyInFlight(let k): return "request \(k) is still being processed"
        case .idempotencyConflict(let k): return "key \(k) was used for a different request"
        case .invariantViolated(let detail): return "invariant violated: \(detail)"
        }
    }
}

func routes(_ app: Application) throws {
    let node = app.node

    app.get("health") { _ async -> String in "ok" }

    app.get("metrics") { _ async -> Response in
        let body = await MetricsRegistry.shared.render()
        return Response(
            status: .ok,
            headers: ["content-type": "text/plain; version=0.0.4; charset=utf-8"],
            body: .init(string: body)
        )
    }

    app.get("node") { req async throws -> NodeStatusView in
        let n = req.node
        var owned: [Int] = []
        if let holder = await n.controllers.leaseHolder {
            owned = await holder.ownedShards
        }
        return NodeStatusView(
            nodeID: n.nodeID, nodeIndex: n.nodeIndex, nodeCount: n.nodeCount,
            multiNodeMode: n.multiNodeMode.rawValue, ownedShards: owned,
            liveNodes: await n.coordination.liveNodes(),
            redisEnabled: n.coordination.isEnabled,
            sweepStrategy: n.sweepStrategy.rawValue,
            maxProbes: n.allocator.config.maxProbes
        )
    }

    // MARK: - Launch administration

    app.post("launches") { req async throws -> LaunchView in
        let body = try req.content.decode(CreateLaunchBody.self)
        let n = req.node
        let launch = Launch(
            sku: body.sku,
            totalUnits: body.totalUnits,
            shardCount: max(1, body.shardCount ?? 1),
            collapseThreshold: body.collapseThreshold ?? 0,
            requiresAdmission: body.admission != nil,
            reservationTTL: .milliseconds(Int((body.reservationTTLSeconds ?? 90) * 1000))
        )
        try await n.store.createLaunch(launch)
        await n.directory.store(launch)

        if let admission = body.admission {
            let config = admission.config()
            let controller = AdmissionController(
                config: config, health: n.health, room: n.room,
                launchID: launch.id, sku: launch.sku, logger: n.logger
            )
            await n.controllers.replace(launch.id, with: controller)
            await n.controllers.start(launch.id, controller: controller)
        }

        // Lease-based mode claims this node's shards up front so the first request does
        // not pay for lease acquisition.
        if n.multiNodeMode == .leasedShards, launch.shardCount > 1 {
            await n.controllers.startLeases(
                holder: LeaseHolder(
                    manager: n.leases, launchID: launch.id, shardCount: launch.shardCount,
                    nodeIndex: n.nodeIndex, nodeCount: n.nodeCount, logger: n.logger
                )
            )
        }
        return LaunchView(launch)
    }

    /// Every launch, for the operator view's selector.
    app.get("launches") { req async throws -> [LaunchView] in
        try await req.node.store.allLaunches().map(LaunchView.init)
    }

    app.get("launches", ":sku") { req async throws -> LaunchView in
        let sku = try req.parameters.require("sku")
        return LaunchView(try await req.node.launch(sku: sku))
    }

    app.get("launches", ":sku", "inventory") { req async throws -> InventoryView in
        let sku = try req.parameters.require("sku")
        let launch = try await req.node.launch(sku: sku)
        return InventoryView(try await req.node.store.inventoryState(launchID: launch.id))
    }

    /// Asserts the invariant server-side and fails the request if it does not hold.
    /// A test that wants the assertion made by the system rather than by itself calls this.
    app.get("launches", ":sku", "invariant") { req async throws -> InventoryView in
        let sku = try req.parameters.require("sku")
        let launch = try await req.node.launch(sku: sku)
        return InventoryView(try await req.node.store.assertInvariant(launchID: launch.id))
    }

    // MARK: - Waiting room

    app.post("launches", ":sku", "queue") { req async throws -> QueueStatusView in
        let sku = try req.parameters.require("sku")
        let body = try req.content.decode(JoinQueueBody.self)
        let n = req.node
        let launch = try await n.launch(sku: sku)
        let result = try await n.room.join(launchID: launch.id, customerID: body.customerID)
        await n.coordination.cachePosition(
            launchID: launch.id, customerID: body.customerID, position: result.position
        )
        return QueueStatusView(
            customerID: body.customerID,
            arrivalSeq: result.entry.seq,
            position: result.position,
            state: result.entry.state.rawValue,
            admitted: result.entry.state == .admitted,
            retries: result.retries,
            waitSeconds: Date().timeIntervalSince(result.entry.arrivalAt),
            wasAlreadyQueued: result.wasAlreadyQueued
        )
    }

    /// The most frequent request during a launch, which is why it tries Redis first.
    app.get("launches", ":sku", "queue", ":customerID") { req async throws -> QueueStatusView in
        let sku = try req.parameters.require("sku")
        let customerID = try req.parameters.require("customerID")
        let n = req.node
        let launch = try await n.launch(sku: sku)
        guard let entry = try await n.room.entry(launchID: launch.id, customerID: customerID) else {
            throw Abort(.notFound, reason: "\(customerID) is not in the queue")
        }
        // Position is counted over the waiting set, so it is the expensive part of this
        // request and the reason the Redis cache exists. It is only computed when the
        // cache misses, and not at all once the customer has been admitted.
        var position = 0
        if entry.state == .waiting {
            if let cached = await n.coordination.cachedPosition(launchID: launch.id, customerID: customerID) {
                position = cached
            } else {
                position = try await n.room.position(launchID: launch.id, seq: entry.seq)
                await n.coordination.cachePosition(
                    launchID: launch.id, customerID: customerID, position: position
                )
            }
        }
        return QueueStatusView(
            customerID: customerID,
            arrivalSeq: entry.seq,
            position: position,
            state: entry.state.rawValue,
            admitted: entry.state == .admitted,
            retries: 0,
            waitSeconds: Date().timeIntervalSince(entry.arrivalAt),
            wasAlreadyQueued: true
        )
    }

    app.get("launches", ":sku", "admission") { req async throws -> AdmissionStatusView in
        let sku = try req.parameters.require("sku")
        let n = req.node
        let launch = try await n.launch(sku: sku)
        guard let controller = await n.controllers.existing(launch.id) else {
            throw Abort(.notFound, reason: "no admission controller for \(sku)")
        }
        return AdmissionStatusView(
            kind: controller.config.kind.rawValue,
            policy: controller.config.policy.rawValue,
            rate: await controller.rate,
            totalAdmitted: await controller.totalAdmitted,
            queueDepth: try await n.room.depth(launchID: launch.id),
            meanQueueDepth: await controller.meanQueueDepth(),
            health: await n.health.snapshot()
        )
    }

    /// The control trace: one sample per interval, with the rate, the action taken and
    /// the health that drove it. This is what the adaptive-vs-fixed comparison is drawn
    /// from, so it is exposed rather than only logged.
    app.get("launches", ":sku", "admission", "trace") { req async throws -> ControlTraceView in
        let sku = try req.parameters.require("sku")
        let n = req.node
        let launch = try await n.launch(sku: sku)
        guard let controller = await n.controllers.existing(launch.id) else {
            throw Abort(.notFound, reason: "no admission controller for \(sku)")
        }
        return ControlTraceView(samples: await controller.controlTrace())
    }

    /// Admits a batch immediately, bypassing the control loop. The simulator uses this
    /// to drive admission on its own clock so a measured run is not at the mercy of
    /// wall-clock timer drift.
    app.post("launches", ":sku", "admission", "tick") { req async throws -> ControlTraceView in
        let sku = try req.parameters.require("sku")
        let n = req.node
        let launch = try await n.launch(sku: sku)
        guard let controller = await n.controllers.existing(launch.id) else {
            throw Abort(.notFound, reason: "no admission controller for \(sku)")
        }
        let sample = try await controller.tick()
        return ControlTraceView(samples: [sample])
    }

    app.get("launches", ":sku", "fairness") { req async throws -> FairnessView in
        let sku = try req.parameters.require("sku")
        let n = req.node
        let launch = try await n.launch(sku: sku)
        let admissions = try await n.room.admissions(launchID: launch.id)
        let controller = await n.controllers.existing(launch.id)
        let policy = controller?.config.policy ?? .strictFIFO
        let cost = await controller?.admitStatementCost() ?? (meanMS: 0, p99MS: 0, batches: 0)
        return FairnessView(
            report: FairnessReport(
                policy: policy,
                admissions: admissions,
                meanQueueDepth: await controller?.meanQueueDepth() ?? 0,
                admissionRate: await controller?.effectiveAdmissionRate() ?? 0
            ),
            admitMeanMS: cost.meanMS,
            admitP99MS: cost.p99MS,
            admitBatches: cost.batches
        )
    }

    /// Checks the property directly: no customer's admission order may be better than
    /// that of someone who arrived earlier and is still waiting, no matter how often
    /// they retried.
    app.get("launches", ":sku", "retry-audit") { req async throws -> RetryAuditView in
        let sku = try req.parameters.require("sku")
        let n = req.node
        let launch = try await n.launch(sku: sku)
        let rows = try await n.room.retryAudit(launchID: launch.id)
        let retried = rows.count { $0.retries > 0 }
        let maxRetries = rows.map(\.retries).max() ?? 0

        // Under strict FIFO, admission order should follow arrival order. Walk the
        // admissions in arrival order and count the ones whose admission number went
        // backwards.
        let admitted = rows.compactMap { row -> (Int64, Int64)? in
            guard let a = row.admittedSeq else { return nil }
            return (row.seq, a)
        }.sorted { $0.0 < $1.0 }
        var inversions = 0
        if await n.controllers.existing(launch.id)?.config.policy == .strictFIFO {
            var highestAdmission: Int64 = -1
            for (_, admissionSeq) in admitted {
                if admissionSeq < highestAdmission { inversions += 1 }
                highestAdmission = max(highestAdmission, admissionSeq)
            }
        }

        // The absolute property. A customer's arrival sequence is fixed by the
        // (launch, customer) primary key on first insert, so a retry cannot change it;
        // this counts any row where that failed to hold.
        let moved = try await n.room.retriesThatChangedPosition(launchID: launch.id)

        return RetryAuditView(
            retried: retried,
            maxRetries: maxRetries,
            fifoInversions: inversions,
            admissionsChecked: admitted.count,
            retriesThatMovedPosition: moved
        )
    }

    // MARK: - Allocation and checkout

    /// Reserve without checkout. Exists for the week-one concurrency test: fire N
    /// simultaneous reservations at M units and assert exactly M succeed.
    app.post("launches", ":sku", "reserve") { req async throws -> ReserveView in
        let sku = try req.parameters.require("sku")
        let body = try req.content.decode(ReserveBody.self)
        let n = req.node
        let launch = try await n.launch(sku: sku)
        let outcome = try await n.allocator.reserve(
            launchID: launch.id, customerID: body.customerID, units: body.units ?? 1
        )
        return ReserveView(
            reservationID: outcome.reservation?.id,
            shardID: outcome.reservation?.shardID,
            probes: outcome.probes,
            succeeded: outcome.succeeded,
            expiresAt: outcome.reservation?.expiresAt
        )
    }

    app.delete("reservations", ":id") { req async throws -> HTTPStatus in
        let id = try req.parameters.require("id", as: UUID.self)
        let released = try await req.node.allocator.cancel(reservationID: id)
        return released ? .noContent : .notFound
    }

    app.post("launches", ":sku", "checkout") { req async throws -> CheckoutView in
        let sku = try req.parameters.require("sku")
        let body = try req.content.decode(CheckoutBody.self)
        let n = req.node
        let launch = try await n.launch(sku: sku)
        // A launch with no waiting room has no admission to check. The flag is read from
        // the launch row rather than from this node's controller registry, so a request
        // that lands on a node which did not create the launch behaves identically.
        let response = try await n.checkout.checkout(
            CheckoutRequest(
                launchID: launch.id, customerID: body.customerID,
                units: body.units ?? 1, idempotencyKey: body.idempotencyKey
            ),
            enforceAdmission: launch.requiresAdmission
        )
        return CheckoutView(response)
    }

    // MARK: - Failure handling

    app.post("launches", ":sku", "sweep") { req async throws -> SweepView in
        let sku = try req.parameters.require("sku")
        let n = req.node
        let launch = try await n.launch(sku: sku)
        let result = try await n.sweeper.sweepOnce(launchID: launch.id)
        return SweepView(
            reclaimed: result.reclaimed, bucketsVisited: result.bucketsVisited,
            durationMS: result.durationMS, strategy: "configured"
        )
    }

    /// Reclaims reservations whose owning node stopped heartbeating, and in-flight
    /// idempotency keys its dead workers left behind.
    app.post("launches", ":sku", "recover") { req async throws -> RecoveryView in
        let sku = try req.parameters.require("sku")
        let n = req.node
        let launch = try await n.launch(sku: sku)
        let orphans = try await n.recovery.recoverOrphans(launchID: launch.id)
        let claims = try await n.checkout.recoverAbandonedClaims(olderThan: .seconds(10))
        return RecoveryView(reservationsReclaimed: orphans, claimsReleased: claims)
    }

    app.post("admin", "reset") { req async throws -> HTTPStatus in
        guard ProcessInfo.processInfo.environment["ALLOW_RESET"] != "0" else {
            throw Abort(.forbidden, reason: "reset disabled")
        }
        let n = req.node
        await n.controllers.stopAll()
        await n.directory.clear()
        try await n.store.reset()
        return .noContent
    }
}
