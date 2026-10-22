import Foundation
import Logging
import Testing

@testable import LaunchCore

/// Tests that need a real Postgres.
///
/// The invariants this project claims are enforced by conditional updates and
/// transactions, so they cannot be tested against a stub without testing the stub
/// instead. These run against a live database and skip themselves when one is not
/// configured, so `swift test` stays useful on a machine with nothing running.
///
///     POSTGRES_HOST=127.0.0.1 swift test
struct Harness {
    let store: Store
    let runTask: Task<Void, Never>

    static var isConfigured: Bool {
        ProcessInfo.processInfo.environment["POSTGRES_HOST"] != nil
    }

    init() async throws {
        var logger = Logger(label: "integration")
        logger.logLevel = .critical
        self.store = Store(config: .fromEnvironment(), logger: logger)
        self.runTask = Task { [store] in await store.run() }
        // The client connects lazily; migrate with a retry so a database that is still
        // starting does not fail the suite.
        var lastError: Error?
        for _ in 0..<40 {
            do {
                try await store.migrate()
                lastError = nil
                break
            } catch {
                lastError = error
                try? await Task.sleep(for: .milliseconds(250))
            }
        }
        if let lastError { throw lastError }
    }

    func shutdown() {
        runTask.cancel()
    }

    /// A launch with a unique sku, so tests never collide.
    func makeLaunch(
        units: Int,
        shards: Int,
        collapseThreshold: Int = 0,
        ttl: Duration = .seconds(60)
    ) async throws -> Launch {
        let launch = Launch(
            sku: "test-\(UUID().uuidString.prefix(8))",
            totalUnits: units,
            shardCount: shards,
            collapseThreshold: collapseThreshold,
            reservationTTL: ttl
        )
        try await store.createLaunch(launch)
        return launch
    }

    func allocator(config: AllocatorConfig = AllocatorConfig(), node: String = "test-node") -> Allocator {
        var logger = Logger(label: "allocator")
        logger.logLevel = .critical
        return Allocator(store: store, nodeID: node, config: config, logger: logger)
    }

    var room: WaitingRoom {
        var logger = Logger(label: "room")
        logger.logLevel = .critical
        return WaitingRoom(store: store, logger: logger)
    }
}

@Suite("Allocation against Postgres", .enabled(if: Harness.isConfigured))
struct AllocationIntegrationTests {

    /// The headline correctness claim: many more concurrent attempts than units, and
    /// exactly the unit count succeeds.
    @Test("exactly the stock count succeeds", arguments: [1, 4, 16])
    func noOversell(shards: Int) async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let units = 50
        let launch = try await h.makeLaunch(units: units, shards: shards)
        let allocator = h.allocator(config: AllocatorConfig(maxProbes: shards, collapseEnabled: false))

        var successes = 0
        try await withThrowingTaskGroup(of: Bool.self) { group in
            for i in 0..<(units * 10) {
                group.addTask {
                    let outcome = try await allocator.reserve(
                        launchID: launch.id, customerID: "c\(i)"
                    )
                    return outcome.succeeded
                }
            }
            for try await ok in group where ok { successes += 1 }
        }

        #expect(successes == units, "\(successes) reservations succeeded for \(units) units")
        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.reserved == units)
        #expect(state.available == 0)
    }

    @Test("committing moves a unit from reserved to sold")
    func commitMovesUnit() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 5, shards: 1)
        let allocator = h.allocator()

        let outcome = try await allocator.reserve(launchID: launch.id, customerID: "buyer")
        let reservation = try #require(outcome.reservation)

        var state = try await h.store.inventoryState(launchID: launch.id)
        #expect(state.available == 4)
        #expect(state.reserved == 1)
        #expect(state.sold == 0)

        let order = try await h.store.commitReservation(reservationID: reservation.id)
        #expect(order.units == 1)

        state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.available == 4)
        #expect(state.reserved == 0)
        #expect(state.sold == 1)
    }

    @Test("cancelling returns the unit to the pool")
    func cancelReturnsUnit() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 3, shards: 1)
        let allocator = h.allocator()

        let outcome = try await allocator.reserve(launchID: launch.id, customerID: "quitter")
        let reservation = try #require(outcome.reservation)
        #expect(try await allocator.cancel(reservationID: reservation.id))

        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.available == 3)
        #expect(state.reserved == 0)
    }

    /// Committing twice must not sell two units. The second attempt finds the
    /// reservation already committed and fails.
    @Test("a reservation cannot be committed twice")
    func noDoubleCommit() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 5, shards: 1)
        let allocator = h.allocator()
        let reservation = try #require(
            try await allocator.reserve(launchID: launch.id, customerID: "double").reservation
        )

        _ = try await h.store.commitReservation(reservationID: reservation.id)
        await #expect(throws: (any Error).self) {
            _ = try await h.store.commitReservation(reservationID: reservation.id)
        }

        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.sold == 1)
    }

    /// Two concurrent commits of one reservation: the conditional update means exactly
    /// one wins, which is the same mechanism that stops a retry selling a second unit.
    @Test("concurrent commits of one reservation produce one order")
    func concurrentCommitRace() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 5, shards: 1)
        let allocator = h.allocator()
        let reservation = try #require(
            try await allocator.reserve(launchID: launch.id, customerID: "racer").reservation
        )

        var committed = 0
        await withTaskGroup(of: Bool.self) { group in
            for _ in 0..<8 {
                group.addTask {
                    (try? await h.store.commitReservation(reservationID: reservation.id)) != nil
                }
            }
            for await ok in group where ok { committed += 1 }
        }

        #expect(committed == 1, "\(committed) commits succeeded for one reservation")
        #expect(try await h.store.orderCount(launchID: launch.id) == 1)
        _ = try await h.store.assertInvariant(launchID: launch.id)
    }

    @Test("cancelling a committed reservation does nothing")
    func cancelAfterCommit() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 2, shards: 1)
        let allocator = h.allocator()
        let reservation = try #require(
            try await allocator.reserve(launchID: launch.id, customerID: "late").reservation
        )
        _ = try await h.store.commitReservation(reservationID: reservation.id)

        #expect(try await allocator.cancel(reservationID: reservation.id) == false)
        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.sold == 1)
        #expect(state.available == 1)
    }

    /// Contention must not be reported as a sell-out.
    ///
    /// The probe statement skips shards another transaction is holding, so that requests
    /// spread out instead of queueing behind one row. On its own that turns a momentary
    /// lock into an allocation failure: a request whose every shard happened to be busy
    /// at that instant is told the launch is sold out while stock is sitting there. Few
    /// shards and many concurrent requests is the shape that provokes it, and every unit
    /// must still sell.
    @Test("heavy contention on few shards still sells every unit")
    func contentionDoesNotLoseSales() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let units = 40
        // Deliberately far more concurrent requests than shards, so that at any instant
        // most shards are locked by somebody.
        let launch = try await h.makeLaunch(units: units, shards: 2)
        let allocator = h.allocator(config: AllocatorConfig(maxProbes: 2, collapseEnabled: false))

        var successes = 0
        try await withThrowingTaskGroup(of: Bool.self) { group in
            for i in 0..<units {
                group.addTask {
                    try await allocator.reserve(launchID: launch.id, customerID: "c\(i)").succeeded
                }
            }
            for try await ok in group where ok { successes += 1 }
        }

        // Every request could have been satisfied, so every one must have been.
        #expect(successes == units, "\(successes) of \(units) allocated; contention lost sales")
        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.available == 0)
        #expect(state.reserved == units)
    }

    @Test("a sold-out launch allocates nothing")
    func soldOut() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 2, shards: 1)
        let allocator = h.allocator()
        for i in 0..<2 {
            #expect(try await allocator.reserve(launchID: launch.id, customerID: "c\(i)").succeeded)
        }
        let outcome = try await allocator.reserve(launchID: launch.id, customerID: "late")
        #expect(!outcome.succeeded)
        _ = try await h.store.assertInvariant(launchID: launch.id)
    }
}

@Suite("Shard collapse", .enabled(if: Harness.isConfigured))
struct CollapseIntegrationTests {

    /// Collapse must be unit-preserving. An earlier version drained the shards without
    /// crediting shard 0 and silently lost every unit that was still available, which
    /// is exactly what this asserts against.
    @Test("collapse preserves every unit")
    func collapsePreservesUnits() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 64, shards: 16, collapseThreshold: 0)
        let allocator = h.allocator(config: AllocatorConfig(maxProbes: 16, collapseEnabled: false))

        // Take some stock first, so collapse has to move a mix of available and
        // reserved rather than a clean split.
        for i in 0..<20 {
            _ = try await allocator.reserve(launchID: launch.id, customerID: "pre\(i)")
        }
        let before = try await h.store.assertInvariant(launchID: launch.id)
        #expect(before.available == 44)
        #expect(before.reserved == 20)

        #expect(try await h.store.collapseShards(launchID: launch.id))

        let after = try await h.store.assertInvariant(launchID: launch.id)
        #expect(after.available == before.available, "collapse changed the available count")
        #expect(after.reserved == before.reserved, "collapse moved reserved units")
        #expect(after.nonEmptyShards == 1, "stock is still spread over \(after.nonEmptyShards) shards")
        #expect(after.shards.first { $0.shardID == 0 }?.available == 44)
    }

    @Test("collapse happens only once")
    func collapseIsIdempotent() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 32, shards: 8)
        #expect(try await h.store.collapseShards(launchID: launch.id))
        #expect(try await h.store.collapseShards(launchID: launch.id) == false)
        _ = try await h.store.assertInvariant(launchID: launch.id)
    }

    /// Concurrent collapse attempts must not double-count the pool.
    @Test("racing collapses are safe")
    func concurrentCollapse() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 48, shards: 16)

        var wins = 0
        await withTaskGroup(of: Bool.self) { group in
            for _ in 0..<6 {
                group.addTask { (try? await h.store.collapseShards(launchID: launch.id)) ?? false }
            }
            for await won in group where won { wins += 1 }
        }
        #expect(wins == 1, "\(wins) collapses reported success")
        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.available == 48)
    }

    /// After a collapse, a released unit has to come back to shard 0. Returning it to
    /// its original shard would silently undo the collapse and reintroduce the
    /// fragmentation it was done to remove.
    @Test("a released unit returns to the collapsed shard")
    func releaseAfterCollapse() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 32, shards: 8)
        let allocator = h.allocator(config: AllocatorConfig(maxProbes: 8, collapseEnabled: false))

        // Reserve from a shard that is not shard 0.
        var reservation: Reservation?
        for i in 0..<40 {
            if let r = try await allocator.reserve(launchID: launch.id, customerID: "c\(i)").reservation,
                r.shardID != 0
            {
                reservation = r
                break
            }
        }
        let held = try #require(reservation, "no reservation landed outside shard 0")

        #expect(try await h.store.collapseShards(launchID: launch.id))
        #expect(try await h.store.releaseReservation(reservationID: held.id, as: .cancelled))

        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.nonEmptyShards <= 1, "the release un-collapsed the launch")
        #expect(state.shards.first { $0.shardID == 0 }?.available == state.available)
    }

    /// Allocation must keep working through a collapse, with nothing lost.
    @Test("allocation during collapse stays correct")
    func allocateDuringCollapse() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 100, shards: 16, collapseThreshold: 40)
        let allocator = h.allocator(config: AllocatorConfig(maxProbes: 4, collapseEnabled: true))

        var successes = 0
        try await withThrowingTaskGroup(of: Bool.self) { group in
            for i in 0..<300 {
                group.addTask {
                    try await allocator.reserve(launchID: launch.id, customerID: "c\(i)").succeeded
                }
            }
            for try await ok in group where ok { successes += 1 }
        }

        #expect(successes == 100, "\(successes) succeeded for 100 units")
        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.reserved == 100)
    }
}

/// These sweep and then assert how much they reclaimed, so they must be the only
/// sweeper touching the database. A running allocator node has its own background
/// sweeper and will reclaim these reservations first, which fails the assertions for a
/// reason that has nothing to do with the code under test — `scripts/wsl-test.sh` stops
/// the cluster before running.
@Suite("Reservation expiry", .enabled(if: Harness.isConfigured), .serialized)
struct ExpiryIntegrationTests {

    @Test("expired reservations come back to the pool", arguments: SweepStrategy.allCases)
    func sweepReclaims(strategy: SweepStrategy) async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        // A TTL short enough that the test does not have to wait on a real one.
        let launch = try await h.makeLaunch(units: 10, shards: 2, ttl: .milliseconds(100))
        let allocator = h.allocator()

        for i in 0..<10 {
            #expect(try await allocator.reserve(launchID: launch.id, customerID: "c\(i)").succeeded)
        }
        var state = try await h.store.inventoryState(launchID: launch.id)
        #expect(state.available == 0)
        #expect(state.reserved == 10)

        try await Task.sleep(for: .milliseconds(400))
        let result = try await h.store.sweepExpired(
            strategy: strategy, batchSize: 100, launchID: launch.id
        )
        #expect(result.reclaimed == 10)

        state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.available == 10)
        #expect(state.reserved == 0)
    }

    /// The sweep is bounded so a huge backlog cannot stall allocation while it runs.
    @Test("the sweep respects its batch size")
    func boundedBatches() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 20, shards: 4, ttl: .milliseconds(100))
        let allocator = h.allocator()
        for i in 0..<20 {
            _ = try await allocator.reserve(launchID: launch.id, customerID: "c\(i)")
        }
        try await Task.sleep(for: .milliseconds(400))

        let first = try await h.store.sweepExpired(strategy: .indexScan, batchSize: 5, launchID: launch.id)
        #expect(first.reclaimed == 5)
        let state = try await h.store.inventoryState(launchID: launch.id)
        #expect(state.available == 5)
        #expect(state.reserved == 15)
        _ = try await h.store.assertInvariant(launchID: launch.id)
    }

    /// An expired reservation must not be committable. If it were, the unit the sweeper
    /// already gave back would be sold as well — the classic double-spend.
    @Test("an expired reservation cannot be committed")
    func expiredCannotCommit() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 4, shards: 1, ttl: .milliseconds(100))
        let allocator = h.allocator()
        let reservation = try #require(
            try await allocator.reserve(launchID: launch.id, customerID: "slow").reservation
        )

        try await Task.sleep(for: .milliseconds(400))
        await #expect(throws: (any Error).self) {
            _ = try await h.store.commitReservation(reservationID: reservation.id)
        }

        _ = try await h.store.sweepExpired(strategy: .bucketed, batchSize: 10, launchID: launch.id)
        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.available == 4)
        #expect(state.sold == 0)
    }

    /// The sweeper and a commit racing for the same reservation: exactly one wins, and
    /// the invariant holds whichever it is.
    @Test("sweeper and commit cannot both win")
    func sweepCommitRace() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 50, shards: 4, ttl: .milliseconds(150))
        let allocator = h.allocator()
        var reservations: [UUID] = []
        for i in 0..<50 {
            if let r = try await allocator.reserve(launchID: launch.id, customerID: "c\(i)").reservation {
                reservations.append(r.id)
            }
        }

        try await Task.sleep(for: .milliseconds(150))
        async let sweepResult = h.store.sweepExpired(
            strategy: .bucketed, batchSize: 100, launchID: launch.id
        )
        async let commits: Int = {
            var ok = 0
            for id in reservations {
                if (try? await h.store.commitReservation(reservationID: id)) != nil { ok += 1 }
            }
            return ok
        }()
        let (swept, committed) = try await (sweepResult, commits)

        // Whatever the split, no unit may be both sold and returned.
        #expect(swept.reclaimed + committed <= 50)
        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.sold == committed)
    }
}

@Suite("Waiting room", .enabled(if: Harness.isConfigured))
struct QueueIntegrationTests {

    /// The property a real waiting room lives or dies by: refreshing must not move you.
    @Test("retries keep the position they arrived with")
    func retriesDoNotJumpTheQueue() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 10, shards: 1)
        let room = h.room

        let first = try await room.join(launchID: launch.id, customerID: "refresher")
        // Other customers arrive behind them.
        for i in 0..<20 {
            _ = try await room.join(launchID: launch.id, customerID: "other\(i)")
        }

        for _ in 0..<20 {
            let again = try await room.join(launchID: launch.id, customerID: "refresher")
            #expect(again.entry.seq == first.entry.seq, "arrival sequence changed on retry")
            #expect(again.entry.bucket == first.entry.bucket)
            #expect(again.position == first.position, "position moved on retry")
            #expect(again.wasAlreadyQueued)
        }

        let audit = try await room.retryAudit(launchID: launch.id)
        let refresher = try #require(audit.first { $0.customerID == "refresher" })
        #expect(refresher.retries == 20)
        #expect(try await room.retriesThatChangedPosition(launchID: launch.id) == 0)
    }

    @Test("position counts only those ahead and still waiting")
    func positionCounting() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 10, shards: 1)
        let room = h.room

        var joins: [QueueJoinResult] = []
        for i in 0..<10 {
            joins.append(try await room.join(launchID: launch.id, customerID: "c\(i)"))
        }
        #expect(joins.first?.position == 0)
        #expect(joins.last?.position == 9)
        #expect(try await room.depth(launchID: launch.id) == 10)

        // Admitting the front of the queue moves everyone behind them up.
        let admitted = try await room.admit(
            launchID: launch.id, count: 3, policy: .strictFIFO, commitHorizon: .zero
        )
        #expect(admitted.count == 3)
        #expect(try await room.depth(launchID: launch.id) == 7)
        let lastPosition = try await room.position(launchID: launch.id, seq: joins.last!.entry.seq)
        #expect(lastPosition == 6)
    }

    @Test("strict FIFO admits in arrival order")
    func strictFIFOOrder() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 100, shards: 1)
        let room = h.room
        // Sequential joins, so sequence order and commit order cannot disagree and the
        // ordering being tested is the policy's, not the horizon's.
        var order: [String] = []
        for i in 0..<30 {
            let id = "c\(String(format: "%03d", i))"
            _ = try await room.join(launchID: launch.id, customerID: id)
            order.append(id)
        }

        var admittedOrder: [String] = []
        while admittedOrder.count < 30 {
            let batch = try await room.admit(
                launchID: launch.id, count: 7, policy: .strictFIFO, commitHorizon: .zero
            )
            if batch.isEmpty { break }
            admittedOrder += batch.sorted { $0.admissionSeq < $1.admissionSeq }.map(\.customerID)
        }
        #expect(admittedOrder == order)
    }

    @Test("a lottery does not admit in arrival order")
    func lotteryIsNotFIFO() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 200, shards: 1)
        let room = h.room
        for i in 0..<200 {
            _ = try await room.join(launchID: launch.id, customerID: "c\(String(format: "%03d", i))")
        }
        // One batch over the whole pool, so the draw really is over everyone waiting.
        let batch = try await room.admit(
            launchID: launch.id, count: 200, policy: .lottery, commitHorizon: .zero
        )
        #expect(batch.count == 200)

        let arrival = batch.map { Double($0.arrivalSeq) }
        let admission = batch.map { Double($0.admissionSeq) }
        let correlation = spearman(arrival, admission)
        #expect(abs(correlation) < 0.5, "a lottery correlated at \(correlation)")
    }

    @Test("admission never hands the same customer out twice")
    func noDoubleAdmission() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 100, shards: 1)
        let room = h.room
        for i in 0..<60 {
            _ = try await room.join(launchID: launch.id, customerID: "c\(i)")
        }

        // Several admitters at once, as several nodes would be.
        var all: [String] = []
        try await withThrowingTaskGroup(of: [AdmissionRecord].self) { group in
            for _ in 0..<6 {
                group.addTask {
                    try await room.admit(
                        launchID: launch.id, count: 20, policy: .strictFIFO, commitHorizon: .zero
                    )
                }
            }
            for try await batch in group { all += batch.map(\.customerID) }
        }

        #expect(all.count == 60, "admitted \(all.count) of 60")
        #expect(Set(all).count == all.count, "a customer was admitted more than once")
        #expect(try await room.depth(launchID: launch.id) == 0)
    }

    /// The commit horizon holds back entries that are too new to trust the ordering of.
    @Test("the commit horizon defers recent arrivals")
    func horizonDefersRecentJoins() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 50, shards: 1)
        let room = h.room
        for i in 0..<10 {
            _ = try await room.join(launchID: launch.id, customerID: "c\(i)")
        }

        // Nothing is old enough yet.
        let tooSoon = try await room.admit(
            launchID: launch.id, count: 10, policy: .strictFIFO, commitHorizon: .seconds(30)
        )
        #expect(tooSoon.isEmpty, "the horizon admitted \(tooSoon.count) entries it should have held")

        // With no horizon the same entries are admissible immediately.
        let now = try await room.admit(
            launchID: launch.id, count: 10, policy: .strictFIFO, commitHorizon: .zero
        )
        #expect(now.count == 10)
    }

    @Test("admitted customers are reported as admitted")
    func admittedFlag() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 10, shards: 1)
        let room = h.room
        _ = try await room.join(launchID: launch.id, customerID: "chosen")
        _ = try await room.join(launchID: launch.id, customerID: "waiting")

        #expect(try await room.isAdmitted(launchID: launch.id, customerID: "chosen") == false)
        _ = try await room.admit(
            launchID: launch.id, count: 1, policy: .strictFIFO, commitHorizon: .zero
        )
        #expect(try await room.isAdmitted(launchID: launch.id, customerID: "chosen"))
        #expect(try await room.isAdmitted(launchID: launch.id, customerID: "waiting") == false)
        #expect(try await room.isAdmitted(launchID: launch.id, customerID: "stranger") == false)
    }
}

@Suite("Checkout idempotency", .enabled(if: Harness.isConfigured))
struct IdempotencyIntegrationTests {

    /// Idempotency keys are global: the table's primary key is the key by itself, which
    /// is correct — a key identifies one request across the whole system, not one per
    /// launch. It does mean a test cannot use a literal key, because the row outlives
    /// the run and the next run sees it as a key reused for different content. Each test
    /// therefore mints its own.
    private func key(_ name: String) -> String {
        "\(name)-\(UUID().uuidString)"
    }

    private func coordinator(_ h: Harness, node: String = "test-node") -> CheckoutCoordinator {
        var logger = Logger(label: "checkout")
        logger.logLevel = .critical
        return CheckoutCoordinator(
            store: h.store,
            allocator: h.allocator(node: node),
            room: h.room,
            health: HealthMonitor(),
            nodeID: node,
            logger: logger,
            requireAdmission: false
        )
    }

    @Test("a retry returns the original order rather than a second one")
    func retryReplaysTheOrder() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 5, shards: 1)
        let checkout = coordinator(h)
        let request = CheckoutRequest(
            launchID: launch.id, customerID: "buyer", idempotencyKey: key("retry")
        )

        let first = try await checkout.checkout(request, enforceAdmission: false)
        #expect(first.outcome == .purchased)
        #expect(!first.replayed)

        // Five more attempts with the same key, as a client retrying a lost response.
        for _ in 0..<5 {
            let again = try await checkout.checkout(request, enforceAdmission: false)
            #expect(again.outcome == .purchased)
            #expect(again.orderID == first.orderID, "a retry produced a different order")
            #expect(again.replayed)
        }

        #expect(try await h.store.orderCount(launchID: launch.id) == 1)
        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.sold == 1)
    }

    /// The race the whole mechanism exists for.
    @Test("concurrent duplicates produce exactly one order")
    func concurrentDuplicates() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 20, shards: 4)
        let checkout = coordinator(h)

        var orderIDs = Set<UUID>()
        var purchases = 0
        try await withThrowingTaskGroup(of: CheckoutResponse?.self) { group in
            for customer in 0..<10 {
                let request = CheckoutRequest(
                    launchID: launch.id, customerID: "c\(customer)",
                    idempotencyKey: "dup-\(customer)-\(launch.id.uuidString)"
                )
                // Four concurrent attempts per customer, all with one key.
                for _ in 0..<4 {
                    group.addTask {
                        try? await checkout.checkout(request, enforceAdmission: false)
                    }
                }
            }
            for try await response in group {
                guard let response, response.outcome == .purchased else { continue }
                purchases += 1
                if let id = response.orderID { orderIDs.insert(id) }
            }
        }

        // Ten customers, so ten orders however many requests arrived.
        #expect(orderIDs.count == 10, "\(orderIDs.count) distinct orders for 10 customers")
        #expect(try await h.store.orderCount(launchID: launch.id) == 10)
        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.sold == 10)
        #expect(purchases >= 10)
    }

    /// A key reused for different content is a client bug, and saying so is better than
    /// answering with the wrong order.
    @Test("reusing a key for a different request is rejected")
    func fingerprintMismatch() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 10, shards: 1)
        let checkout = coordinator(h)

        let shared = key("shared")
        _ = try await checkout.checkout(
            CheckoutRequest(launchID: launch.id, customerID: "a", units: 1, idempotencyKey: shared),
            enforceAdmission: false
        )
        await #expect(throws: LaunchError.idempotencyConflict(shared)) {
            _ = try await checkout.checkout(
                CheckoutRequest(
                    launchID: launch.id, customerID: "b", units: 1, idempotencyKey: shared
                ),
                enforceAdmission: false
            )
        }
        #expect(try await h.store.orderCount(launchID: launch.id) == 1)
    }

    /// A sell-out is a terminal answer, so a retry must get the same answer rather than
    /// a fresh attempt that might succeed off someone else's cancellation.
    @Test("a sold-out answer is replayed too")
    func soldOutIsSticky() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 1, shards: 1)
        let checkout = coordinator(h)

        _ = try await checkout.checkout(
            CheckoutRequest(launchID: launch.id, customerID: "first", idempotencyKey: key("k1")),
            enforceAdmission: false
        )
        let request = CheckoutRequest(
            launchID: launch.id, customerID: "second", idempotencyKey: key("k2")
        )
        let denied = try await checkout.checkout(request, enforceAdmission: false)
        #expect(denied.outcome == .soldOut)

        let retry = try await checkout.checkout(request, enforceAdmission: false)
        #expect(retry.outcome == .soldOut)
        #expect(retry.replayed)
        _ = try await h.store.assertInvariant(launchID: launch.id)
    }

    @Test("admission is enforced when the launch has a waiting room")
    func admissionEnforced() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 5, shards: 1)
        let checkout = coordinator(h)
        let request = CheckoutRequest(
            launchID: launch.id, customerID: "gatecrasher", idempotencyKey: key("gate")
        )

        await #expect(throws: LaunchError.notAdmitted("gatecrasher")) {
            _ = try await checkout.checkout(request, enforceAdmission: true)
        }

        _ = try await h.room.join(launchID: launch.id, customerID: "gatecrasher")
        _ = try await h.room.admit(
            launchID: launch.id, count: 1, policy: .strictFIFO, commitHorizon: .zero
        )
        let response = try await checkout.checkout(request, enforceAdmission: true)
        #expect(response.outcome == .purchased)
    }

    /// A worker that dies mid-checkout leaves its key in flight. Nothing may lock the
    /// customer out of their own order permanently.
    @Test("abandoned in-flight keys are recoverable")
    func abandonedClaimsRecovered() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 5, shards: 1)

        // Simulate the dead worker by writing the in-flight record it would have left.
        let orphanKey = key("orphan")
        let fingerprint = "\(launch.id.uuidString):ghost:1"
        _ = try await h.store.client.query(
            """
            INSERT INTO idempotency_keys
                (key, launch_id, customer_id, fingerprint, state, owner_node, updated_at)
            VALUES (\(orphanKey), \(launch.id), 'ghost', \(fingerprint),
                    'in_flight', 'dead-node', now() - interval '60 seconds')
            """,
            logger: silentLogger
        )

        let checkout = coordinator(h)
        let request = CheckoutRequest(
            launchID: launch.id, customerID: "ghost", idempotencyKey: orphanKey
        )
        // While the record stands, the retry is told the original is still running.
        await #expect(throws: LaunchError.idempotencyInFlight(orphanKey)) {
            _ = try await checkout.checkout(request, enforceAdmission: false)
        }

        #expect(try await checkout.recoverAbandonedClaims(olderThan: .seconds(10)) >= 1)
        let response = try await checkout.checkout(request, enforceAdmission: false)
        #expect(response.outcome == .purchased)
        _ = try await h.store.assertInvariant(launchID: launch.id)
    }
}

@Suite("Shard leases", .enabled(if: Harness.isConfigured))
struct LeaseIntegrationTests {

    private func manager(_ h: Harness, node: String, ttl: Duration = .seconds(5)) -> LeaseManager {
        LeaseManager(store: h.store, nodeID: node, logger: silentLogger, ttl: ttl)
    }

    @Test("a live lease cannot be stolen")
    func liveLeaseHolds() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 16, shards: 4)

        let a = manager(h, node: "node-a")
        let b = manager(h, node: "node-b")
        let held = try #require(try await a.acquire(launchID: launch.id, shardID: 0))
        #expect(try await b.acquire(launchID: launch.id, shardID: 0) == nil)

        let owners = try await a.currentOwners(launchID: launch.id)
        #expect(owners[0] == "node-a")
        #expect(held.fenceToken > 0)
    }

    /// Renewing is not a change of ownership, so it must not issue a new fence token —
    /// otherwise a node would keep invalidating its own work.
    @Test("renewal keeps the fence token")
    func renewalKeepsToken() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 16, shards: 4)
        let a = manager(h, node: "node-a")

        let first = try #require(try await a.acquire(launchID: launch.id, shardID: 1))
        let renewed = try #require(try await a.acquire(launchID: launch.id, shardID: 1))
        #expect(renewed.fenceToken == first.fenceToken)
        #expect(renewed.expiresAt >= first.expiresAt)
    }

    /// Taking over an expired lease must issue a new token, so the previous owner can
    /// be told it is no longer in charge.
    @Test("takeover issues a new fence token")
    func takeoverBumpsToken() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 16, shards: 4)
        let a = manager(h, node: "node-a", ttl: .milliseconds(150))
        let b = manager(h, node: "node-b", ttl: .seconds(5))

        let original = try #require(try await a.acquire(launchID: launch.id, shardID: 2))
        try await Task.sleep(for: .milliseconds(400))
        #expect(try await a.expiredShards(launchID: launch.id).contains(2))

        let stolen = try #require(try await b.acquire(launchID: launch.id, shardID: 2))
        #expect(stolen.fenceToken > original.fenceToken, "the token did not change on takeover")

        // The old owner now discovers it has lost the shard.
        let renewed = try await a.renew(launchID: launch.id, leases: [original])
        #expect(renewed.isEmpty, "the previous owner still believes it holds the lease")
    }

    @Test("nodes take disjoint shares")
    func disjointShares() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 64, shards: 16)
        let a = manager(h, node: "node-a")
        let b = manager(h, node: "node-b")

        let aLeases = try await a.acquireShare(
            launchID: launch.id, shardCount: 16, nodeIndex: 0, nodeCount: 2
        )
        let bLeases = try await b.acquireShare(
            launchID: launch.id, shardCount: 16, nodeIndex: 1, nodeCount: 2
        )
        let aShards = Set(aLeases.map(\.shardID))
        let bShards = Set(bLeases.map(\.shardID))

        #expect(aShards.intersection(bShards).isEmpty, "two nodes hold the same shard")
        #expect(!aShards.isEmpty)
        #expect(!bShards.isEmpty)
    }

    @Test("releasing lets another node take over immediately")
    func releaseAllowsTakeover() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 16, shards: 4)
        let a = manager(h, node: "node-a")
        let b = manager(h, node: "node-b")

        _ = try await a.acquire(launchID: launch.id, shardID: 3)
        try await a.release(launchID: launch.id, shardID: 3)
        #expect(try await b.acquire(launchID: launch.id, shardID: 3) != nil)
    }
}

@Suite("Multi-node allocation", .enabled(if: Harness.isConfigured))
struct MultiNodeIntegrationTests {

    /// Several allocators against the same sharded inventory, which is the
    /// Postgres-arbitrated mode. Correctness must not depend on how many are running.
    @Test("three nodes allocating at once do not oversell")
    func threeNodesNoOversell() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let units = 60
        let launch = try await h.makeLaunch(units: units, shards: 8)
        let allocators = (0..<3).map {
            h.allocator(config: AllocatorConfig(maxProbes: 8, collapseEnabled: false), node: "node-\($0)")
        }

        var successes = 0
        try await withThrowingTaskGroup(of: Bool.self) { group in
            for i in 0..<(units * 5) {
                let allocator = allocators[i % allocators.count]
                group.addTask {
                    try await allocator.reserve(launchID: launch.id, customerID: "c\(i)").succeeded
                }
            }
            for try await ok in group where ok { successes += 1 }
        }

        #expect(successes == units, "\(successes) succeeded for \(units) units")
        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.reserved == units)
    }

    /// Reservations held by a node that has gone away have to be reclaimable without
    /// waiting out their TTL.
    @Test("a dead node's reservations are identifiable")
    func orphanDetection() async throws {
        let h = try await Harness()
        defer { h.shutdown() }
        let launch = try await h.makeLaunch(units: 20, shards: 4)
        let live = h.allocator(node: "live-node")
        let doomed = h.allocator(node: "doomed-node")

        for i in 0..<5 { _ = try await live.reserve(launchID: launch.id, customerID: "live\(i)") }
        for i in 0..<5 { _ = try await doomed.reserve(launchID: launch.id, customerID: "dead\(i)") }

        let orphans = try await h.store.orphanedReservations(
            excludingNodes: ["live-node"], launchID: launch.id
        )
        #expect(orphans.count == 5)

        for id in orphans {
            #expect(try await h.store.releaseReservation(reservationID: id, as: .expired))
        }
        let state = try await h.store.assertInvariant(launchID: launch.id)
        #expect(state.reserved == 5)
        #expect(state.available == 15)
    }
}
