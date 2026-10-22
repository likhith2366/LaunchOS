import Foundation

/// A launch is one SKU with a fixed number of units and a sharding configuration.
public struct Launch: Sendable, Codable, Equatable {
    public var id: UUID
    public var sku: String
    public var totalUnits: Int
    /// Number of inventory rows the units are split across. 1 means the hot-row baseline.
    public var shardCount: Int
    /// Remaining-unit count below which the shards are merged back into shard 0.
    /// Zero disables collapse.
    public var collapseThreshold: Int
    public var collapsed: Bool
    /// True when the launch has a waiting room, so checkout must check admission.
    public var requiresAdmission: Bool
    public var reservationTTL: Duration

    public init(
        id: UUID = UUID(),
        sku: String,
        totalUnits: Int,
        shardCount: Int = 1,
        collapseThreshold: Int = 0,
        collapsed: Bool = false,
        requiresAdmission: Bool = false,
        reservationTTL: Duration = .seconds(90)
    ) {
        self.id = id
        self.sku = sku
        self.totalUnits = totalUnits
        self.shardCount = shardCount
        self.collapseThreshold = collapseThreshold
        self.collapsed = collapsed
        self.requiresAdmission = requiresAdmission
        self.reservationTTL = reservationTTL
    }
}

/// One inventory row. The whole performance story of this project is about how many
/// of these exist per launch and how requests are spread over them.
public struct InventoryShard: Sendable, Codable, Equatable {
    public var shardID: Int
    public var available: Int
    public var reserved: Int
    public var sold: Int

    public init(shardID: Int, available: Int, reserved: Int, sold: Int) {
        self.shardID = shardID
        self.available = available
        self.reserved = reserved
        self.sold = sold
    }

    public var total: Int { available + reserved + sold }
}

/// The aggregate the invariant is asserted against: available + reserved + sold == total.
public struct InventoryState: Sendable, Codable, Equatable {
    public var launchID: UUID
    public var totalUnits: Int
    public var shards: [InventoryShard]
    public var collapsed: Bool

    public init(launchID: UUID, totalUnits: Int, shards: [InventoryShard], collapsed: Bool) {
        self.launchID = launchID
        self.totalUnits = totalUnits
        self.shards = shards
        self.collapsed = collapsed
    }

    public var available: Int { shards.reduce(0) { $0 + $1.available } }
    public var reserved: Int { shards.reduce(0) { $0 + $1.reserved } }
    public var sold: Int { shards.reduce(0) { $0 + $1.sold } }
    public var accountedFor: Int { available + reserved + sold }
    public var holds: Bool { accountedFor == totalUnits }
    /// Shards with stock left. The gap between this and `shardCount` is tail fragmentation.
    public var nonEmptyShards: Int { shards.count { $0.available > 0 } }
}

public enum ReservationState: String, Sendable, Codable {
    case held
    case committed
    case cancelled
    case expired
}

public struct Reservation: Sendable, Codable, Equatable {
    public var id: UUID
    public var launchID: UUID
    public var shardID: Int
    public var customerID: String
    public var units: Int
    public var state: ReservationState
    public var expiresAt: Date
    /// Which process holds this reservation, so recovery can tell an abandoned
    /// reservation from one whose owner is still alive.
    public var ownerNode: String
    public var probes: Int

    public init(
        id: UUID = UUID(),
        launchID: UUID,
        shardID: Int,
        customerID: String,
        units: Int,
        state: ReservationState = .held,
        expiresAt: Date,
        ownerNode: String,
        probes: Int = 1
    ) {
        self.id = id
        self.launchID = launchID
        self.shardID = shardID
        self.customerID = customerID
        self.units = units
        self.state = state
        self.expiresAt = expiresAt
        self.ownerNode = ownerNode
        self.probes = probes
    }
}

public struct Order: Sendable, Codable, Equatable {
    public var id: UUID
    public var launchID: UUID
    public var customerID: String
    public var reservationID: UUID
    public var units: Int
    public var createdAt: Date

    public init(
        id: UUID = UUID(),
        launchID: UUID,
        customerID: String,
        reservationID: UUID,
        units: Int,
        createdAt: Date = Date()
    ) {
        self.id = id
        self.launchID = launchID
        self.customerID = customerID
        self.reservationID = reservationID
        self.units = units
        self.createdAt = createdAt
    }
}

public enum QueueEntryState: String, Sendable, Codable {
    case waiting
    case admitted
    case expired
}

/// A position in the waiting room. `seq` comes from a Postgres sequence, which is what
/// makes queue order survive a restart and survive Redis being wiped.
public struct QueueEntry: Sendable, Codable, Equatable {
    public var launchID: UUID
    public var customerID: String
    public var seq: Int64
    public var bucket: Int64
    public var arrivalAt: Date
    public var state: QueueEntryState
    public var admittedSeq: Int64?
    public var admittedAt: Date?

    public init(
        launchID: UUID,
        customerID: String,
        seq: Int64,
        bucket: Int64,
        arrivalAt: Date,
        state: QueueEntryState,
        admittedSeq: Int64? = nil,
        admittedAt: Date? = nil
    ) {
        self.launchID = launchID
        self.customerID = customerID
        self.seq = seq
        self.bucket = bucket
        self.arrivalAt = arrivalAt
        self.state = state
        self.admittedSeq = admittedSeq
        self.admittedAt = admittedAt
    }
}

/// Result of allocating against the sharded inventory. `probes` is what makes tail
/// fragmentation measurable: it is 1 on a clean hit and climbs as shards drain.
public struct AllocationOutcome: Sendable, Equatable {
    public var reservation: Reservation?
    public var probes: Int
    public var collapsedDuringCall: Bool
    /// Stock left anywhere at the moment this attempt failed, or nil when it succeeded.
    ///
    /// Carried out of the allocator because the allocator already had to read it to
    /// decide between collapsing, reporting fragmentation and reporting a sell-out.
    /// Without it the caller asks the database the same question a second time, which is
    /// a wasted round-trip on the failure path and lets the two answers disagree.
    public var remainingWhenFailed: Int?

    public init(
        reservation: Reservation?,
        probes: Int,
        collapsedDuringCall: Bool = false,
        remainingWhenFailed: Int? = nil
    ) {
        self.reservation = reservation
        self.probes = probes
        self.collapsedDuringCall = collapsedDuringCall
        self.remainingWhenFailed = remainingWhenFailed
    }

    public var succeeded: Bool { reservation != nil }
}

public enum LaunchError: Error, Sendable, Equatable {
    case launchNotFound(String)
    case soldOut
    case reservationNotFound(UUID)
    case reservationNotHeld(UUID, ReservationState)
    case reservationExpired(UUID)
    case notAdmitted(String)
    case idempotencyInFlight(String)
    case idempotencyConflict(String)
    case invariantViolated(String)
}
