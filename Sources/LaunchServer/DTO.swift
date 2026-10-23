import Foundation
import LaunchCore
import Vapor

struct CreateLaunchBody: Content {
    var sku: String
    var totalUnits: Int
    var shardCount: Int?
    var collapseThreshold: Int?
    var reservationTTLSeconds: Double?
    /// Admission settings for this launch. Absent means the launch runs without a
    /// waiting room, which is the week-one baseline.
    var admission: AdmissionBody?
}

struct AdmissionBody: Content {
    var kind: String?
    var policy: String?
    var intervalMS: Double?
    var initialRate: Double?
    var minRate: Double?
    var maxRate: Double?
    var increaseStep: Double?
    var decreaseFactor: Double?
    var latencyThresholdMS: Double?
    var inFlightThreshold: Double?
    var errorRateThreshold: Double?
    var deadBand: Double?
    var hardInFlightCeiling: Int?
    var jitterReleases: Bool?
    var commitHorizonMS: Double?

    func config() -> AdmissionConfig {
        AdmissionConfig(
            kind: AdmitterKind(rawValue: kind ?? "") ?? .aimd,
            policy: FairnessPolicy(rawValue: policy ?? "") ?? .bucketedFIFO,
            interval: .milliseconds(Int(intervalMS ?? 500)),
            initialRate: initialRate ?? 50,
            minRate: minRate ?? 5,
            maxRate: maxRate ?? 5_000,
            increaseStep: increaseStep ?? 25,
            decreaseFactor: decreaseFactor ?? 0.5,
            latencyThresholdMS: latencyThresholdMS ?? 250,
            inFlightThreshold: inFlightThreshold ?? 120,
            errorRateThreshold: errorRateThreshold ?? 0.02,
            deadBand: deadBand ?? 0.15,
            hardInFlightCeiling: hardInFlightCeiling ?? 400,
            jitterReleases: jitterReleases ?? true,
            commitHorizon: .milliseconds(Int(commitHorizonMS ?? 250))
        )
    }
}

struct LaunchView: Content {
    var id: UUID
    var sku: String
    var totalUnits: Int
    var shardCount: Int
    var collapseThreshold: Int
    var collapsed: Bool
    var requiresAdmission: Bool

    init(_ launch: Launch) {
        self.id = launch.id
        self.sku = launch.sku
        self.totalUnits = launch.totalUnits
        self.shardCount = launch.shardCount
        self.collapseThreshold = launch.collapseThreshold
        self.collapsed = launch.collapsed
        self.requiresAdmission = launch.requiresAdmission
    }
}

struct ShardView: Content {
    var shardID: Int
    var available: Int
    var reserved: Int
    var sold: Int
}

struct InventoryView: Content {
    var launchID: UUID
    var totalUnits: Int
    var available: Int
    var reserved: Int
    var sold: Int
    var collapsed: Bool
    /// available + reserved + sold. Exposed so the dashboard and the simulator can both
    /// check the invariant rather than trusting the server to have checked it.
    var accountedFor: Int
    var invariantHolds: Bool
    var nonEmptyShards: Int
    var shards: [ShardView]

    init(_ state: InventoryState) {
        self.launchID = state.launchID
        self.totalUnits = state.totalUnits
        self.available = state.available
        self.reserved = state.reserved
        self.sold = state.sold
        self.collapsed = state.collapsed
        self.accountedFor = state.accountedFor
        self.invariantHolds = state.holds
        self.nonEmptyShards = state.nonEmptyShards
        self.shards = state.shards.map {
            ShardView(shardID: $0.shardID, available: $0.available, reserved: $0.reserved, sold: $0.sold)
        }
    }
}

struct JoinQueueBody: Content {
    var customerID: String
}

struct QueueStatusView: Content {
    var customerID: String
    var arrivalSeq: Int64
    var position: Int
    /// Whether the position was estimated in constant time, counted exactly, or served
    /// from cache. Exposed so a caller can tell the three apart rather than having to
    /// trust one number.
    var positionSource: String
    var state: String
    var admitted: Bool
    var retries: Int
    var waitSeconds: Double
    /// True when this request found an existing queue entry rather than creating one.
    /// A retrying client sees this and an unchanged `arrivalSeq`.
    var wasAlreadyQueued: Bool
}

struct CheckoutBody: Content {
    var customerID: String
    var units: Int?
    var idempotencyKey: String
    /// Take the unit and record the order in one statement, for a sale with no payment
    /// step between reserving and committing. Absent means the two-phase flow, which is
    /// what a sale with a payment authorisation needs.
    var singlePhase: Bool?
}

struct CheckoutView: Content {
    var outcome: String
    var orderID: UUID?
    var reservationID: UUID?
    var units: Int
    var probes: Int
    var replayed: Bool

    init(_ response: CheckoutResponse) {
        self.outcome = response.outcome.rawValue
        self.orderID = response.orderID
        self.reservationID = response.reservationID
        self.units = response.units
        self.probes = response.probes
        self.replayed = response.replayed
    }
}

struct ReserveBody: Content {
    var customerID: String
    var units: Int?
}

struct ReserveView: Content {
    var reservationID: UUID?
    var shardID: Int?
    var probes: Int
    var succeeded: Bool
    var expiresAt: Date?
}

struct AdmissionStatusView: Content {
    var kind: String
    var policy: String
    var rate: Double
    var totalAdmitted: Int
    var queueDepth: Int
    var meanQueueDepth: Double
    var health: HealthSnapshot
}

struct ControlTraceView: Content {
    var samples: [ControlSample]
}

struct FairnessView: Content {
    var report: FairnessReport
    /// Mean and p99 time for the admit statement itself, i.e. what the policy costs.
    var admitMeanMS: Double
    var admitP99MS: Double
    var admitBatches: Int
}

struct RetryAuditView: Content {
    /// Customers who retried at least once.
    var retried: Int
    var maxRetries: Int
    /// Admissions that happened out of arrival-sequence order.
    ///
    /// Distinct from a retry improving a position, and caused by something else
    /// entirely: sequence numbers are taken at INSERT and can commit out of order, so
    /// an admitter can miss a lower-numbered entry that has not landed yet. Bounded by
    /// the admission commit horizon, and reported rather than asserted to be zero
    /// because the horizon is a tunable, not a guarantee.
    var fifoInversions: Int
    /// Total admissions the inversion count was computed over.
    var admissionsChecked: Int
    /// The property that must hold absolutely: no customer's arrival sequence changed,
    /// however many times they retried.
    var retriesThatMovedPosition: Int
}

struct NodeStatusView: Content {
    var nodeID: String
    var nodeIndex: Int
    var nodeCount: Int
    var multiNodeMode: String
    var ownedShards: [Int]
    var liveNodes: [String]
    var redisEnabled: Bool
    /// The expiry-sweep strategy this node was started with. Reported here because the
    /// sweep measurement has to name the strategy that actually ran, and that is a
    /// property of the node, not of whoever is asking.
    var sweepStrategy: String
    var maxProbes: Int
}

struct SweepView: Content {
    var reclaimed: Int
    var bucketsVisited: Int
    var durationMS: Double
    var strategy: String
}

struct ErrorView: Content {
    var error: String
    var reason: String
}

struct RecoveryView: Content {
    var reservationsReclaimed: Int
    var claimsReleased: Int
}

struct SeedQueueBody: Content {
    var count: Int
    var prefix: String?
}

struct SeedQueueView: Content {
    var inserted: Int
    var queueDepth: Int
}

struct PositionBenchmarkBody: Content {
    /// Defaults to the customer at the back of the queue, which is the worst case for
    /// the count the estimate replaces.
    var customerID: String?
    var iterations: Int?
}

struct PositionBenchmarkView: Content {
    var result: PositionBenchmarkResult
}
