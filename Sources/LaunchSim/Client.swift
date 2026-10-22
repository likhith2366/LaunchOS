import AsyncHTTPClient
import Foundation
import LaunchCore
import NIOCore
import NIOFoundationCompat
import NIOHTTP1

struct ClientError: Error, CustomStringConvertible {
    var status: UInt
    var body: String
    var description: String { "HTTP \(status): \(body)" }
}

/// Thin typed client over the node API.
///
/// Measures its own latency per call and reports status codes faithfully rather than
/// throwing them away, because the difference between "the server refused" and "the
/// request failed" is the difference between admission control working and the system
/// falling over.
struct LaunchClient: Sendable {
    let http: HTTPClient
    let baseURLs: [String]
    let timeout: TimeAmount

    init(http: HTTPClient, baseURLs: [String], timeout: TimeAmount = .seconds(30)) {
        self.http = http
        self.baseURLs = baseURLs
        self.timeout = timeout
    }

    /// Spreads requests over the nodes. Which node handles a request must not matter,
    /// so the simulator deliberately does not pin a customer to one.
    func baseURL(for salt: UInt64) -> String {
        baseURLs[Int(salt % UInt64(baseURLs.count))]
    }

    struct Reply<T: Decodable & Sendable>: Sendable {
        var value: T?
        var status: UInt
        var latencyMS: Double
        var body: String

        var ok: Bool { status >= 200 && status < 300 }
    }

    private func send<Body: Encodable, T: Decodable & Sendable>(
        _ method: HTTPMethod,
        _ path: String,
        body: Body?,
        salt: UInt64,
        decode: T.Type
    ) async throws -> Reply<T> {
        var request = HTTPClientRequest(url: baseURL(for: salt) + path)
        request.method = method
        if let body {
            request.headers.add(name: "content-type", value: "application/json")
            request.body = .bytes(ByteBuffer(data: try JSONEncoder().encode(body)))
        }
        let started = ContinuousClock.now
        let response = try await http.execute(request, timeout: timeout)
        let buffer = try await response.body.collect(upTo: 4 * 1024 * 1024)
        let elapsed = started.duration(to: .now).milliseconds
        let text = String(buffer: buffer)
        let status = response.status.code
        guard status >= 200, status < 300 else {
            return Reply(value: nil, status: status, latencyMS: elapsed, body: text)
        }
        let decoded = T.self == Empty.self
            ? (Empty() as! T)
            : try JSONDecoder.sim.decode(T.self, from: Data(buffer: buffer))
        return Reply(value: decoded, status: status, latencyMS: elapsed, body: text)
    }

    struct Empty: Codable, Sendable {}

    // MARK: - Calls

    func health() async throws -> Bool {
        var request = HTTPClientRequest(url: baseURLs[0] + "/health")
        request.method = .GET
        let response = try await http.execute(request, timeout: .seconds(2))
        _ = try await response.body.collect(upTo: 1024)
        return response.status == .ok
    }

    /// Resets every node, not just one.
    ///
    /// Truncating the tables once would be enough for the database, but each node also
    /// caches the sku-to-launch mapping. A node that missed the reset would keep serving
    /// the id of a launch that no longer exists, and since experiments reuse skus across
    /// resets that shows up as a run quietly measuring nothing.
    func reset() async throws {
        for url in baseURLs {
            var request = HTTPClientRequest(url: url + "/admin/reset")
            request.method = .POST
            let response = try await http.execute(request, timeout: timeout)
            _ = try await response.body.collect(upTo: 1024)
        }
    }

    func createLaunch(_ body: CreateLaunchRequest) async throws -> LaunchInfo {
        let reply = try await send(.POST, "/launches", body: body, salt: 0, decode: LaunchInfo.self)
        guard let value = reply.value else { throw ClientError(status: reply.status, body: reply.body) }
        return value
    }

    func inventory(sku: String, assertInvariant: Bool = false) async throws -> InventoryInfo {
        let path = "/launches/\(sku)/" + (assertInvariant ? "invariant" : "inventory")
        let reply = try await send(.GET, path, body: Optional<Empty>.none, salt: 0, decode: InventoryInfo.self)
        guard let value = reply.value else { throw ClientError(status: reply.status, body: reply.body) }
        return value
    }

    func joinQueue(sku: String, customerID: String, salt: UInt64) async throws -> Reply<QueueStatus> {
        try await send(
            .POST, "/launches/\(sku)/queue", body: ["customerID": customerID],
            salt: salt, decode: QueueStatus.self
        )
    }

    func queueStatus(sku: String, customerID: String, salt: UInt64) async throws -> Reply<QueueStatus> {
        try await send(
            .GET, "/launches/\(sku)/queue/\(customerID)", body: Optional<Empty>.none,
            salt: salt, decode: QueueStatus.self
        )
    }

    func reserve(sku: String, customerID: String, units: Int = 1, salt: UInt64) async throws -> Reply<ReserveResult> {
        try await send(
            .POST, "/launches/\(sku)/reserve",
            body: ReserveRequest(customerID: customerID, units: units),
            salt: salt, decode: ReserveResult.self
        )
    }

    func checkout(
        sku: String, customerID: String, units: Int = 1, idempotencyKey: String, salt: UInt64
    ) async throws -> Reply<CheckoutResult> {
        try await send(
            .POST, "/launches/\(sku)/checkout",
            body: CheckoutRequestBody(customerID: customerID, units: units, idempotencyKey: idempotencyKey),
            salt: salt, decode: CheckoutResult.self
        )
    }

    func cancel(reservationID: UUID, salt: UInt64) async throws -> UInt {
        try await send(
            .DELETE, "/reservations/\(reservationID.uuidString)", body: Optional<Empty>.none,
            salt: salt, decode: Empty.self
        ).status
    }

    func admissionTick(sku: String) async throws -> Reply<ControlTrace> {
        try await send(
            .POST, "/launches/\(sku)/admission/tick", body: Optional<Empty>.none,
            salt: 0, decode: ControlTrace.self
        )
    }

    func admissionStatus(sku: String) async throws -> AdmissionStatus? {
        try await send(
            .GET, "/launches/\(sku)/admission", body: Optional<Empty>.none,
            salt: 0, decode: AdmissionStatus.self
        ).value
    }

    func controlTrace(sku: String) async throws -> [ControlSample] {
        let reply = try await send(
            .GET, "/launches/\(sku)/admission/trace", body: Optional<Empty>.none,
            salt: 0, decode: ControlTrace.self
        )
        return reply.value?.samples ?? []
    }

    func fairness(sku: String) async throws -> FairnessEnvelope? {
        try await send(
            .GET, "/launches/\(sku)/fairness", body: Optional<Empty>.none,
            salt: 0, decode: FairnessEnvelope.self
        ).value
    }

    func retryAudit(sku: String) async throws -> RetryAudit? {
        try await send(
            .GET, "/launches/\(sku)/retry-audit", body: Optional<Empty>.none,
            salt: 0, decode: RetryAudit.self
        ).value
    }

    func sweep(sku: String) async throws -> SweepInfo? {
        try await send(
            .POST, "/launches/\(sku)/sweep", body: Optional<Empty>.none,
            salt: 0, decode: SweepInfo.self
        ).value
    }

    func recover(sku: String, nodeIndex: Int = 0) async throws -> RecoveryInfo? {
        try await send(
            .POST, "/launches/\(sku)/recover", body: Optional<Empty>.none,
            salt: UInt64(nodeIndex), decode: RecoveryInfo.self
        ).value
    }

    func nodeStatus(index: Int) async throws -> NodeStatus? {
        try await send(
            .GET, "/node", body: Optional<Empty>.none, salt: UInt64(index), decode: NodeStatus.self
        ).value
    }
}

extension JSONDecoder {
    static let sim: JSONDecoder = {
        let d = JSONDecoder()
        d.dateDecodingStrategy = .iso8601
        return d
    }()
}

// MARK: - Wire types

struct CreateLaunchRequest: Encodable, Sendable {
    var sku: String
    var totalUnits: Int
    var shardCount: Int
    var collapseThreshold: Int
    var reservationTTLSeconds: Double
    var admission: AdmissionRequest?
}

struct AdmissionRequest: Encodable, Sendable {
    var kind: String
    var policy: String
    var intervalMS: Double
    var initialRate: Double
    var minRate: Double
    var maxRate: Double
    var increaseStep: Double
    var decreaseFactor: Double
    var latencyThresholdMS: Double
    var inFlightThreshold: Double
    var errorRateThreshold: Double
    var deadBand: Double
    var hardInFlightCeiling: Int
    var jitterReleases: Bool
    var commitHorizonMS: Double
}

struct ReserveRequest: Encodable, Sendable {
    var customerID: String
    var units: Int
}

struct CheckoutRequestBody: Encodable, Sendable {
    var customerID: String
    var units: Int
    var idempotencyKey: String
}

struct LaunchInfo: Decodable, Sendable {
    var id: UUID
    var sku: String
    var totalUnits: Int
    var shardCount: Int
    var collapseThreshold: Int
    var collapsed: Bool
    var requiresAdmission: Bool
}

struct ShardInfo: Decodable, Sendable {
    var shardID: Int
    var available: Int
    var reserved: Int
    var sold: Int
}

struct InventoryInfo: Decodable, Sendable {
    var launchID: UUID
    var totalUnits: Int
    var available: Int
    var reserved: Int
    var sold: Int
    var collapsed: Bool
    var accountedFor: Int
    var invariantHolds: Bool
    var nonEmptyShards: Int
    var shards: [ShardInfo]
}

struct QueueStatus: Decodable, Sendable {
    var customerID: String
    var arrivalSeq: Int64
    var position: Int
    var state: String
    var admitted: Bool
    var retries: Int
    var waitSeconds: Double
    var wasAlreadyQueued: Bool
}

struct ReserveResult: Decodable, Sendable {
    var reservationID: UUID?
    var shardID: Int?
    var probes: Int
    var succeeded: Bool
}

struct CheckoutResult: Decodable, Sendable {
    var outcome: String
    var orderID: UUID?
    var reservationID: UUID?
    var units: Int
    var probes: Int
    var replayed: Bool
}

struct ControlTrace: Decodable, Sendable {
    var samples: [ControlSample]
}

struct AdmissionStatus: Decodable, Sendable {
    var kind: String
    var policy: String
    var rate: Double
    var totalAdmitted: Int
    var queueDepth: Int
    var meanQueueDepth: Double
    var health: HealthSnapshot
}

struct FairnessEnvelope: Codable, Sendable {
    var report: FairnessReport
    var admitMeanMS: Double
    var admitP99MS: Double
    var admitBatches: Int
}

struct RetryAudit: Codable, Sendable {
    var retried: Int
    var maxRetries: Int
    var fifoInversions: Int
    var admissionsChecked: Int
    var retriesThatMovedPosition: Int
}

struct SweepInfo: Codable, Sendable {
    var reclaimed: Int
    var bucketsVisited: Int
    var durationMS: Double
}

struct RecoveryInfo: Decodable, Sendable {
    var reservationsReclaimed: Int
    var claimsReleased: Int
}

struct NodeStatus: Decodable, Sendable {
    var nodeID: String
    var nodeIndex: Int
    var nodeCount: Int
    var multiNodeMode: String
    var ownedShards: [Int]
    var liveNodes: [String]
    var redisEnabled: Bool
    var sweepStrategy: String
    var maxProbes: Int
}
