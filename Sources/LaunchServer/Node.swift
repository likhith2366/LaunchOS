import Foundation
import LaunchCore
import Logging
import Vapor

/// Everything one allocator node owns. Several of these run at once under Docker
/// Compose; they share Postgres and Redis and nothing else.
final class Node: Sendable {
    let nodeID: String
    let store: Store
    let coordination: Coordination
    let allocator: Allocator
    let room: WaitingRoom
    let health: HealthMonitor
    let positions: PositionEstimator
    let checkout: CheckoutCoordinator
    let sweeper: Sweeper
    let recovery: CrashRecovery
    let leases: LeaseManager
    let selfCheck: SelfCheck
    let sweepStrategy: SweepStrategy
    let logger: Logger
    let multiNodeMode: MultiNodeMode
    let nodeIndex: Int
    let nodeCount: Int
    /// Admission controllers, one per launch, created on demand.
    let controllers: ControllerRegistry
    let directory: LaunchDirectory

    init(application app: Application) {
        let env = ProcessInfo.processInfo.environment
        self.nodeID = env["NODE_ID"] ?? "node-\(UUID().uuidString.prefix(8))"
        self.nodeIndex = Int(env["NODE_INDEX"] ?? "0") ?? 0
        self.nodeCount = Int(env["NODE_COUNT"] ?? "1") ?? 1
        self.multiNodeMode = MultiNodeMode(rawValue: env["MULTI_NODE_MODE"] ?? "") ?? .postgresArbitrated

        var logger = Logger(label: "launchos.\(nodeID)")
        logger.logLevel = Logger.Level(rawValue: env["LOG_LEVEL"] ?? "info") ?? .info
        self.logger = logger

        self.store = Store(config: .fromEnvironment(), logger: logger)
        self.coordination = Coordination(
            config: .fromEnvironment(), logger: logger,
            eventLoopGroup: app.eventLoopGroup
        )
        let allocatorConfig = AllocatorConfig(
            maxProbes: Int(env["MAX_PROBES"] ?? "4") ?? 4,
            collapseEnabled: env["COLLAPSE_ENABLED"] != "0"
        )
        self.allocator = Allocator(
            store: store, nodeID: nodeID, config: allocatorConfig, logger: logger
        )
        self.room = WaitingRoom(
            store: store, logger: logger,
            bucketWidth: Double(env["QUEUE_BUCKET_SECONDS"] ?? "1") ?? 1
        )
        self.health = HealthMonitor()
        self.positions = PositionEstimator(
            room: room, coordination: coordination, logger: logger
        )
        self.checkout = CheckoutCoordinator(
            store: store, allocator: allocator, room: room, health: health,
            coordination: coordination, nodeID: nodeID, logger: logger,
            requireAdmission: env["REQUIRE_ADMISSION"] != "0"
        )
        let sweepStrategy = SweepStrategy(rawValue: env["SWEEP_STRATEGY"] ?? "") ?? .bucketed
        self.sweepStrategy = sweepStrategy
        self.sweeper = Sweeper(
            store: store,
            config: SweeperConfig(
                strategy: sweepStrategy,
                batchSize: Int(env["SWEEP_BATCH"] ?? "500") ?? 500
            ),
            logger: logger
        )
        self.recovery = CrashRecovery(store: store, coordination: coordination, logger: logger)
        self.leases = LeaseManager(store: store, nodeID: nodeID, logger: logger)
        self.selfCheck = SelfCheck(store: store, logger: logger)
        self.controllers = ControllerRegistry()
        self.directory = LaunchDirectory()
    }
}

/// One admission controller per launch, started on first use.
///
/// Admission is per launch, not per node: several nodes each run a controller against
/// the same queue, and `FOR UPDATE SKIP LOCKED` in the admit statement is what keeps
/// them from handing the same customer to two nodes.
actor ControllerRegistry {
    private var controllers: [UUID: AdmissionController] = [:]
    private var tasks: [UUID: Task<Void, Never>] = [:]
    /// Set only in lease-based multi-node mode.
    private(set) var leaseHolder: LeaseHolder?
    private var leaseTask: Task<Void, Never>?

    func existing(_ launchID: UUID) -> AdmissionController? { controllers[launchID] }

    func startLeases(holder: LeaseHolder) {
        leaseTask?.cancel()
        leaseHolder = holder
        leaseTask = Task { await holder.run() }
    }

    func start(_ launchID: UUID, controller: AdmissionController) {
        guard tasks[launchID] == nil else { return }
        tasks[launchID] = Task { await controller.run() }
    }

    func stop(_ launchID: UUID) {
        tasks[launchID]?.cancel()
        tasks[launchID] = nil
    }

    func replace(_ launchID: UUID, with controller: AdmissionController) {
        stop(launchID)
        controllers[launchID] = controller
    }

    func stopAll() {
        for (_, task) in tasks { task.cancel() }
        tasks.removeAll()
        leaseTask?.cancel()
        leaseTask = nil
        leaseHolder = nil
        controllers.removeAll()
    }
}

/// Caches the sku -> launch mapping.
///
/// Every route begins by resolving a sku, so without this the most frequent request in
/// a launch pays for a `launches` lookup before it does anything useful. The mapping and
/// everything the request path reads from it are set when the launch is created and
/// never change; `collapsed` does change, but no request path decides anything from the
/// cached copy of it — the allocator keeps its own, which it refreshes when a probe
/// sweep comes up empty.
actor LaunchDirectory {
    private var bySKU: [String: Launch] = [:]

    func cached(_ sku: String) -> Launch? { bySKU[sku] }
    func store(_ launch: Launch) { bySKU[launch.sku] = launch }
    func clear() { bySKU.removeAll() }
}

extension Node {
    /// Resolves a sku, from cache when possible.
    func launch(sku: String) async throws -> Launch {
        if let cached = await directory.cached(sku) { return cached }
        let launch = try await store.launch(sku: sku)
        await directory.store(launch)
        return launch
    }
}

extension Application {
    private struct NodeKey: StorageKey {
        typealias Value = Node
    }

    var node: Node {
        get {
            guard let node = storage[NodeKey.self] else {
                fatalError("Node not configured; call configure(app) first")
            }
            return node
        }
        set { storage[NodeKey.self] = newValue }
    }
}

extension Request {
    var node: Node { application.node }
}
