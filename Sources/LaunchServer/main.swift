import Foundation
import LaunchCore
import Logging
import Vapor

/// One allocator node.
///
/// The background work a node owns — the Postgres client's connection management, the
/// expiry sweeper, node heartbeats and the admission control loop — all runs in tasks
/// started here and cancelled on shutdown. Nothing is started lazily on the request
/// path, because a launch spike is the worst possible moment to be initializing.
@main
struct Entrypoint {
    static func main() async throws {
        var env = try Environment.detect()
        try LoggingSystem.bootstrap(from: &env)
        let app = try await Application.make(env)

        do {
            try await configure(app)
            try await app.execute()
        } catch {
            app.logger.report(error: error)
            try? await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }
}

func configure(_ app: Application) async throws {
    let node = Node(application: app)
    app.node = node

    let processEnv = ProcessInfo.processInfo.environment
    app.http.server.configuration.hostname = processEnv["HOST"] ?? "0.0.0.0"
    app.http.server.configuration.port = Int(processEnv["PORT"] ?? "8080") ?? 8080
    // A launch means a very large number of short-lived connections. The default
    // backlog is the first thing that drops traffic under a spike, and a dropped
    // connection shows up in the results as a client error rather than as the
    // server-side queueing it actually is.
    app.http.server.configuration.backlog = Int(processEnv["BACKLOG"] ?? "2048") ?? 2048
    app.http.server.configuration.reuseAddress = true
    app.routes.defaultMaxBodySize = "64kb"

    // The Postgres client needs a task for the process lifetime; it manages the pool
    // and reconnects from inside it.
    let storeTask = Task { await node.store.run() }

    // Give the pool a moment to come up before issuing the migration, and retry: in
    // Compose the server and the database start together and Postgres is not ready yet.
    try await withRetry(attempts: 30, delay: .milliseconds(500), logger: node.logger) {
        try await node.store.migrate()
    }
    node.logger.info("schema ready", metadata: ["node": .string(node.nodeID)])

    let sweeperTask = Task { await node.sweeper.run() }
    // Verifies the inventory invariant on a timer and publishes the result, so
    // "oversold units" on the dashboard is something the running system checked rather
    // than something a test asserted once.
    let selfCheckTask = Task { await node.selfCheck.run() }
    let heartbeatTask = Task {
        while !Task.isCancelled {
            await node.coordination.heartbeat(nodeID: node.nodeID)
            try? await Task.sleep(for: .seconds(2))
        }
    }

    app.lifecycle.use(
        BackgroundWork(
            tasks: [storeTask, sweeperTask, heartbeatTask, selfCheckTask], node: node
        )
    )

    try routes(app)
}

/// Cancels the node's background tasks on shutdown, so a stopping node stops
/// heartbeating promptly and the other nodes can reclaim its shards.
struct BackgroundWork: LifecycleHandler {
    let tasks: [Task<Void, Never>]
    let node: Node

    func shutdownAsync(_ application: Application) async {
        await node.controllers.stopAll()
        for task in tasks { task.cancel() }
        await node.coordination.shutdown()
    }
}

func withRetry(
    attempts: Int,
    delay: Duration,
    logger: Logger,
    _ body: () async throws -> Void
) async throws {
    var lastError: Error?
    for attempt in 1...attempts {
        do {
            try await body()
            return
        } catch {
            lastError = error
            if attempt == attempts { break }
            logger.debug(
                "waiting for dependency",
                metadata: ["attempt": .stringConvertible(attempt), "error": .string("\(error)")]
            )
            try? await Task.sleep(for: delay)
        }
    }
    throw lastError ?? Abort(.internalServerError, reason: "retry failed with no error")
}
