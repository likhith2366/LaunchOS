import ArgumentParser
import AsyncHTTPClient
import Foundation
import LaunchCore
import NIOCore
import NIOPosix

@main
struct LaunchSim: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "launch-sim",
        abstract: "Seeded, deterministic load and fault simulator for LaunchOS.",
        discussion: """
            Every run is driven from a seed: arrival times, which customers send \
            duplicate requests, which abandon, and how long each thinks before \
            checking out. Re-running with the same seed replays the same traffic, which \
            is what makes a comparison between two admission policies meaningful and a \
            failure reproducible.
            """,
        subcommands: [
            Oversell.self, ShardSweep.self, Controller.self, Fairness.self,
            Tail.self, Idempotency.self, SweepProbe.self, Horizon.self, All.self,
        ]
    )
}

struct GlobalOptions: ParsableArguments {
    @Option(help: "Base URL of each allocator node, repeated.")
    var node: [String] = ["http://127.0.0.1:8080"]

    @Option(help: "Root seed. The same seed replays the same run.")
    var seed: UInt64 = 0xA5A5_1234

    @Option(help: "Cap on simultaneous open connections from the simulator.")
    var connections: Int = 256

    @Option(help: "Where to write results.")
    var output: String = "results"

    @Flag(help: "Print each run's outcome as it finishes.")
    var verbose: Bool = false
}

/// Runs `body` with an HTTP client sized for the run, and waits for the nodes to be up.
func withClient<T>(
    _ options: GlobalOptions,
    _ body: (LaunchClient) async throws -> T
) async throws -> T {
    var configuration = HTTPClient.Configuration()
    // Keep-alive and a generous per-host limit: without them the simulator spends its
    // time in TCP handshakes and measures its own connection setup instead of the
    // server.
    configuration.connectionPool = .init(
        idleTimeout: .seconds(60),
        concurrentHTTP1ConnectionsPerHostSoftLimit: max(options.connections, 8)
    )
    configuration.timeout = .init(connect: .seconds(10), read: .seconds(60))
    configuration.decompression = .disabled
    let http = HTTPClient(eventLoopGroupProvider: .singleton, configuration: configuration)

    do {
        let client = LaunchClient(http: http, baseURLs: options.node)
        var ready = false
        for _ in 0..<60 {
            if (try? await client.health()) == true { ready = true; break }
            try? await Task.sleep(for: .milliseconds(500))
        }
        guard ready else {
            throw ValidationError("no node answered at \(options.node.joined(separator: ", "))")
        }
        let result = try await body(client)
        try await http.shutdown()
        return result
    } catch {
        try? await http.shutdown()
        throw error
    }
}

func environmentRecord(nodes: Int, notes: String) -> Environment {
    Environment(
        recordedAt: Date(),
        host: ProcessInfo.processInfo.hostName,
        cores: ProcessInfo.processInfo.activeProcessorCount,
        nodes: nodes,
        notes: notes
    )
}

func report(_ outcome: RunOutcome) {
    print(
        String(
            format: """
                  %@
                    offered %.0f arrivals/s, peak %d concurrent connections
                    %d purchases in %.2f s (%.0f/s), p99 %.1f ms, completion %.1f%%
                    inventory: %d/%d sold, %d available, %d reserved, invariant %@
                """,
            outcome.label, outcome.arrivalRatePerSecond, outcome.peakConcurrentConnections,
            outcome.completedPurchases, outcome.wallClockSeconds, outcome.throughputPerSecond,
            outcome.checkoutP99MS, outcome.completionRate * 100,
            outcome.inventory.sold, outcome.inventory.totalUnits,
            outcome.inventory.available, outcome.inventory.reserved,
            outcome.inventory.invariantHolds ? "holds" : "VIOLATED"
        )
    )
    if !outcome.errorSamples.isEmpty {
        print("    errors: \(outcome.errorSamples.joined(separator: " | "))")
    }
}

// MARK: - Subcommands

struct Oversell: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Fire many simultaneous reservations at limited stock; assert none oversell."
    )

    @OptionGroup var options: GlobalOptions
    @Option var units: Int = 100
    @Option var contenders: Int = 1_000
    @Option var trials: Int = 5
    @Option(help: "Shard counts to test, repeated.") var shards: [Int] = [1, 16]

    func run() async throws {
        try await withClient(options) { client in
            for shardCount in shards {
                let result = try await runOversell(
                    client: client, trials: trials, units: units, contenders: contenders,
                    shardCount: shardCount, seed: options.seed, connectionLimit: options.connections
                )
                print("shards=\(shardCount): oversold=\(result.oversoldUnits) successes=\(result.successesPerTrial)")
                if options.verbose { result.runs.forEach(report) }
                guard result.oversoldUnits == 0, result.invariantHeldEveryTrial else {
                    throw ExitCode.failure
                }
            }
        }
    }
}

struct ShardSweep: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Identical load at several shard counts; find the speedup and the next ceiling."
    )

    @OptionGroup var options: GlobalOptions
    @Option var units: Int = 2_000
    @Option var contenders: Int = 4_000
    @Option(help: "Shard counts to sweep, repeated.") var shards: [Int] = [1, 4, 16, 64]

    func run() async throws {
        try await withClient(options) { client in
            let result = try await runShardSweep(
                client: client, shardCounts: shards, units: units, contenders: contenders,
                seed: options.seed, connectionLimit: options.connections
            )
            for p in result.points {
                print(
                    String(
                        format: "shards=%3d  %7.1f allocations/s  %5.2fx  p99 %6.1f ms",
                        p.shardCount, p.reservationThroughput, p.speedupVsSingleRow,
                        p.run.checkoutP99MS
                    )
                )
                if options.verbose { report(p.run) }
            }
            print("best speedup \(String(format: "%.2f", result.bestSpeedup))x, plateau at \(result.plateauAtShards.map(String.init) ?? "none")")
        }
    }
}

struct Controller: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "A/B the adaptive AIMD controller against fixed-rate admission.",
        discussion: """
            Stock is not scarce in this experiment: units defaults to the customer             count, so that inventory running out cannot be what limits completion. What             is scarce is backend capacity, which is the thing admission control exists             to protect.
            """
    )

    @OptionGroup var options: GlobalOptions
    @Option(help: "Units available. Defaults to one per customer, so stock is not the limit.")
    var units: Int = 0
    @Option var customers: Int = 4_000
    @Option var duration: Double = 15
    @Option(
        help: """
            Sustained checkouts per second this deployment manages. The controller's \
            thresholds are derived from it, so read it off scripts/probe-bottleneck.sh \
            rather than guessing.
            """
    )
    var capacity: Double = 600
    @Option(help: "The checkout p99 the controller is asked to hold, in milliseconds.")
    var targetP99: Double = 600

    func run() async throws {
        try await withClient(options) { client in
            let result = try await runControllerComparison(
                client: client, units: units > 0 ? units : customers, customers: customers,
                seed: options.seed, connectionLimit: options.connections,
                durationSeconds: duration,
                serviceCapacityPerSecond: capacity, targetP99MS: targetP99
            )
            print("burst:  \(result.verdict)")
            print("steady: \(result.steadyLoadVerdict)")
            if options.verbose {
                (result.burstArms + result.steadyArms).forEach { report($0.run) }
            }
        }
    }
}

struct Fairness: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Measure fairness as rank correlation across the three queue policies."
    )

    @OptionGroup var options: GlobalOptions
    @Option var units: Int = 1_000
    @Option var customers: Int = 4_000
    @Option var duration: Double = 12

    func run() async throws {
        try await withClient(options) { client in
            let result = try await runFairness(
                client: client, units: units, customers: customers, seed: options.seed,
                connectionLimit: options.connections, durationSeconds: duration
            )
            print(result.tradeoffSummary)
            if result.retriesEverImprovedPosition {
                print("FAIL: a retry improved a queue position")
                throw ExitCode.failure
            }
            if options.verbose { result.arms.forEach { report($0.run) } }
        }
    }
}

struct Tail: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Failure rate in the last 5% of inventory, with and without shard collapse."
    )

    @OptionGroup var options: GlobalOptions
    @Option var units: Int = 640
    // 64 shards, not 16. With a probe limit of 4, a request covers a quarter of 16
    // shards and almost never misses stock that exists — the effect being measured is
    // real but invisible at that ratio. At 64 shards a request sees one shard in
    // sixteen, which is where tail fragmentation actually bites.
    @Option var shards: Int = 64
    @Option var contenders: Int = 800
    @Option(help: "Independent drain-and-measure cycles to pool. One pass is too few requests to read a rate from.")
    var trials: Int = 8

    func run() async throws {
        try await withClient(options) { client in
            let result = try await runTailFragmentation(
                client: client, units: units, shardCount: shards, contenders: contenders,
                seed: options.seed, connectionLimit: options.connections, trials: trials
            ).result
            print(result.summary)
            if options.verbose {
                [result.withoutCollapse, result.withCollapse].compactMap { $0 }.forEach { report($0.run) }
            }
        }
    }
}

struct Idempotency: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Every customer sends its checkout twice with one key; assert no duplicate orders."
    )

    @OptionGroup var options: GlobalOptions
    @Option var units: Int = 500
    @Option var customers: Int = 800

    func run() async throws {
        try await withClient(options) { client in
            let result = try await runIdempotency(
                client: client, units: units, customers: customers,
                seed: options.seed, connectionLimit: options.connections
            )
            print(result.summary)
            if options.verbose { report(result.run) }
            guard result.duplicateOrders == 0 else { throw ExitCode.failure }
        }
    }
}

struct SweepProbe: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "sweep-probe",
        abstract: "Time the expiry sweeper against a backlog of abandoned reservations."
    )

    @OptionGroup var options: GlobalOptions
    @Option var units: Int = 5_000
    @Option var abandoners: Int = 3_000

    func run() async throws {
        try await withClient(options) { client in
            let result = try await runSweepProbe(
                client: client, units: units, abandoners: abandoners,
                seed: options.seed, connectionLimit: options.connections
            )
            print(
                String(
                    format: "backlog %d · batch mean %.1f ms, p99 %.1f ms · pool back in full after %.2f s",
                    result.expiredBacklog, result.batchMeanMS, result.batchP99MS,
                    result.secondsToFullRecovery
                )
            )
            print(result.note)
        }
    }
}

struct Horizon: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Measure FIFO inversions against the admission commit horizon."
    )

    @OptionGroup var options: GlobalOptions
    @Option var customers: Int = 2_000
    @Option(help: "Horizons to sweep, in milliseconds, repeated.")
    var horizon: [Double] = [0, 100, 250, 500, 1_000, 2_000]

    func run() async throws {
        try await withClient(options) { client in
            let result = try await runHorizonSweep(
                client: client, customers: customers, horizonsMS: horizon,
                seed: options.seed, connectionLimit: options.connections
            )
            for p in result.points {
                print(
                    String(
                        format: "horizon %5.0f ms  inversions %5d/%-5d (%.2f%%)  correlation %.5f  mean wait %.2f s",
                        p.horizonMS, p.fifoInversions, p.admissionsChecked,
                        p.inversionRate * 100, p.rankCorrelation, p.meanWaitSeconds
                    )
                )
            }
            print(result.explanation)
        }
    }
}

struct All: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Run the whole suite and write results.json and RESULTS.md."
    )

    @OptionGroup var options: GlobalOptions
    @Option(help: "Scale factor for customer counts. Lower it on a small machine.")
    var scale: Double = 1.0
    @Option(help: "Sustained checkouts/s this deployment manages, for deriving the controller's thresholds.")
    var capacity: Double = 600
    @Option(help: "The checkout p99 the controller is asked to hold, in ms.")
    var targetP99: Double = 600
    @Option(help: "Free-text note recorded with the results, e.g. the hardware.")
    var notes: String = "Local run under WSL2; Postgres and Redis on the same host."

    func run() async throws {
        try await withClient(options) { client in
            func n(_ base: Int) -> Int { max(10, Int(Double(base) * scale)) }
            var suite = ExperimentSuite(
                environment: environmentRecord(nodes: options.node.count, notes: notes)
            )

            print("== overselling ==")
            // Run the correctness trial at both ends of the sharding range: it is the
            // sharded configuration that is most likely to break the invariant.
            var oversell = try await runOversell(
                client: client, trials: 3, units: 100, contenders: n(1_000),
                shardCount: 1, seed: options.seed, connectionLimit: options.connections
            )
            let sharded = try await runOversell(
                client: client, trials: 3, units: 100, contenders: n(1_000),
                shardCount: 16, seed: options.seed, connectionLimit: options.connections
            )
            oversell.oversoldUnits += sharded.oversoldUnits
            oversell.invariantHeldEveryTrial =
                oversell.invariantHeldEveryTrial && sharded.invariantHeldEveryTrial
            oversell.successesPerTrial += sharded.successesPerTrial
            oversell.shardCountsTested += sharded.shardCountsTested
            oversell.runs += sharded.runs
            suite.oversell = oversell
            print("  oversold units: \(oversell.oversoldUnits)")

            print("== shard sweep ==")
            suite.shardSweep = try await runShardSweep(
                client: client, shardCounts: [1, 4, 16, 64], units: n(2_000),
                contenders: n(4_000), seed: options.seed, connectionLimit: options.connections
            )
            for p in suite.shardSweep!.points {
                print(String(format: "  shards=%3d %7.1f/s %5.2fx", p.shardCount, p.reservationThroughput, p.speedupVsSingleRow))
            }

            print("== admission controller ==")
            suite.controller = try await runControllerComparison(
                // One unit per customer: this arm measures admission, not scarcity.
                client: client, units: n(4_000), customers: n(4_000), seed: options.seed,
                connectionLimit: options.connections, durationSeconds: 15,
                serviceCapacityPerSecond: capacity, targetP99MS: targetP99
            )
            print("  burst:  \(suite.controller!.verdict)")
            print("  steady: \(suite.controller!.steadyLoadVerdict)")

            print("== fairness ==")
            suite.fairness = try await runFairness(
                client: client, units: n(1_000), customers: n(4_000), seed: options.seed,
                connectionLimit: options.connections, durationSeconds: 12
            )
            print("  \(suite.fairness!.tradeoffSummary)")

            print("== tail fragmentation ==")
            suite.tailFragmentation = try await runTailFragmentation(
                client: client, units: n(640), shardCount: 64, contenders: n(800),
                seed: options.seed, connectionLimit: options.connections, trials: 8
            ).result
            print("  \(suite.tailFragmentation!.summary)")

            print("== idempotency ==")
            suite.idempotency = try await runIdempotency(
                client: client, units: n(500), customers: n(800),
                seed: options.seed, connectionLimit: options.connections
            )
            print("  \(suite.idempotency!.summary)")

            print("== commit horizon ==")
            suite.commitHorizon = try await runHorizonSweep(
                client: client, customers: n(2_000), horizonsMS: [0, 100, 250, 500, 1_000],
                seed: options.seed, connectionLimit: options.connections
            )
            for p in suite.commitHorizon!.points {
                print(String(format: "  horizon %5.0f ms: %d inversions of %d", p.horizonMS, p.fifoInversions, p.admissionsChecked))
            }

            print("== expiry sweep ==")
            suite.sweepStrategy = try await runSweepProbe(
                client: client, units: n(5_000), abandoners: n(3_000),
                seed: options.seed, connectionLimit: options.connections
            )

            let writer = ReportWriter(directory: options.output)
            let (json, md) = try writer.write(suite)
            print("\nwrote \(json.path)")
            print("wrote \(md.path)")

            // The suite fails loudly on a correctness regression. A performance number
            // can be disappointing; overselling cannot be tolerated.
            if oversell.oversoldUnits > 0 || !oversell.invariantHeldEveryTrial {
                print("FAIL: inventory invariant violated")
                throw ExitCode.failure
            }
            if (suite.idempotency?.duplicateOrders ?? 0) > 0 {
                print("FAIL: duplicate orders for one idempotency key")
                throw ExitCode.failure
            }
            if suite.fairness?.retriesEverImprovedPosition == true {
                print("FAIL: a retry changed a customer's queue position")
                throw ExitCode.failure
            }
        }
    }
}
