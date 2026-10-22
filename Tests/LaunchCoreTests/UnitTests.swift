import Foundation
import Logging
import Testing

@testable import LaunchCore

// MARK: - Determinism

@Suite("Seeded randomness")
struct RandomTests {
    @Test("the same seed replays the same stream")
    func sameSeedSameStream() {
        var a = SplitMix64(seed: 42)
        var b = SplitMix64(seed: 42)
        for _ in 0..<1_000 {
            #expect(a.next() == b.next())
        }
    }

    @Test("different seeds diverge")
    func differentSeedsDiverge() {
        var a = SplitMix64(seed: 1)
        var b = SplitMix64(seed: 2)
        let left = (0..<100).map { _ in a.next() }
        let right = (0..<100).map { _ in b.next() }
        #expect(left != right)
    }

    @Test("uniform stays in range")
    func uniformInRange() {
        var rng = SplitMix64(seed: 7)
        for _ in 0..<10_000 {
            let u = rng.uniform()
            #expect(u >= 0)
            #expect(u < 1)
        }
    }

    @Test("split produces independent but reproducible streams")
    func splitIsReproducible() {
        var root1 = SplitMix64(seed: 99)
        var root2 = SplitMix64(seed: 99)
        var childA = root1.split()
        var childB = root2.split()
        #expect(childA.next() == childB.next())
    }

    /// Swift's own `Hashable` is seeded per process, so anything that has to agree
    /// across runs or across nodes cannot use it. Shard selection is one of those.
    @Test("stable hash does not vary")
    func stableHashIsStable() {
        #expect(stableHash("c000001") == stableHash("c000001"))
        #expect(stableHash("c000001") != stableHash("c000002"))
    }
}

// MARK: - Probe order

@Suite("Shard probe order")
struct ProbeOrderTests {
    @Test("a single shard is probed once")
    func singleShard() {
        #expect(Allocator.probeOrder(customerID: "a", shardCount: 1, limit: 4) == [0])
    }

    @Test("probe order is within range and has no repeats", arguments: [2, 4, 7, 16, 64, 97])
    func noRepeats(shardCount: Int) {
        for i in 0..<200 {
            let order = Allocator.probeOrder(
                customerID: "customer-\(i)", shardCount: shardCount, limit: shardCount
            )
            #expect(order.count == shardCount)
            #expect(Set(order).count == shardCount, "repeat in \(order) for \(shardCount) shards")
            #expect(order.allSatisfy { $0 >= 0 && $0 < shardCount })
        }
    }

    /// A bounded sweep must not waste a probe on a shard it has already tried.
    @Test("a limited sweep still has no repeats")
    func limitedNoRepeats() {
        for i in 0..<200 {
            let order = Allocator.probeOrder(customerID: "c\(i)", shardCount: 64, limit: 4)
            #expect(order.count == 4)
            #expect(Set(order).count == 4)
        }
    }

    /// A retrying customer must probe the same shards, or a retry could succeed where
    /// the first attempt failed purely by landing somewhere else.
    @Test("the same customer always probes the same shards")
    func deterministicPerCustomer() {
        let first = Allocator.probeOrder(customerID: "steady", shardCount: 32, limit: 5)
        let second = Allocator.probeOrder(customerID: "steady", shardCount: 32, limit: 5)
        #expect(first == second)
    }

    /// If every customer started at shard 0 the sharding would be decorative.
    @Test("start positions spread across shards")
    func startsAreSpread() {
        let starts = (0..<2_000).map {
            Allocator.probeOrder(customerID: "c\($0)", shardCount: 16, limit: 1)[0]
        }
        let distinct = Set(starts)
        #expect(distinct.count == 16, "only reached \(distinct.count) of 16 shards")

        // And roughly evenly: no shard should take more than twice its fair share.
        var counts = [Int](repeating: 0, count: 16)
        for s in starts { counts[s] += 1 }
        let fairShare = starts.count / 16
        #expect(counts.allSatisfy { $0 < fairShare * 2 }, "lopsided spread: \(counts)")
    }

    @Test("gcd helper")
    func gcdWorks() {
        #expect(gcd(12, 18) == 6)
        #expect(gcd(7, 13) == 1)
        #expect(gcd(0, 5) == 5)
    }
}

// MARK: - Fairness measurement

@Suite("Fairness measurement")
struct FairnessMathTests {
    @Test("perfect FIFO is 1.0")
    func perfectFIFO() {
        let arrival = (1...100).map(Double.init)
        #expect(abs(spearman(arrival, arrival) - 1.0) < 1e-9)
    }

    @Test("exact reversal is -1.0")
    func reversed() {
        let arrival = (1...100).map(Double.init)
        let admission = arrival.reversed().map { $0 }
        #expect(abs(spearman(arrival, admission) + 1.0) < 1e-9)
    }

    /// A lottery should land near zero. Checked over many seeds rather than one, so the
    /// test is not just asserting that one particular shuffle happened to be balanced.
    @Test("random admission is near zero")
    func randomIsNearZero() {
        var worst = 0.0
        for seed in 0..<50 {
            var rng = SplitMix64(seed: UInt64(seed))
            let arrival = (1...500).map(Double.init)
            var admission = arrival
            admission.shuffle(using: &rng)
            worst = max(worst, abs(spearman(arrival, admission)))
        }
        #expect(worst < 0.2, "a shuffle correlated at \(worst)")
    }

    /// Bucketed FIFO produces ties by construction, which is exactly the case the
    /// 1 - 6Σd²/n(n²-1) shortcut gets wrong. This checks the tie-aware form is used.
    @Test("ties are handled")
    func tiesHandled() {
        // Two buckets: everything in the first is admitted before everything in the
        // second, but order within a bucket is arbitrary.
        let arrival: [Double] = [1, 1, 1, 1, 2, 2, 2, 2]
        let admission: [Double] = [3, 1, 4, 2, 7, 5, 8, 6]
        let r = spearman(arrival, admission)
        #expect(r > 0.8, "bucketed ordering scored only \(r)")
        #expect(r <= 1.0)
    }

    @Test("average ranks")
    func averageRanks_() {
        #expect(averageRanks([10, 20, 30]) == [1, 2, 3])
        // Two tied values share the average of the ranks they span.
        #expect(averageRanks([10, 10, 30]) == [1.5, 1.5, 3])
    }

    @Test("degenerate input does not crash")
    func degenerate() {
        #expect(spearman([], []) == 0)
        #expect(spearman([1], [1]) == 0)
        // No variance in one series means correlation is undefined; report 0, not NaN.
        #expect(spearman([1, 1, 1], [1, 2, 3]) == 0)
    }
}

// MARK: - Statistics

@Suite("Statistics")
struct StatsTests {
    @Test("quantiles of a known series")
    func quantiles() {
        let s = Samples((1...100).map(Double.init))
        #expect(abs(s.p50 - 50.5) < 0.01)
        #expect(abs(s.p99 - 99.01) < 0.1)
        #expect(s.max == 100)
        #expect(abs(s.mean - 50.5) < 0.01)
    }

    @Test("empty samples are zero, not a crash")
    func empty() {
        let s = Samples()
        #expect(s.p50 == 0)
        #expect(s.p99 == 0)
        #expect(s.mean == 0)
    }

    @Test("single sample")
    func single() {
        let s = Samples([42])
        #expect(s.p50 == 42)
        #expect(s.p99 == 42)
    }

    @Test("histogram buckets are cumulative")
    func histogramCumulative() {
        var h = Histogram(bounds: [1, 10, 100])
        for v in [0.5, 5, 5, 50, 500] { h.observe(v) }
        let c = h.cumulative
        #expect(c[0].count == 1)  // <= 1
        #expect(c[1].count == 3)  // <= 10
        #expect(c[2].count == 4)  // <= 100
        #expect(c[3].count == 5)  // +Inf picks up the overflow
        #expect(h.count == 5)
    }

    @Test("histogram quantile stays inside its bucket")
    func histogramQuantile() {
        var h = Histogram(bounds: [10, 20, 30])
        for _ in 0..<100 { h.observe(15) }
        let q = h.quantile(0.5)
        #expect(q > 10)
        #expect(q <= 20)
    }
}

// MARK: - AIMD control law

@Suite("AIMD control law")
struct ControllerTests {
    private func controller(_ config: AdmissionConfig) -> AdmissionController {
        // The control law is pure: it reads a health snapshot and the current rate and
        // returns the next rate. It needs no database, so it is tested directly rather
        // than inferred from a run.
        AdmissionController(
            config: config,
            health: HealthMonitor(),
            room: WaitingRoom(store: unusableStore(), logger: silentLogger),
            launchID: UUID(),
            sku: "test",
            logger: silentLogger
        )
    }

    private func snapshot(
        inFlight: Double = 0, latency: Double = 0, errorRate: Double = 0
    ) -> HealthSnapshot {
        HealthSnapshot(
            inFlight: Int(inFlight), smoothedInFlight: inFlight, latencyP99MS: latency,
            smoothedLatencyMS: latency, errorRate: errorRate, completed: 0, failed: 0,
            peakInFlight: Int(inFlight)
        )
    }

    @Test("a healthy system increases additively")
    func increases() async {
        let c = controller(
            AdmissionConfig(kind: .aimd, initialRate: 100, increaseStep: 25, inFlightThreshold: 100)
        )
        let (rate, action) = await c.nextRate(snapshot(inFlight: 10, latency: 20))
        #expect(action == .increase)
        #expect(rate == 125)
    }

    @Test("pressure decreases multiplicatively")
    func decreases() async {
        let c = controller(
            AdmissionConfig(
                kind: .aimd, initialRate: 100, decreaseFactor: 0.5, inFlightThreshold: 100
            )
        )
        let (rate, action) = await c.nextRate(snapshot(inFlight: 500, latency: 20))
        #expect(action == .decrease)
        #expect(rate == 50)
    }

    /// The dead band is what stops the admission graph sawing. Inside it the rate is
    /// left alone even though the signal is moving.
    @Test("the dead band holds the rate steady")
    func deadBand() async {
        let c = controller(
            AdmissionConfig(
                kind: .aimd, initialRate: 100, inFlightThreshold: 100, deadBand: 0.15
            )
        )
        for inFlight in [90.0, 100.0, 110.0] {
            let (rate, action) = await c.nextRate(snapshot(inFlight: inFlight, latency: 0))
            #expect(action == .hold, "in-flight \(inFlight) should sit in the dead band")
            #expect(rate == 100)
        }
    }

    @Test("errors force a back-off even when latency looks fine")
    func errorsBackOff() async {
        let c = controller(
            AdmissionConfig(
                kind: .aimd, initialRate: 200, decreaseFactor: 0.5,
                inFlightThreshold: 1_000, errorRateThreshold: 0.02
            )
        )
        let (rate, action) = await c.nextRate(snapshot(inFlight: 1, latency: 1, errorRate: 0.10))
        #expect(action == .decrease)
        #expect(rate == 100)
    }

    @Test("the rate never falls below the floor or climbs past the ceiling")
    func bounds() async {
        let low = controller(
            AdmissionConfig(
                kind: .aimd, initialRate: 6, minRate: 5, decreaseFactor: 0.5,
                inFlightThreshold: 10
            )
        )
        let (floored, _) = await low.nextRate(snapshot(inFlight: 1_000))
        #expect(floored == 5)

        let high = controller(
            AdmissionConfig(
                kind: .aimd, initialRate: 990, maxRate: 1_000, increaseStep: 25,
                inFlightThreshold: 1_000
            )
        )
        let (capped, _) = await high.nextRate(snapshot(inFlight: 1))
        #expect(capped == 1_000)
    }

    /// The fixed admitter is the baseline, and it has to actually be fixed: whatever
    /// the health signal says, the rate does not move.
    @Test("the fixed admitter ignores health entirely")
    func fixedIsFixed() async {
        let c = controller(AdmissionConfig(kind: .fixed, initialRate: 75))
        for inFlight in [0.0, 50.0, 5_000.0] {
            let (rate, action) = await c.nextRate(
                snapshot(inFlight: inFlight, latency: inFlight * 10, errorRate: 0.5)
            )
            #expect(rate == 75)
            #expect(action == .hold)
        }
    }

    /// Controlling on the faster-moving signal is the point; whichever of the two is
    /// under more pressure should drive the decision.
    @Test("latency alone can trigger a back-off")
    func latencyDrivesToo() async {
        let c = controller(
            AdmissionConfig(
                kind: .aimd, initialRate: 100, decreaseFactor: 0.5,
                latencyThresholdMS: 250, inFlightThreshold: 100_000
            )
        )
        let (rate, action) = await c.nextRate(snapshot(inFlight: 1, latency: 900))
        #expect(action == .decrease)
        #expect(rate == 50)
    }
}

// MARK: - Health monitor

@Suite("Health monitor")
struct HealthTests {
    @Test("in-flight count rises and falls")
    func inFlight() async {
        let h = HealthMonitor()
        await h.checkoutStarted()
        await h.checkoutStarted()
        #expect(await h.inFlight == 2)
        await h.checkoutFinished(latencyMS: 10, failed: false)
        #expect(await h.inFlight == 1)
        #expect(await h.peakInFlight == 2)
    }

    @Test("in-flight never goes negative")
    func noNegative() async {
        let h = HealthMonitor()
        await h.checkoutFinished(latencyMS: 10, failed: false)
        #expect(await h.inFlight == 0)
    }

    @Test("error rate is measured over the window")
    func errorRate() async {
        let h = HealthMonitor()
        for i in 0..<10 {
            await h.checkoutStarted()
            await h.checkoutFinished(latencyMS: 5, failed: i < 3)
        }
        #expect(abs(await h.errorRate() - 0.3) < 1e-9)
        #expect(await h.failed == 3)
        #expect(await h.completed == 7)
    }

    /// The smoothed signal must lag a step change rather than jumping to it; that lag
    /// is what the dead band and the EWMA are for.
    @Test("the EWMA lags a step change")
    func ewmaLags() async {
        let h = HealthMonitor(alpha: 0.2)
        await h.checkoutStarted()
        await h.checkoutFinished(latencyMS: 10, failed: false)
        await h.checkoutStarted()
        await h.checkoutFinished(latencyMS: 1_000, failed: false)
        let smoothed = await h.smoothedLatencyMS
        #expect(smoothed > 10)
        #expect(smoothed < 1_000, "the EWMA jumped straight to the new value")
    }
}

// MARK: - Model invariants

@Suite("Inventory model")
struct InventoryTests {
    @Test("the invariant holds for a clean split")
    func cleanSplit() {
        let state = InventoryState(
            launchID: UUID(),
            totalUnits: 100,
            shards: (0..<4).map { InventoryShard(shardID: $0, available: 25, reserved: 0, sold: 0) },
            collapsed: false
        )
        #expect(state.holds)
        #expect(state.available == 100)
        #expect(state.nonEmptyShards == 4)
    }

    @Test("units moving between states keeps the total")
    func movementPreservesTotal() {
        let state = InventoryState(
            launchID: UUID(),
            totalUnits: 10,
            shards: [
                InventoryShard(shardID: 0, available: 3, reserved: 2, sold: 1),
                InventoryShard(shardID: 1, available: 0, reserved: 1, sold: 3),
            ],
            collapsed: false
        )
        #expect(state.holds)
        #expect(state.available == 3)
        #expect(state.reserved == 3)
        #expect(state.sold == 4)
    }

    @Test("a missing unit is detected")
    func detectsLoss() {
        let state = InventoryState(
            launchID: UUID(),
            totalUnits: 10,
            shards: [InventoryShard(shardID: 0, available: 4, reserved: 0, sold: 5)],
            collapsed: false
        )
        #expect(!state.holds)
        #expect(state.accountedFor == 9)
    }

    /// Fragmentation is the gap between shards that exist and shards with stock.
    @Test("non-empty shards count tracks fragmentation")
    func fragmentation() {
        let state = InventoryState(
            launchID: UUID(),
            totalUnits: 64,
            shards: (0..<64).map {
                InventoryShard(shardID: $0, available: $0 < 5 ? 1 : 0, reserved: 0, sold: $0 < 5 ? 0 : 1)
            },
            collapsed: false
        )
        #expect(state.nonEmptyShards == 5)
        #expect(state.holds)
    }
}

// MARK: - Durations

@Suite("Duration conversion")
struct DurationTests {
    @Test("milliseconds and seconds round-trip")
    func conversions() {
        #expect(Duration.seconds(2).milliseconds == 2_000)
        #expect(Duration.milliseconds(250).seconds == 0.25)
        #expect(Duration.milliseconds(1).milliseconds == 1)
    }
}

// MARK: - Metrics exposition

@Suite("Prometheus exposition")
struct MetricsTests {
    @Test("counters, gauges and histograms all render")
    func renders() async {
        let registry = MetricsRegistry()
        await registry.increment("things_total", by: 3, labels: ["sku": "x"], help: "things")
        await registry.set("depth", to: 7, labels: ["sku": "x"])
        await registry.observe("latency_ms", 15, labels: ["sku": "x"])
        let text = await registry.render()

        #expect(text.contains("# TYPE things_total counter"))
        #expect(text.contains("things_total{sku=\"x\"} 3.0"))
        #expect(text.contains("# TYPE depth gauge"))
        #expect(text.contains("depth{sku=\"x\"} 7.0"))
        #expect(text.contains("# TYPE latency_ms histogram"))
        #expect(text.contains("latency_ms_count{sku=\"x\"} 1"))
        #expect(text.contains("le=\"+Inf\""))
    }

    @Test("labels render in a stable order")
    func stableLabelOrder() async {
        let registry = MetricsRegistry()
        await registry.increment("x_total", labels: ["b": "2", "a": "1"])
        let text = await registry.render()
        #expect(text.contains("x_total{a=\"1\",b=\"2\"}"))
    }

    @Test("a series with no labels renders without braces")
    func noLabels() async {
        let registry = MetricsRegistry()
        await registry.increment("plain_total")
        let text = await registry.render()
        #expect(text.contains("plain_total 1.0"))
    }

    @Test("counters accumulate per label set")
    func perLabel() async {
        let registry = MetricsRegistry()
        await registry.increment("hits_total", labels: ["node": "a"])
        await registry.increment("hits_total", labels: ["node": "a"])
        await registry.increment("hits_total", labels: ["node": "b"])
        #expect(await registry.counterValue("hits_total", labels: ["node": "a"]) == 2)
        #expect(await registry.counterValue("hits_total", labels: ["node": "b"]) == 1)
    }
}

// MARK: - Fairness report

@Suite("Fairness report")
struct FairnessReportTests {
    private func record(_ arrival: Int64, _ admission: Int64, wait: Double) -> AdmissionRecord {
        let now = Date()
        return AdmissionRecord(
            customerID: "c\(arrival)", arrivalSeq: arrival, admissionSeq: admission,
            arrivalAt: now, admittedAt: now.addingTimeInterval(wait), waitSeconds: wait
        )
    }

    @Test("a FIFO run reports a correlation of 1")
    func fifoReport() {
        let records = (1...50).map { record(Int64($0), Int64($0), wait: Double($0) / 10) }
        let report = FairnessReport(
            policy: .strictFIFO, admissions: records, meanQueueDepth: 100, admissionRate: 50
        )
        #expect(abs(report.rankCorrelation - 1.0) < 1e-9)
        #expect(report.admitted == 50)
        // Little's Law: 100 waiting at 50 admitted per second predicts a two second wait.
        #expect(abs(report.predictedMeanWait - 2.0) < 1e-9)
    }

    @Test("a zero admission rate does not divide by zero")
    func zeroRate() {
        let report = FairnessReport(
            policy: .lottery, admissions: [], meanQueueDepth: 10, admissionRate: 0
        )
        #expect(report.predictedMeanWait == 0)
        #expect(report.rankCorrelation == 0)
    }
}

// MARK: - Policy SQL

@Suite("Fairness policy ordering")
struct PolicySQLTests {
    /// The ORDER BY is the entire implementation of each policy, so it is worth pinning:
    /// a change here silently changes what "fair" means.
    @Test("each policy orders as documented")
    func orderings() {
        #expect(FairnessPolicy.strictFIFO.orderBy == "seq")
        #expect(FairnessPolicy.lottery.orderBy == "random()")
        #expect(FairnessPolicy.bucketedFIFO.orderBy == "bucket, random()")
    }

    @Test("all three policies are covered")
    func allCases() {
        #expect(FairnessPolicy.allCases.count == 3)
    }
}

// MARK: - Test helpers

let silentLogger: Logger = {
    var l = Logger(label: "test")
    l.logLevel = .critical
    return l
}()

/// A store pointed at a port nothing listens on.
///
/// Used only where a component needs a `Store` to be constructed but the test never
/// touches the database — the control law, for instance. Any test that actually queries
/// lives in the integration suite, which skips itself when no database is configured.
func unusableStore() -> Store {
    Store(config: StoreConfig(host: "127.0.0.1", port: 1, maxConnections: 1), logger: silentLogger)
}
