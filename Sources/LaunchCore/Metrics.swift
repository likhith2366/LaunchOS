import Foundation

/// Fixed-bucket latency histogram. Buckets are cumulative on render, as Prometheus wants.
public struct Histogram: Sendable {
    public static let defaultBounds: [Double] = [
        1, 2, 5, 10, 20, 35, 50, 75, 100, 150, 250, 400, 600, 1_000, 2_500, 5_000, 10_000,
    ]

    public let bounds: [Double]
    private var counts: [Int]
    private var overflow: Int = 0
    public private(set) var sum: Double = 0
    public private(set) var count: Int = 0

    public init(bounds: [Double] = Histogram.defaultBounds) {
        self.bounds = bounds
        self.counts = Array(repeating: 0, count: bounds.count)
    }

    public mutating func observe(_ value: Double) {
        sum += value
        count += 1
        if let idx = bounds.firstIndex(where: { value <= $0 }) {
            counts[idx] += 1
        } else {
            overflow += 1
        }
    }

    /// Cumulative count at each bucket upper bound, plus +Inf.
    public var cumulative: [(bound: Double, count: Int)] {
        var running = 0
        var out: [(Double, Int)] = []
        for (i, b) in bounds.enumerated() {
            running += counts[i]
            out.append((b, running))
        }
        out.append((.infinity, running + overflow))
        return out
    }

    /// Bucket-interpolated quantile. Honest about its own resolution: a value in the
    /// overflow bucket reports the top bound, not a fabricated number.
    public func quantile(_ q: Double) -> Double {
        guard count > 0 else { return 0 }
        let target = Double(count) * q
        var running = 0.0
        var lower = 0.0
        for (i, b) in bounds.enumerated() {
            let c = Double(counts[i])
            if running + c >= target {
                let within = (target - running) / max(c, 1)
                return lower + (b - lower) * within
            }
            running += c
            lower = b
        }
        return bounds.last ?? 0
    }
}

/// Exact-quantile sample store for the experiment harness, where the sample count is
/// bounded and known. The server uses `Histogram`; the simulator uses this, because a
/// headline p99 should not be a bucket interpolation.
public struct Samples: Sendable {
    public private(set) var values: [Double] = []

    public init() {}
    public init(_ values: [Double]) { self.values = values }

    public mutating func add(_ v: Double) { values.append(v) }

    public var count: Int { values.count }
    public var mean: Double { values.isEmpty ? 0 : values.reduce(0, +) / Double(values.count) }

    public func quantile(_ q: Double) -> Double {
        guard !values.isEmpty else { return 0 }
        let sorted = values.sorted()
        if sorted.count == 1 { return sorted[0] }
        let pos = q * Double(sorted.count - 1)
        let lo = Int(pos.rounded(.down))
        let hi = min(lo + 1, sorted.count - 1)
        let frac = pos - Double(lo)
        return sorted[lo] * (1 - frac) + sorted[hi] * frac
    }

    public var p50: Double { quantile(0.50) }
    public var p95: Double { quantile(0.95) }
    public var p99: Double { quantile(0.99) }
    public var max: Double { values.max() ?? 0 }
}

/// Spearman rank correlation. This is the fairness definition the project commits to:
/// 1.0 is perfect FIFO, ~0.0 is a lottery.
///
/// Uses average ranks for ties and falls back to the Pearson-on-ranks form, which is
/// the correct general definition — the 1 - 6Σd²/n(n²-1) shortcut is only valid when
/// there are no ties, and bucketed FIFO produces ties by construction.
public func spearman(_ x: [Double], _ y: [Double]) -> Double {
    guard x.count == y.count, x.count > 1 else { return 0 }
    let rx = averageRanks(x)
    let ry = averageRanks(y)
    let n = Double(rx.count)
    let mx = rx.reduce(0, +) / n
    let my = ry.reduce(0, +) / n
    var num = 0.0, dx = 0.0, dy = 0.0
    for i in 0..<rx.count {
        let a = rx[i] - mx, b = ry[i] - my
        num += a * b
        dx += a * a
        dy += b * b
    }
    guard dx > 0, dy > 0 else { return 0 }
    return num / (dx * dy).squareRoot()
}

func averageRanks(_ v: [Double]) -> [Double] {
    let idx = v.indices.sorted { v[$0] < v[$1] }
    var ranks = [Double](repeating: 0, count: v.count)
    var i = 0
    while i < idx.count {
        var j = i
        while j + 1 < idx.count, v[idx[j + 1]] == v[idx[i]] { j += 1 }
        let avg = Double(i + j) / 2.0 + 1.0
        for k in i...j { ranks[idx[k]] = avg }
        i = j + 1
    }
    return ranks
}

/// The server-side metric set. One actor, rendered on /metrics in Prometheus text
/// format. Hand-rolled rather than pulled from a client library so the exposition
/// format is visible and there is no dependency to keep in step.
public actor MetricsRegistry {
    public static let shared = MetricsRegistry()

    private var counters: [String: [String: Double]] = [:]
    private var gauges: [String: [String: Double]] = [:]
    private var histograms: [String: [String: Histogram]] = [:]
    private var help: [String: String] = [:]

    public init() {}

    private func key(_ labels: [String: String]) -> String {
        labels.keys.sorted().map { "\($0)=\"\(labels[$0]!)\"" }.joined(separator: ",")
    }

    public func increment(_ name: String, by amount: Double = 1, labels: [String: String] = [:], help h: String? = nil) {
        if let h { help[name] = h }
        counters[name, default: [:]][key(labels), default: 0] += amount
    }

    public func set(_ name: String, to value: Double, labels: [String: String] = [:], help h: String? = nil) {
        if let h { help[name] = h }
        gauges[name, default: [:]][key(labels)] = value
    }

    public func observe(_ name: String, _ value: Double, labels: [String: String] = [:], help h: String? = nil) {
        if let h { help[name] = h }
        var hist = histograms[name]?[key(labels)] ?? Histogram()
        hist.observe(value)
        histograms[name, default: [:]][key(labels)] = hist
    }

    public func counterValue(_ name: String, labels: [String: String] = [:]) -> Double {
        counters[name]?[key(labels)] ?? 0
    }

    public func gaugeValue(_ name: String, labels: [String: String] = [:]) -> Double {
        gauges[name]?[key(labels)] ?? 0
    }

    public func histogramQuantile(_ name: String, _ q: Double, labels: [String: String] = [:]) -> Double {
        histograms[name]?[key(labels)]?.quantile(q) ?? 0
    }

    private func labelSuffix(_ k: String, extra: String? = nil) -> String {
        var parts: [String] = []
        if !k.isEmpty { parts.append(k) }
        if let extra { parts.append(extra) }
        return parts.isEmpty ? "" : "{\(parts.joined(separator: ","))}"
    }

    public func render() -> String {
        var out = ""
        for (name, series) in counters.sorted(by: { $0.key < $1.key }) {
            if let h = help[name] { out += "# HELP \(name) \(h)\n" }
            out += "# TYPE \(name) counter\n"
            for (k, v) in series.sorted(by: { $0.key < $1.key }) {
                out += "\(name)\(labelSuffix(k)) \(v)\n"
            }
        }
        for (name, series) in gauges.sorted(by: { $0.key < $1.key }) {
            if let h = help[name] { out += "# HELP \(name) \(h)\n" }
            out += "# TYPE \(name) gauge\n"
            for (k, v) in series.sorted(by: { $0.key < $1.key }) {
                out += "\(name)\(labelSuffix(k)) \(v)\n"
            }
        }
        for (name, series) in histograms.sorted(by: { $0.key < $1.key }) {
            if let h = help[name] { out += "# HELP \(name) \(h)\n" }
            out += "# TYPE \(name) histogram\n"
            for (k, hist) in series.sorted(by: { $0.key < $1.key }) {
                for (bound, count) in hist.cumulative {
                    let le = bound.isInfinite ? "+Inf" : String(bound)
                    out += "\(name)_bucket\(labelSuffix(k, extra: "le=\"\(le)\"")) \(count)\n"
                }
                out += "\(name)_sum\(labelSuffix(k)) \(hist.sum)\n"
                out += "\(name)_count\(labelSuffix(k)) \(hist.count)\n"
            }
        }
        return out
    }
}
