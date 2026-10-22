import Foundation

/// SplitMix64. Deterministic, seedable, and identical on every platform, which is what
/// lets a failed simulator run be replayed exactly from its seed.
public struct SplitMix64: RandomNumberGenerator, Sendable {
    private var state: UInt64

    public init(seed: UInt64) {
        self.state = seed
    }

    public mutating func next() -> UInt64 {
        state = state &+ 0x9E37_79B9_7F4A_7C15
        var z = state
        z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
        z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
        return z ^ (z >> 31)
    }

    /// Uniform in [0, 1).
    public mutating func uniform() -> Double {
        Double(next() >> 11) * (1.0 / 9_007_199_254_740_992.0)
    }

    /// Exponential with the given mean. Used for Poisson arrival gaps.
    public mutating func exponential(mean: Double) -> Double {
        let u = max(uniform(), 1e-12)
        return -mean * log(u)
    }

    public mutating func int(in range: Range<Int>) -> Int {
        guard range.lowerBound < range.upperBound else { return range.lowerBound }
        let span = UInt64(range.upperBound - range.lowerBound)
        return range.lowerBound + Int(next() % span)
    }

    public mutating func bernoulli(_ p: Double) -> Bool {
        uniform() < p
    }

    /// A child generator derived from this one, so independent streams in the
    /// simulator stay independent but still depend only on the root seed.
    public mutating func split() -> SplitMix64 {
        SplitMix64(seed: next())
    }
}

/// Stable 64-bit hash of a string. Swift's `Hashable` is per-process seeded, so it
/// cannot be used for anything that has to be reproducible across runs or nodes.
public func stableHash(_ s: String) -> UInt64 {
    var h: UInt64 = 0xCBF2_9CE4_8422_2325
    for byte in s.utf8 {
        h ^= UInt64(byte)
        h = h &* 0x0000_0100_0000_01B3
    }
    return h
}
