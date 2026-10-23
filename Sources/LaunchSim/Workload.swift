import ArgumentParser
import Foundation
import LaunchCore

/// Shape of the arrival process.
public enum ArrivalProfile: String, Codable, Sendable, CaseIterable, ExpressibleByArgument {
    /// Everyone at once, at t=0. The worst case, and the one the week-one concurrency
    /// test uses.
    case thunderingHerd
    /// Poisson arrivals at a constant rate. The case where a well-tuned fixed rate is
    /// expected to beat the controller.
    case steady
    /// Steady, then a sharp multi-fold jump, then steady again. The case the adaptive
    /// controller exists for.
    case spike
    /// Linear ramp from nothing to the peak rate.
    case ramp
}

/// One customer's scripted behaviour, decided entirely from the seed.
///
/// Every awkward thing a real customer does is represented here, because a simulator
/// that only models the happy path cannot produce a trustworthy number: duplicate
/// requests, abandonment and retries are what the idempotency and expiry machinery
/// exists for.
struct CustomerScript: Sendable {
    var customerID: String
    /// Seconds after the run starts at which this customer arrives.
    var arrivalOffset: Double
    /// Extra position polls beyond the minimum, to exercise retry-safety.
    var extraPolls: Int
    /// Sends its checkout twice concurrently with the same key.
    var sendsDuplicate: Bool
    /// Joins the queue, gets admitted, and never checks out. Its reservation, if any,
    /// has to come back via expiry.
    var abandons: Bool
    /// Rejoins the queue after arriving, which must not improve its position.
    var rejoinsQueue: Bool
    /// Artificial client-side delay before checkout, in milliseconds.
    var thinkTimeMS: Double
    /// Stable per-customer salt, used to pick which node to talk to.
    var salt: UInt64
    var idempotencyKey: String
}

struct WorkloadConfig: Sendable {
    var customers: Int
    var profile: ArrivalProfile
    var durationSeconds: Double
    /// Peak arrivals per second for the shaped profiles.
    var peakRate: Double
    var duplicateRate: Double
    var abandonRate: Double
    var rejoinRate: Double
    var maxThinkTimeMS: Double
    var seed: UInt64

    init(
        customers: Int,
        profile: ArrivalProfile = .spike,
        durationSeconds: Double = 20,
        peakRate: Double = 500,
        duplicateRate: Double = 0.05,
        abandonRate: Double = 0.10,
        rejoinRate: Double = 0.25,
        maxThinkTimeMS: Double = 200,
        seed: UInt64 = 0xA5A5_1234
    ) {
        self.customers = customers
        self.profile = profile
        self.durationSeconds = durationSeconds
        self.peakRate = peakRate
        self.duplicateRate = duplicateRate
        self.abandonRate = abandonRate
        self.rejoinRate = rejoinRate
        self.maxThinkTimeMS = maxThinkTimeMS
        self.seed = seed
    }
}

/// Builds the scripts for a run.
///
/// Seeded and deterministic: the same seed produces the same arrival times, the same
/// customers duplicating and abandoning, and the same think times. That is what makes a
/// failure replayable and a benchmark comparison fair — two admission policies see
/// byte-identical traffic.
///
/// ## Why the scripts are generated rather than stored
///
/// At a million customers, materialising a `CustomerScript` each is a few hundred
/// megabytes, almost all of it two heap-allocated strings per customer that are used
/// once. Only the arrival offsets are kept — eight bytes each, so eight megabytes at a
/// million — and everything else is derived on demand from the seed and the customer's
/// index.
///
/// That also makes the determinism stronger rather than weaker. Behaviour used to come
/// from one sequential RNG stream, so inserting a customer shifted every later
/// customer's behaviour; deriving it from the index means customer 500,000 behaves the
/// same way whether the run has a thousand customers or a million.
struct Workload: Sendable {
    let config: WorkloadConfig
    /// Sorted arrival offsets in seconds. The only per-customer state held in memory.
    let offsets: [Double]

    var count: Int { offsets.count }

    init(config: WorkloadConfig) {
        self.config = config
        var rng = SplitMix64(seed: config.seed)
        // Separate streams per decision, so changing the duplicate rate does not shift
        // every arrival time as well.
        var arrivalRNG = rng.split()
        var behaviourRNG = rng.split()

        var offsets: [Double] = []
        switch config.profile {
        case .thunderingHerd:
            offsets = Array(repeating: 0, count: config.customers)

        case .steady:
            let mean = 1.0 / max(config.peakRate, 0.001)
            var t = 0.0
            for _ in 0..<config.customers {
                t += arrivalRNG.exponential(mean: mean)
                offsets.append(t)
            }

        case .ramp:
            // Inverting the CDF of a linearly rising rate gives arrival times that
            // actually follow the ramp, rather than a uniform spread relabelled as one.
            for i in 0..<config.customers {
                let u = Double(i + 1) / Double(config.customers)
                offsets.append(config.durationSeconds * u.squareRoot())
            }

        case .spike:
            // Three phases: a calm third at a tenth of peak, a spike at full peak, then
            // calm again. The controller's job is to hold p99 through the middle third.
            let third = config.durationSeconds / 3
            let calmShare = 0.15
            let calmCount = Int(Double(config.customers) * calmShare / 2)
            let spikeCount = config.customers - 2 * calmCount
            for i in 0..<calmCount {
                offsets.append(third * Double(i) / Double(max(calmCount, 1)))
            }
            for i in 0..<spikeCount {
                // Poisson inside the spike window so arrivals are bursty rather than
                // evenly spaced.
                let frac = Double(i) / Double(max(spikeCount, 1))
                let jitter = arrivalRNG.uniform() * third / Double(max(spikeCount, 1)) * 2
                offsets.append(third + third * frac + jitter)
            }
            for i in 0..<calmCount {
                offsets.append(2 * third + third * Double(i) / Double(max(calmCount, 1)))
            }
        }
        offsets.sort()

        _ = behaviourRNG
        self.offsets = offsets
    }

    /// The script for one customer, derived rather than looked up.
    ///
    /// The RNG is seeded from the run seed mixed with the index, so this is a pure
    /// function of the two: the same seed and index always produce the same customer,
    /// and nothing has to be held in memory between generating them.
    func script(at index: Int) -> CustomerScript {
        let id = "c\(String(format: "%07d", index))"
        // Mixed with a large odd constant so neighbouring indexes do not produce
        // correlated streams, which they would if the index were simply added.
        var rng = SplitMix64(seed: config.seed ^ (UInt64(index) &* 0x9E37_79B9_7F4A_7C15))
        return CustomerScript(
            customerID: id,
            arrivalOffset: index < offsets.count ? offsets[index] : 0,
            extraPolls: rng.int(in: 0..<4),
            sendsDuplicate: rng.bernoulli(config.duplicateRate),
            abandons: rng.bernoulli(config.abandonRate),
            rejoinsQueue: rng.bernoulli(config.rejoinRate),
            thinkTimeMS: rng.uniform() * config.maxThinkTimeMS,
            salt: rng.next(),
            // Stable across retries, which is the entire point: a retry carries the key
            // the first attempt used.
            idempotencyKey: "idem-\(id)-\(config.seed)"
        )
    }

    var arrivalRate: Double {
        let span = offsets.last ?? 0
        return span > 0 ? Double(offsets.count) / span : Double(offsets.count)
    }
}
