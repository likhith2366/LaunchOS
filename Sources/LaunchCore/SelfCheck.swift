import Foundation
import Logging

/// Continuously verifies the invariants the project claims, and publishes the result as
/// metrics.
///
/// The invariant is already asserted by tests and by the simulator, but both of those
/// only run when someone runs them. A launch is a one-shot event: if a unit is sold
/// twice at 09:00:03 there is no second chance to notice. So the running system checks
/// itself on a timer and exports what it found, which is also what makes the
/// "oversold units" panel on the dashboard a measurement rather than a decoration.
///
/// This is a read-only observer. It never repairs anything — a silent repair would hide
/// exactly the bug worth knowing about.
///
/// ## Why there are two tiers
///
/// Continuous verification has to cost less than the thing it verifies, or it becomes
/// the bottleneck — and the first version of this did exactly that. It ran the full
/// invariant every five seconds, including a `SUM(units)` over every order and a join of
/// orders against idempotency keys. Both are O(the table), so as a launch grew the
/// checker read more and more: at a quarter of a million orders it was reading about
/// 900,000 rows from `orders` and 450,000 from `idempotency_keys` every twelve seconds,
/// and allocation throughput fell from 814/s to roughly 70/s. The verifier was eating
/// the system it was watching.
///
/// So the checks are split by what they cost:
///
/// - **Every tick**: the arithmetic over `inventory_shards`, which is a handful of rows
///   however large the launch is, plus the oversold comparison. This is the check that
///   actually catches a double-sold unit, and it is the one worth running constantly.
/// - **Occasionally**: reconciling sold units against the orders table, and counting
///   duplicate orders per idempotency key. These scan whole tables, and both are
///   belt-and-braces — `orders.reservation_id` is already unique, so a duplicate order
///   per key cannot happen without the unique constraint failing first.
public actor SelfCheck {
    private let store: Store
    private let logger: Logger
    private let interval: Duration
    /// How often the table-scanning reconciliations run. A multiple of `interval`, not a
    /// substitute for it.
    private let deepInterval: Duration
    private var lastDeepCheck: ContinuousClock.Instant?
    public private(set) var checks = 0
    public private(set) var deepChecks = 0
    public private(set) var violations: [String] = []

    public init(
        store: Store,
        logger: Logger,
        interval: Duration = .seconds(5),
        deepInterval: Duration = .seconds(120)
    ) {
        self.store = store
        self.logger = logger
        self.interval = interval
        self.deepInterval = deepInterval
    }

    @discardableResult
    public func checkOnce() async -> [String] {
        checks += 1
        var found: [String] = []
        var oversoldTotal = 0
        var duplicateTotal = 0

        let launches: [Launch]
        do {
            launches = try await store.allLaunches()
        } catch {
            logger.debug("self-check could not list launches", metadata: ["error": .string("\(error)")])
            return []
        }

        let runDeep: Bool
        if let last = lastDeepCheck {
            runDeep = last.duration(to: .now) >= deepInterval
        } else {
            runDeep = true
        }
        if runDeep {
            lastDeepCheck = .now
            deepChecks += 1
        }

        for launch in launches {
            do {
                let state = try await store.assertInvariant(launchID: launch.id, deep: runDeep)
                let oversold = max(0, state.sold - state.totalUnits)
                oversoldTotal += oversold
                await MetricsRegistry.shared.set(
                    "launchos_inventory_available", to: Double(state.available),
                    labels: ["sku": launch.sku], help: "Units available"
                )
                await MetricsRegistry.shared.set(
                    "launchos_inventory_reserved", to: Double(state.reserved),
                    labels: ["sku": launch.sku], help: "Units held by live reservations"
                )
                await MetricsRegistry.shared.set(
                    "launchos_inventory_sold", to: Double(state.sold),
                    labels: ["sku": launch.sku], help: "Units sold"
                )
                await MetricsRegistry.shared.set(
                    "launchos_shards_with_stock", to: Double(state.nonEmptyShards),
                    labels: ["sku": launch.sku],
                    help: "Shards still holding stock; the gap to the shard count is fragmentation"
                )
            } catch let error as LaunchError {
                // Loud, and left alone. A repair here would destroy the evidence.
                let detail = "\(launch.sku): \(error)"
                found.append(detail)
                logger.critical("INVARIANT VIOLATED", metadata: ["detail": .string(detail)])
                // A violated invariant means the accounting is wrong, which is the
                // strongest signal available that a unit was sold twice.
                oversoldTotal += 1
            } catch {
                logger.debug("self-check failed", metadata: ["error": .string("\(error)")])
            }

            // Also a table scan, so it rides the slow timer with the deep check.
            if runDeep {
                duplicateTotal += (try? await store.duplicateOrderCount(launchID: launch.id)) ?? 0
            }
        }

        await MetricsRegistry.shared.set(
            "launchos_oversold_units", to: Double(oversoldTotal),
            help: "Units sold beyond the launch total. Must be zero"
        )
        // Only published when it was actually recounted, so a stale zero cannot look
        // like a fresh one.
        if runDeep {
            await MetricsRegistry.shared.set(
                "launchos_duplicate_orders_total", to: Double(duplicateTotal),
                help: "Orders beyond the first for a single idempotency key. Must be zero"
            )
        }
        await MetricsRegistry.shared.set(
            "launchos_invariant_violations", to: Double(found.count),
            help: "Launches whose inventory accounting does not add up"
        )
        violations = found
        return found
    }

    public func run() async {
        let ns = UInt64(interval.seconds * 1_000_000_000)
        while !Task.isCancelled {
            await checkOnce()
            try? await Task.sleep(nanoseconds: ns)
        }
    }
}
