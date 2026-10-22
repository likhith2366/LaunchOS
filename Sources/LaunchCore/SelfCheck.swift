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
public actor SelfCheck {
    private let store: Store
    private let logger: Logger
    private let interval: Duration
    public private(set) var checks = 0
    public private(set) var violations: [String] = []

    public init(store: Store, logger: Logger, interval: Duration = .seconds(5)) {
        self.store = store
        self.logger = logger
        self.interval = interval
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

        for launch in launches {
            do {
                let state = try await store.assertInvariant(launchID: launch.id)
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

            duplicateTotal += (try? await store.duplicateOrderCount(launchID: launch.id)) ?? 0
        }

        await MetricsRegistry.shared.set(
            "launchos_oversold_units", to: Double(oversoldTotal),
            help: "Units sold beyond the launch total. Must be zero"
        )
        await MetricsRegistry.shared.set(
            "launchos_duplicate_orders_total", to: Double(duplicateTotal),
            help: "Orders beyond the first for a single idempotency key. Must be zero"
        )
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
