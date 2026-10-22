import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  AdmissionStatus,
  ControlSample,
  Inventory,
  LaunchSummary,
  NodeStatus,
  api,
  optional,
} from "./api";
import { LineChart, Point, ShardHeatmap } from "./charts";

const POLL_MS = 2000;

/** Polls while the tab is visible. A background tab does not need live data. */
function usePolling(work: () => Promise<void>, intervalMS: number) {
  const latest = useRef(work);
  latest.current = work;

  useEffect(() => {
    let cancelled = false;
    let timer: number | undefined;

    async function tick() {
      if (cancelled) return;
      if (!document.hidden) await latest.current();
      if (!cancelled) timer = window.setTimeout(tick, intervalMS);
    }
    void tick();

    return () => {
      cancelled = true;
      if (timer) window.clearTimeout(timer);
    };
  }, [intervalMS]);
}

function StatTile({
  label,
  value,
  sub,
  children,
}: {
  label: string;
  value: string;
  sub?: string;
  children?: React.ReactNode;
}) {
  return (
    <div className="card tile">
      <p className="tile-label">{label}</p>
      <div className="tile-value">{value}</div>
      {sub && <div className="tile-sub">{sub}</div>}
      {children}
    </div>
  );
}

/**
 * The invariant, stated plainly.
 *
 * Status colour is never the only signal: the banner carries an icon and words, so it
 * reads correctly in greyscale, under full colour-vision deficiency, and to a screen
 * reader.
 */
function InvariantBanner({ inventory }: { inventory: Inventory }) {
  const ok = inventory.invariantHolds;
  return (
    <div className={`banner ${ok ? "ok" : "bad"}`} role="status">
      <svg className="icon" viewBox="0 0 20 20" aria-hidden="true">
        {ok ? (
          <path
            d="M3 10.5l4.2 4.2L17 5"
            fill="none"
            stroke="var(--good)"
            strokeWidth="2.4"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        ) : (
          <>
            <circle
              cx="10"
              cy="10"
              r="8.2"
              fill="none"
              stroke="var(--critical)"
              strokeWidth="2.2"
            />
            <path
              d="M10 5.6v6"
              stroke="var(--critical)"
              strokeWidth="2.2"
              strokeLinecap="round"
            />
            <circle cx="10" cy="14.6" r="1.2" fill="var(--critical)" />
          </>
        )}
      </svg>
      <div>
        <div className="banner-title">
          {ok ? "Inventory accounting holds" : "INVARIANT VIOLATED"}
        </div>
        <div className="banner-detail">
          available {inventory.available} + reserved {inventory.reserved} + sold{" "}
          {inventory.sold} = {inventory.accountedFor}
          {ok ? " = " : " ≠ "}
          {inventory.totalUnits} total
          {ok ? "" : " — a unit has been sold twice or lost"}
        </div>
      </div>
    </div>
  );
}

export default function App() {
  const [launches, setLaunches] = useState<LaunchSummary[]>([]);
  const [sku, setSku] = useState<string | null>(null);
  const [node, setNode] = useState<NodeStatus | null>(null);
  const [inventory, setInventory] = useState<Inventory | null>(null);
  const [admission, setAdmission] = useState<AdmissionStatus | null>(null);
  const [trace, setTrace] = useState<ControlSample[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [showTables, setShowTables] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const [nodeStatus, list] = await Promise.all([
        api.node(),
        api.launches(),
      ]);
      setNode(nodeStatus);
      setLaunches(list);

      // Default to the newest launch, which during a launch is the one being watched.
      const active =
        sku && list.some((l) => l.sku === sku)
          ? sku
          : (list[list.length - 1]?.sku ?? null);
      if (active !== sku) setSku(active);
      if (!active) {
        setInventory(null);
        setAdmission(null);
        setTrace([]);
        setError(null);
        return;
      }

      const [inv, adm, tr] = await Promise.all([
        api.inventory(active),
        optional(() => api.admission(active)),
        optional(() => api.trace(active)),
      ]);
      setInventory(inv);
      setAdmission(adm);
      setTrace(tr?.samples ?? []);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [sku]);

  usePolling(refresh, POLL_MS);

  const launch = useMemo(
    () => launches.find((l) => l.sku === sku) ?? null,
    [launches, sku],
  );

  const series = useMemo(() => {
    const rate: Point[] = [];
    const depth: Point[] = [];
    const latency: Point[] = [];
    const inFlight: Point[] = [];
    for (const sample of trace) {
      const t = new Date(sample.at).getTime();
      if (Number.isNaN(t)) continue;
      rate.push({ t, v: sample.rate });
      depth.push({ t, v: sample.queueDepth });
      latency.push({ t, v: sample.health.latencyP99MS });
      inFlight.push({ t, v: sample.health.inFlight });
    }
    return { rate, depth, latency, inFlight };
  }, [trace]);

  const soldFraction =
    inventory && inventory.totalUnits > 0
      ? inventory.sold / inventory.totalUnits
      : 0;
  const reservedFraction =
    inventory && inventory.totalUnits > 0
      ? inventory.reserved / inventory.totalUnits
      : 0;

  return (
    <div className="app">
      <header className="masthead">
        <h1>LaunchOS operator</h1>
        <div className="tile-sub">
          {node
            ? `${node.nodeID} · ${node.liveNodes.length} of ${node.nodeCount} node${
                node.nodeCount === 1 ? "" : "s"
              } live · ${node.multiNodeMode} · redis ${node.redisEnabled ? "up" : "off"}`
            : "connecting…"}
        </div>
      </header>

      <p className="subtitle">
        Live view of one launch: whether the inventory accounting still adds up,
        how much stock is left and where it sits, and what the admission
        controller is doing about the queue behind it. Read-only — nothing here
        can change a launch in progress.
      </p>

      <div className="toolbar">
        <label className="field">
          Launch
          <select
            value={sku ?? ""}
            onChange={(e) => setSku(e.target.value || null)}
            disabled={launches.length === 0}
          >
            {launches.length === 0 && <option value="">no launches</option>}
            {launches.map((l) => (
              <option key={l.id} value={l.sku}>
                {l.sku} · {l.totalUnits} units · {l.shardCount} shard
                {l.shardCount === 1 ? "" : "s"}
              </option>
            ))}
          </select>
        </label>
        <button
          onClick={() => setShowTables((v) => !v)}
          aria-pressed={showTables}
          title="Every chart here also has a table, so nothing depends on reading a colour"
        >
          {showTables ? "Hide tables" : "Show tables"}
        </button>
      </div>

      {error && (
        <div className="card error" style={{ marginBottom: 16 }}>
          <strong>Cannot reach a node.</strong> {error}
        </div>
      )}

      {!inventory && !error && (
        <div className="card">
          <div className="empty-state">
            No launch yet. Create one, or run the simulator, and this fills in.
          </div>
        </div>
      )}

      {inventory && launch && (
        <>
          <InvariantBanner inventory={inventory} />

          <div className="kpi-row">
            <StatTile
              label="Sold"
              value={`${inventory.sold}`}
              sub={`of ${inventory.totalUnits} units · ${(soldFraction * 100).toFixed(1)}%`}
            >
              {/* A single ratio against a limit: a meter, not a two-slice pie. */}
              <div className="meter" aria-hidden="true">
                <span
                  style={{
                    width: `${soldFraction * 100}%`,
                    background: "var(--seq-600)",
                  }}
                />
                <span
                  style={{
                    width: `${reservedFraction * 100}%`,
                    background: "var(--seq-300)",
                  }}
                />
              </div>
              <div className="tile-sub">
                dark: sold · light: reserved · track: available
              </div>
            </StatTile>

            <StatTile
              label="Available"
              value={`${inventory.available}`}
              sub={`on ${inventory.nonEmptyShards} of ${launch.shardCount} shard${
                launch.shardCount === 1 ? "" : "s"
              }${inventory.collapsed ? " · collapsed" : ""}`}
            />

            <StatTile
              label="Queue depth"
              value={admission ? `${admission.queueDepth}` : "—"}
              sub={
                admission
                  ? `mean ${admission.meanQueueDepth.toFixed(0)} over the run`
                  : "no waiting room"
              }
            />

            <StatTile
              label="Admission rate"
              value={admission ? `${admission.rate.toFixed(0)}/s` : "—"}
              sub={
                admission
                  ? `${admission.kind} · ${admission.policy}`
                  : "unthrottled"
              }
            />

            <StatTile
              label="Checkout p99"
              value={
                admission
                  ? `${admission.health.latencyP99MS.toFixed(0)} ms`
                  : "—"
              }
              sub={
                admission
                  ? `${admission.health.inFlight} in flight · peak ${admission.health.peakInFlight}`
                  : undefined
              }
            />

            <StatTile
              label="Errors"
              value={
                admission
                  ? `${(admission.health.errorRate * 100).toFixed(2)}%`
                  : "—"
              }
              sub={
                admission
                  ? `${admission.health.completed} completed · ${admission.health.failed} failed`
                  : undefined
              }
            />
          </div>

          <div className="charts">
            <div className="card">
              <h2>Admission rate</h2>
              <p className="note">
                What the controller decided, in customers per second. A sawtooth
                here means the signal needs more smoothing or a wider dead band.
              </p>
              <LineChart
                points={series.rate}
                label="Admission rate"
                unit="/s"
                color="var(--series-1)"
              />
            </div>

            <div className="card">
              <h2>Queue depth</h2>
              <p className="note">
                The backlog the controller is draining. Its own chart rather
                than a second axis on the one above: two scales on one plot
                invite a relationship that the axis scaling invented.
              </p>
              <LineChart
                points={series.depth}
                label="Waiting"
                color="var(--series-2)"
              />
            </div>

            <div className="card">
              <h2>In-flight checkouts</h2>
              <p className="note">
                The leading indicator the controller acts on — it rises the
                moment service time does, before any latency percentile has
                moved.
              </p>
              <LineChart
                points={series.inFlight}
                label="In flight"
                color="var(--series-3)"
              />
            </div>

            <div className="card">
              <h2>Checkout p99</h2>
              <p className="note">
                The lagging indicator, which is what it is alarmed on rather
                than controlled on.
              </p>
              <LineChart
                points={series.latency}
                label="p99"
                unit=" ms"
                color="var(--series-1)"
              />
            </div>
          </div>

          <div className="card" style={{ marginBottom: 16 }}>
            <h2>Stock by shard</h2>
            <p className="note">
              One cell per inventory row. Late in a launch this goes mostly
              empty with a few cells still holding stock — which is the state in
              which a request can be told sold out while units remain.
            </p>
            <ShardHeatmap
              cells={inventory.shards}
              capacity={Math.max(
                1,
                Math.ceil(launch.totalUnits / launch.shardCount),
              )}
            />
          </div>

          {showTables && (
            <div className="charts">
              <div className="card">
                <h2>Shards</h2>
                <p className="note">The heatmap above, as numbers.</p>
                <table className="data">
                  <thead>
                    <tr>
                      <th scope="col">Shard</th>
                      <th scope="col">Available</th>
                      <th scope="col">Reserved</th>
                      <th scope="col">Sold</th>
                    </tr>
                  </thead>
                  <tbody>
                    {inventory.shards.map((s) => (
                      <tr key={s.shardID}>
                        <td>{s.shardID}</td>
                        <td>{s.available}</td>
                        <td>{s.reserved}</td>
                        <td>{s.sold}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="card">
                <h2>Control trace</h2>
                <p className="note">
                  The last twenty control intervals: the rate, what the
                  controller did and the health that drove it.
                </p>
                <table className="data">
                  <thead>
                    <tr>
                      <th scope="col">At</th>
                      <th scope="col">Rate</th>
                      <th scope="col">Granted</th>
                      <th scope="col">Action</th>
                      <th scope="col">Depth</th>
                      <th scope="col">p99 ms</th>
                    </tr>
                  </thead>
                  <tbody>
                    {trace
                      .slice(-20)
                      .reverse()
                      .map((s, i) => (
                        <tr key={`${s.at}-${i}`}>
                          <td>{new Date(s.at).toLocaleTimeString()}</td>
                          <td>{s.rate.toFixed(0)}</td>
                          <td>{s.granted}</td>
                          <td>{s.action}</td>
                          <td>{s.queueDepth}</td>
                          <td>{s.health.latencyP99MS.toFixed(0)}</td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}

      <footer className="footnote">
        Polls every {POLL_MS / 1000}s while this tab is visible. Latency figures
        are measured server-side inside the node; the queue depth and admission
        rate come from the control trace, one sample per interval. Grafana at{" "}
        <code>:3000</code>
        has the same metrics over a longer window and across all nodes.
      </footer>
    </div>
  );
}
