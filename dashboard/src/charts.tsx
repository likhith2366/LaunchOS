import { useMemo, useState } from "react";

/** One point on a time series. */
export interface Point {
  t: number;
  v: number;
}

interface LineChartProps {
  points: Point[];
  /** Names the series, so a single-series chart needs no legend box. */
  label: string;
  unit?: string;
  color: string;
  height?: number;
  /** Shown as a dashed reference line, e.g. a controller threshold. */
  threshold?: { value: number; label: string };
  format?: (v: number) => string;
}

const PAD = { top: 10, right: 44, bottom: 20, left: 8 };

/**
 * A single-series line chart with a crosshair and tooltip.
 *
 * One series per chart, by design. Admission rate, queue depth and latency are three
 * different scales, and putting any two of them on one plot with two y-axes would let
 * the reader infer a relationship from whatever the axis scaling happened to produce.
 * Small multiples instead: same x, one measure each, honest comparison.
 */
export function LineChart({
  points,
  label,
  unit = "",
  color,
  height = 132,
  threshold,
  format = (v) => v.toFixed(0),
}: LineChartProps) {
  const [hover, setHover] = useState<number | null>(null);
  const width = 480;

  const geometry = useMemo(() => {
    if (points.length === 0) return null;
    const xs = points.map((p) => p.t);
    const minX = Math.min(...xs);
    const maxX = Math.max(...xs);
    const maxV = Math.max(
      ...points.map((p) => p.v),
      threshold ? threshold.value : 0,
      1,
    );
    // Axis starts at zero: these are all magnitudes, and a truncated baseline
    // exaggerates every change on them.
    const spanX = maxX - minX || 1;
    const plotW = width - PAD.left - PAD.right;
    const plotH = height - PAD.top - PAD.bottom;
    const x = (t: number) => PAD.left + ((t - minX) / spanX) * plotW;
    const y = (v: number) => PAD.top + plotH - (v / maxV) * plotH;
    return { x, y, minX, maxX, maxV, plotW, plotH };
  }, [points, threshold, height]);

  if (!geometry || points.length === 0) {
    return (
      <div className="empty-state" style={{ height }}>
        No samples yet
      </div>
    );
  }

  const { x, y, maxV, plotH } = geometry;
  const path = points
    .map((p, i) => `${i === 0 ? "M" : "L"}${x(p.t)},${y(p.v)}`)
    .join(" ");
  const latest = points[points.length - 1];
  const hovered = hover === null ? null : points[hover];

  function onMove(event: React.MouseEvent<SVGSVGElement>) {
    const rect = event.currentTarget.getBoundingClientRect();
    const px = ((event.clientX - rect.left) / rect.width) * width;
    // Nearest point rather than the one under the cursor exactly, so the hit target is
    // comfortably larger than the 2px line.
    let best = 0;
    let bestDistance = Infinity;
    points.forEach((p, i) => {
      const d = Math.abs(x(p.t) - px);
      if (d < bestDistance) {
        bestDistance = d;
        best = i;
      }
    });
    setHover(best);
  }

  return (
    <div className="chart-wrap">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        width="100%"
        height={height}
        role="img"
        aria-label={`${label} over time, currently ${format(latest!.v)}${unit}`}
        onMouseMove={onMove}
        onMouseLeave={() => setHover(null)}
        style={{ display: "block", overflow: "visible" }}
      >
        {/* Recessive gridlines: hairlines at a quarter of the range. */}
        {[0.25, 0.5, 0.75, 1].map((f) => (
          <line
            key={f}
            x1={PAD.left}
            x2={width - PAD.right}
            y1={y(maxV * f)}
            y2={y(maxV * f)}
            stroke="var(--gridline)"
            strokeWidth={1}
          />
        ))}
        <line
          x1={PAD.left}
          x2={width - PAD.right}
          y1={PAD.top + plotH}
          y2={PAD.top + plotH}
          stroke="var(--baseline)"
          strokeWidth={1}
        />

        {threshold && threshold.value <= maxV && (
          <>
            <line
              x1={PAD.left}
              x2={width - PAD.right}
              y1={y(threshold.value)}
              y2={y(threshold.value)}
              stroke="var(--text-muted)"
              strokeWidth={1}
              strokeDasharray="4 3"
            />
            <text
              x={width - PAD.right + 4}
              y={y(threshold.value) + 3}
              fontSize={10}
              fill="var(--text-muted)"
            >
              {threshold.label}
            </text>
          </>
        )}

        <path
          d={path}
          fill="none"
          stroke={color}
          strokeWidth={2}
          strokeLinejoin="round"
        />

        {/* Direct label at the live end, so identity never depends on colour alone.
            This is also the relief the contrast check asks for on the light surface. */}
        <circle
          cx={x(latest!.t)}
          cy={y(latest!.v)}
          r={3.5}
          fill={color}
          stroke="var(--surface-1)"
          strokeWidth={2}
        />
        <text
          x={width - PAD.right + 4}
          y={y(latest!.v) + 4}
          fontSize={11}
          fill="var(--text-secondary)"
          style={{ fontVariantNumeric: "tabular-nums" }}
        >
          {format(latest!.v)}
          {unit}
        </text>

        {hovered && (
          <>
            <line
              x1={x(hovered.t)}
              x2={x(hovered.t)}
              y1={PAD.top}
              y2={PAD.top + plotH}
              stroke="var(--baseline)"
              strokeWidth={1}
            />
            <circle
              cx={x(hovered.t)}
              cy={y(hovered.v)}
              r={4}
              fill={color}
              stroke="var(--surface-1)"
              strokeWidth={2}
            />
          </>
        )}
      </svg>

      {hovered && (
        <div
          className="tooltip"
          style={{
            left: `calc(${(x(hovered.t) / width) * 100}% + 10px)`,
            top: 4,
          }}
        >
          <div className="tt-title">
            {new Date(hovered.t).toLocaleTimeString()}
          </div>
          <div>
            {label}:{" "}
            <strong>
              {format(hovered.v)}
              {unit}
            </strong>
          </div>
        </div>
      )}
    </div>
  );
}

interface HeatmapProps {
  /** One cell per shard. */
  cells: {
    shardID: number;
    available: number;
    reserved: number;
    sold: number;
  }[];
  /** Units per shard at the start, for scaling the ramp. */
  capacity: number;
}

/**
 * Availability per shard, as a grid of magnitudes on one sequential hue.
 *
 * This is the view that makes tail fragmentation visible: late in a launch the grid
 * goes mostly empty with a few cells still holding stock, which is exactly the state in
 * which a request can be told "sold out" while units remain.
 */
export function ShardHeatmap({ cells, capacity }: HeatmapProps) {
  const [hover, setHover] = useState<number | null>(null);
  const steps = [
    "var(--seq-100)",
    "var(--seq-200)",
    "var(--seq-300)",
    "var(--seq-400)",
    "var(--seq-500)",
    "var(--seq-600)",
    "var(--seq-700)",
  ];

  function colorFor(available: number) {
    if (available === 0) return "var(--gridline)";
    const fraction = capacity > 0 ? Math.min(1, available / capacity) : 0;
    const index = Math.min(
      steps.length - 1,
      Math.floor(fraction * steps.length),
    );
    return steps[index]!;
  }

  const hovered =
    hover === null ? null : cells.find((c) => c.shardID === hover);

  return (
    <div className="chart-wrap">
      <div
        className="heatmap"
        role="img"
        aria-label={`Availability across ${cells.length} shards`}
      >
        {cells.map((cell) => (
          <div
            key={cell.shardID}
            className="cell"
            style={{ background: colorFor(cell.available) }}
            onMouseEnter={() => setHover(cell.shardID)}
            onMouseLeave={() => setHover(null)}
            title={`shard ${cell.shardID}: ${cell.available} available`}
          />
        ))}
      </div>
      <ul className="legend" style={{ marginTop: 10 }}>
        <li>
          <span className="swatch" style={{ background: "var(--gridline)" }} />
          empty
        </li>
        <li>
          <span className="swatch" style={{ background: "var(--seq-200)" }} />
          low
        </li>
        <li>
          <span className="swatch" style={{ background: "var(--seq-700)" }} />
          full
        </li>
      </ul>
      {hovered && (
        <div className="tooltip" style={{ left: 0, top: -6 }}>
          <div className="tt-title">shard {hovered.shardID}</div>
          <div>
            {hovered.available} available · {hovered.reserved} reserved ·{" "}
            {hovered.sold} sold
          </div>
        </div>
      )}
    </div>
  );
}
