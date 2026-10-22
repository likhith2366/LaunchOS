// Typed client for the node API.
//
// Everything here is a read. The operator view deliberately cannot admit customers,
// cancel reservations or reset a launch: a dashboard that can change a launch in
// progress is a dashboard that can ruin one by a misclick.

const BASE = "/api";

export interface Shard {
  shardID: number;
  available: number;
  reserved: number;
  sold: number;
}

export interface Inventory {
  launchID: string;
  totalUnits: number;
  available: number;
  reserved: number;
  sold: number;
  collapsed: boolean;
  accountedFor: number;
  invariantHolds: boolean;
  nonEmptyShards: number;
  shards: Shard[];
}

export interface LaunchSummary {
  id: string;
  sku: string;
  totalUnits: number;
  shardCount: number;
  collapseThreshold: number;
  collapsed: boolean;
  requiresAdmission: boolean;
}

export interface HealthSnapshot {
  inFlight: number;
  smoothedInFlight: number;
  latencyP99MS: number;
  smoothedLatencyMS: number;
  errorRate: number;
  completed: number;
  failed: number;
  peakInFlight: number;
}

export interface AdmissionStatus {
  kind: string;
  policy: string;
  rate: number;
  totalAdmitted: number;
  queueDepth: number;
  meanQueueDepth: number;
  health: HealthSnapshot;
}

export interface ControlSample {
  at: string;
  rate: number;
  granted: number;
  action: string;
  queueDepth: number;
  admitDurationMS: number;
  health: HealthSnapshot;
}

export interface NodeStatus {
  nodeID: string;
  nodeIndex: number;
  nodeCount: number;
  multiNodeMode: string;
  ownedShards: number[];
  liveNodes: string[];
  redisEnabled: boolean;
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function get<T>(path: string): Promise<T> {
  const response = await fetch(`${BASE}${path}`, {
    headers: { accept: "application/json" },
  });
  if (!response.ok) {
    throw new ApiError(response.status, `${path} returned ${response.status}`);
  }
  return (await response.json()) as T;
}

export const api = {
  node: () => get<NodeStatus>("/node"),
  launches: () => get<LaunchSummary[]>("/launches"),
  inventory: (sku: string) =>
    get<Inventory>(`/launches/${encodeURIComponent(sku)}/inventory`),
  admission: (sku: string) =>
    get<AdmissionStatus>(`/launches/${encodeURIComponent(sku)}/admission`),
  trace: (sku: string) =>
    get<{ samples: ControlSample[] }>(
      `/launches/${encodeURIComponent(sku)}/admission/trace`,
    ),
};

/**
 * Not every launch has a waiting room, so the admission endpoints legitimately 404.
 * Treated as "absent" rather than as an error, so the view does not complain about a
 * launch that simply has no queue.
 */
export async function optional<T>(work: () => Promise<T>): Promise<T | null> {
  try {
    return await work();
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return null;
    throw error;
  }
}
