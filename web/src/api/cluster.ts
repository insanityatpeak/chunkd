// ClusterAPI is the dashboard's only view of a cluster. WasmClusterAPI runs
// the simulated cluster in a Web Worker; HttpClusterAPI talks to a real
// gateway (docker compose up). Shapes mirror internal/client in Go.

export interface FileInfo {
  path: string;
  version: number;
  size: number;
  sha256?: string;
  chunks: number;
  // Replication: chunks below RF, and the fewest alive copies of any chunk.
  underReplicated?: number;
  minLive?: number;
}

export interface ChunkRef {
  index: number;
  id: string;
  size: number;
  replicas: string[] | null;
  servedBy?: string;
  rejected?: string[] | null;
}

export interface Manifest extends FileInfo {
  chunkSize: number;
  chunkList: ChunkRef[] | null;
}

export type NodeState = 'alive' | 'suspect' | 'dead' | 'unknown';

export interface NodeView {
  id: string;
  rack: string;
  alive: boolean;
  state: NodeState;
  heartbeatAgeMs: number;
  crashed?: boolean; // sim only: the process is down
  usedBytes: number;
  chunks: number;
  heartbeats?: number;
}

// Health is replication across all chunks (client.Health in Go).
export interface Health {
  chunks: number;
  underReplicated: number;
  overReplicated: number;
  lost: number;
  replicas: number[] | null;
  repairQueued: number;
  repairInFlight: number;
  repairWaiting: number;
  repairCompleted: number;
  repairBytes: number;
  repairTrimmed: number;
  repairTimedOut: number;
  repairFailed: number;
  detectorStalls: number;
}

export interface RepairCopy {
  id: number;
  chunk: string;
  source: string;
  target: string;
  bytes: number;
  startedMs: number;
}

export interface TimelineEvent {
  seq: number;
  atMs: number;
  kind: 'node' | 'copy' | 'trim';
  node: string;
  text: string;
}

export interface NetStats {
  sent: number;
  delivered: number;
  dropped: number;
  duplicated: number;
  bytes: number;
}

// ClusterView is one snapshot. Every *Ms instant is on the metadata
// server's clock, whose reading is nowMs (simulated time in the sim).
export interface ClusterView {
  nowMs: number;
  nodes: NodeView[];
  files: FileInfo[];
  health?: Health;
  copies: RepairCopy[];
  // The newest events, oldest first, at most TIMELINE_MAX.
  timeline: TimelineEvent[];
  net?: NetStats; // sim only
  meta?: { id: string; applied: number; pendingUploads: number };
}

export const TIMELINE_MAX = 500;

// Timeline accumulates events fetched incrementally by seq. A latest seq
// below the one already seen means the metadata server restarted and began
// a new sequence, so the old timeline is dropped.
export class Timeline {
  private events: TimelineEvent[] = [];
  seq = 0;

  merge(events: TimelineEvent[] | null, latest: number): TimelineEvent[] {
    if (latest < this.seq) this.events = [];
    const fresh = (events ?? []).filter((e) => e.seq > (this.events.at(-1)?.seq ?? 0));
    if (fresh.length > 0) this.events = [...this.events, ...fresh].slice(-TIMELINE_MAX);
    this.seq = latest;
    return this.events;
  }

  reset() {
    this.events = [];
    this.seq = 0;
  }
}

export interface Download {
  manifest: Manifest;
  data: Uint8Array;
}

export interface ClusterAPI {
  readonly kind: 'sim' | 'http';
  readonly label: string;
  start(seed: number): Promise<void>;
  subscribe(fn: (v: ClusterView) => void): () => void;
  upload(path: string, data: Uint8Array): Promise<Manifest>;
  download(path: string): Promise<Download>;
  stat(path: string): Promise<Manifest>;
  remove(path: string): Promise<void>;
  // Sim controls; no-ops against a real cluster.
  pause(): void;
  resume(): void;
  setSpeed(simMsPerSec: number): void;
  crash(node: string): void;
  restart(node: string): void;
  // Scripted failure: load demo files if empty, kill node, restart it after downMs.
  scenario(node: string, downMs: number): Promise<void>;
  dispose(): void;
}

export class ChunkdError extends Error {
  constructor(
    message: string,
    readonly code: string,
  ) {
    super(message);
  }
}
