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
  // Integrity, from heartbeats: copies quarantined since the node started,
  // and the current scrub pass's progress in chunks.
  corrupt?: number;
  scrubDone?: number;
  scrubTotal?: number;
  scrubPasses?: number;
  crashed?: boolean; // sim only: the process is down
  frozen?: boolean; // sim only: paused, messages held
  slowMs?: number; // sim only: added to every message to or from it
  partitioned?: boolean; // sim only: cut off from the metadata server
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
  corruptReplicas?: number;
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
  kind: 'node' | 'copy' | 'trim' | 'corrupt' | 'read' | 'write' | 'gc';
  node: string;
  text: string;
}

// VersionInfo is one retained version of a path, oldest first in a log.
export interface VersionInfo {
  version: number;
  size: number;
  sha256?: string;
  chunks: number;
  deleted?: boolean; // a delete marker
  retired?: boolean; // superseded; undelete can restore it until expiresEpoch
  expiresEpoch?: number;
}

// DeletedFile is a deleted path with a version undelete can still restore.
export interface DeletedFile {
  path: string;
  version: number;
  size: number;
  expiresEpoch: number;
}

// GCStats are the metadata server's sweep counters and retention settings.
export interface GCStats {
  orphans: number;
  sent: number;
  deleted: number;
  kept: number;
  drift: number;
  retainEpochs: number;
  epochEveryMs: number;
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
  // Scripted client reads and writes (sim only), newest last; their seq is separate.
  reads?: TimelineEvent[];
  net?: NetStats; // sim only
  meta?: { id: string; applied: number; pendingUploads: number };
  // Dedup: bytes committed versions reference vs bytes of distinct chunks.
  referencedBytes?: number;
  distinctBytes?: number;
  epoch?: number; // logical GC epoch
  gc?: GCStats;
  deleted?: DeletedFile[];
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

export interface ScenarioInfo {
  name: string;
  title: string;
  what: string;
}

export interface ClusterAPI {
  readonly kind: 'sim' | 'http';
  readonly label: string;
  // Fault injection from the page: the simulation only.
  readonly canInject: boolean;
  // A scenario runs right after the cluster starts, so a seed replays it.
  start(seed: number, scenario?: string): Promise<void>;
  scenarios(): Promise<ScenarioInfo[]>;
  subscribe(fn: (v: ClusterView) => void): () => void;
  upload(path: string, data: Uint8Array): Promise<Manifest>;
  download(path: string): Promise<Download>;
  stat(path: string): Promise<Manifest>;
  remove(path: string): Promise<void>;
  // Every retained version, oldest first; tombstones included.
  log(path: string): Promise<VersionInfo[]>;
  // Restores a retained version (0: the newest) as a new version.
  undelete(path: string, version: number): Promise<number>;
  // Sim controls; no-ops against a real cluster.
  pause(): void;
  resume(): void;
  step(): void; // advance one simulated second while paused
  setSpeed(simMsPerSec: number): void;
  crash(node: string): void;
  restart(node: string): void;
  freeze(node: string, on: boolean): void;
  slow(node: string, ms: number): void;
  partition(node: string, on: boolean): void;
  corrupt(node: string, chunk: string): Promise<void>;
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
