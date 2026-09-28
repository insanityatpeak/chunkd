// ClusterAPI is the dashboard's only view of a cluster. WasmClusterAPI runs the
// sim in a Web Worker; HttpClusterAPI will talk to a real gateway.

export interface PeerView {
  id: string;
  pings: number;
  lastSeq: number;
  lastSeen: number; // sim nanoseconds
  alive: boolean;
}

export interface NodeView {
  id: string;
  sent: number;
  acked: number;
  lastAckSeq: number;
}

export interface NetStats {
  sent: number;
  delivered: number;
  dropped: number;
  duplicated: number;
}

// Mirrors cluster.State in internal/sim/cluster.
export interface ClusterState {
  seed: number;
  nowMs: number;
  meta: { id: string; peers: PeerView[] | null };
  nodes: NodeView[];
  net: NetStats;
}

export interface ClusterAPI {
  start(seed: number): Promise<void>;
  subscribe(fn: (s: ClusterState) => void): () => void;
  pause(): void;
  resume(): void;
  // Simulated milliseconds advanced per wall-clock second.
  setSpeed(simMsPerSec: number): void;
  dispose(): void;
}
