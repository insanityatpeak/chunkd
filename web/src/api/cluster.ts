// ClusterAPI is the dashboard's only view of a cluster. WasmClusterAPI runs
// the simulated cluster in a Web Worker; HttpClusterAPI talks to a real
// gateway (docker compose up). Shapes mirror internal/client in Go.

export interface FileInfo {
  path: string;
  version: number;
  size: number;
  sha256: string;
  chunks: number;
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

export interface NodeView {
  id: string;
  rack: string;
  alive: boolean;
  crashed?: boolean;
  usedBytes: number;
  chunks: number;
  heartbeats?: number;
}

export interface NetStats {
  sent: number;
  delivered: number;
  dropped: number;
  duplicated: number;
  bytes: number;
}

export interface ClusterView {
  nowMs?: number; // sim only
  nodes: NodeView[];
  files: FileInfo[];
  net?: NetStats; // sim only
  meta?: { id: string; applied: number; pendingUploads: number };
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
