import type { ClusterState } from './cluster';

// Fixed simulation step. The worker only ever calls tick(STEP_MS), so the
// state sequence depends on the seed alone, never on frame timing.
export const STEP_MS = 50;

export type ToWorker =
  | { type: 'start'; seed: number; baseUrl: string }
  | { type: 'pause' }
  | { type: 'resume' }
  | { type: 'speed'; simMsPerSec: number };

export type FromWorker =
  | { type: 'ready' }
  | { type: 'state'; state: ClusterState }
  | { type: 'error'; message: string };
