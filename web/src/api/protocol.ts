import type { ClusterView } from './cluster';

// Fixed simulation step. The worker only ever calls tick(STEP_MS), so the
// state sequence depends on the seed alone, never on frame timing.
export const STEP_MS = 50;

export type Method =
  | 'upload'
  | 'download'
  | 'stat'
  | 'remove'
  | 'crash'
  | 'restart'
  | 'freeze'
  | 'slow'
  | 'partition'
  | 'corrupt'
  | 'scenarios';

export type ToWorker =
  | { type: 'start'; seed: number; baseUrl: string; scenario?: string }
  | { type: 'pause' }
  | { type: 'step' }
  | { type: 'resume' }
  | { type: 'speed'; simMsPerSec: number }
  | { type: 'call'; id: number; method: Method; args: unknown[] };

export type FromWorker =
  | { type: 'ready' }
  | { type: 'state'; state: ClusterView }
  | { type: 'error'; message: string }
  | { type: 'result'; id: number; value?: unknown; error?: { message: string; code: string } };
