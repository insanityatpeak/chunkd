import type { ClusterAPI, ClusterState } from './cluster';
import type { FromWorker, ToWorker } from './protocol';

// WasmClusterAPI runs the simulated cluster in a Web Worker so ticking never
// blocks the UI thread.
export class WasmClusterAPI implements ClusterAPI {
  private worker = new Worker(new URL('../worker.ts', import.meta.url), { type: 'module' });
  private subs = new Set<(s: ClusterState) => void>();
  private ready?: { resolve: () => void; reject: (e: Error) => void };

  constructor() {
    this.worker.onmessage = (e: MessageEvent<FromWorker>) => {
      const m = e.data;
      switch (m.type) {
        case 'ready':
          this.ready?.resolve();
          break;
        case 'state':
          this.subs.forEach((fn) => fn(m.state));
          break;
        case 'error':
          this.ready?.reject(new Error(m.message));
          break;
      }
    };
  }

  start(seed: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this.ready = { resolve, reject };
      this.send({ type: 'start', seed, baseUrl: import.meta.env.BASE_URL });
    });
  }

  subscribe(fn: (s: ClusterState) => void): () => void {
    this.subs.add(fn);
    return () => this.subs.delete(fn);
  }

  pause() {
    this.send({ type: 'pause' });
  }

  resume() {
    this.send({ type: 'resume' });
  }

  setSpeed(simMsPerSec: number) {
    this.send({ type: 'speed', simMsPerSec });
  }

  dispose() {
    this.worker.terminate();
    this.subs.clear();
  }

  private send(m: ToWorker) {
    this.worker.postMessage(m);
  }
}
