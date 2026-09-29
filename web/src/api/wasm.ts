import { ChunkdError, type ClusterAPI, type ClusterView, type Download, type Manifest } from './cluster';
import type { FromWorker, Method, ToWorker } from './protocol';

// WasmClusterAPI runs the simulated cluster in a Web Worker so ticking and
// hashing never block the UI thread.
export class WasmClusterAPI implements ClusterAPI {
  readonly kind = 'sim' as const;
  readonly label = 'Simulated cluster in this browser';
  private worker = new Worker(new URL('../worker.ts', import.meta.url), { type: 'module' });
  private subs = new Set<(v: ClusterView) => void>();
  private ready?: { resolve: () => void; reject: (e: Error) => void };
  private nextId = 1;
  private pending = new Map<number, { resolve: (v: unknown) => void; reject: (e: Error) => void }>();

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
        case 'result': {
          const p = this.pending.get(m.id);
          this.pending.delete(m.id);
          if (m.error) p?.reject(new ChunkdError(m.error.message, m.error.code));
          else p?.resolve(m.value);
          break;
        }
      }
    };
  }

  start(seed: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this.ready = { resolve, reject };
      this.send({ type: 'start', seed, baseUrl: import.meta.env.BASE_URL });
    });
  }

  subscribe(fn: (v: ClusterView) => void): () => void {
    this.subs.add(fn);
    return () => this.subs.delete(fn);
  }

  upload(path: string, data: Uint8Array): Promise<Manifest> {
    return this.call('upload', [path, data]) as Promise<Manifest>;
  }

  download(path: string): Promise<Download> {
    return this.call('download', [path]) as Promise<Download>;
  }

  stat(path: string): Promise<Manifest> {
    return this.call('stat', [path]) as Promise<Manifest>;
  }

  async remove(path: string): Promise<void> {
    await this.call('remove', [path]);
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

  crash(node: string) {
    void this.call('crash', [node]);
  }

  restart(node: string) {
    void this.call('restart', [node]);
  }

  async scenario(node: string, downMs: number): Promise<void> {
    await this.call('scenario', [node, downMs]);
  }

  async rot(node: string, n: number): Promise<number> {
    const r = (await this.call('rot', [node, n])) as { rotted: number };
    return r.rotted;
  }

  dispose() {
    this.worker.terminate();
    this.subs.clear();
    this.pending.forEach((p) => p.reject(new Error('disposed')));
    this.pending.clear();
  }

  private call(method: Method, args: unknown[]): Promise<unknown> {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.send({ type: 'call', id, method, args });
    });
  }

  private send(m: ToWorker) {
    this.worker.postMessage(m);
  }
}
