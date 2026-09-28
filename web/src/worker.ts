/// <reference lib="webworker" />
import { STEP_MS, type FromWorker, type ToWorker } from './api/protocol';
import type { ClusterState } from './api/cluster';

declare const self: DedicatedWorkerGlobalScope;

interface GoRuntime {
  importObject: WebAssembly.Imports;
  run(instance: WebAssembly.Instance): Promise<void>;
}

interface ChunkdGlobal {
  start(seed: number): void;
  tick(ms: number): void;
  state(): string | null;
}

const g = globalThis as unknown as { Go: new () => GoRuntime; chunkd?: ChunkdGlobal };

const FRAME_MS = 50;
let simMsPerSec = 1000;
let running = false;
let timer: ReturnType<typeof setInterval> | undefined;
let owed = 0; // fractional steps carried between frames

function post(m: FromWorker) {
  self.postMessage(m);
}

async function load(baseUrl: string) {
  // wasm_exec.js is copied from the Go toolchain that built cluster.wasm.
  await import(/* @vite-ignore */ `${baseUrl}wasm_exec.js`);
  const go = new g.Go();
  const url = `${baseUrl}cluster.wasm`;
  let instance: WebAssembly.Instance;
  try {
    ({ instance } = await WebAssembly.instantiateStreaming(fetch(url), go.importObject));
  } catch {
    // Some servers send the wrong MIME type; fall back to a buffered compile.
    const buf = await (await fetch(url)).arrayBuffer();
    ({ instance } = await WebAssembly.instantiate(buf, go.importObject));
  }
  void go.run(instance);
  if (!g.chunkd) throw new Error('cluster.wasm did not register the chunkd API');
}

function frame() {
  const api = g.chunkd;
  if (!api || !running) return;
  owed += (simMsPerSec * FRAME_MS) / 1000 / STEP_MS;
  const steps = Math.floor(owed);
  owed -= steps;
  for (let i = 0; i < steps; i++) api.tick(STEP_MS);
  if (steps > 0) publish();
}

function publish() {
  const raw = g.chunkd?.state();
  if (raw) post({ type: 'state', state: JSON.parse(raw) as ClusterState });
}

self.onmessage = async (e: MessageEvent<ToWorker>) => {
  const m = e.data;
  try {
    switch (m.type) {
      case 'start':
        if (!g.chunkd) await load(m.baseUrl);
        g.chunkd!.start(m.seed);
        owed = 0;
        running = true;
        timer ??= setInterval(frame, FRAME_MS);
        post({ type: 'ready' });
        publish();
        break;
      case 'pause':
        running = false;
        break;
      case 'resume':
        running = true;
        break;
      case 'speed':
        simMsPerSec = m.simMsPerSec;
        break;
    }
  } catch (err) {
    post({ type: 'error', message: err instanceof Error ? err.message : String(err) });
  }
};
