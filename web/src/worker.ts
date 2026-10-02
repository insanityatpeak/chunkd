/// <reference lib="webworker" />
import { STEP_MS, type FromWorker, type Method, type ToWorker } from './api/protocol';
import { Timeline, type ClusterView, type TimelineEvent } from './api/cluster';

declare const self: DedicatedWorkerGlobalScope;

interface GoRuntime {
  importObject: WebAssembly.Imports;
  run(instance: WebAssembly.Instance): Promise<void>;
}

// The Go side (cmd/chunkd-wasm). Calls that can fail return JSON, with
// {"error": ..., "code": ...} on failure.
interface ChunkdGlobal {
  start(seed: number): void;
  tick(ms: number): void;
  state(afterSeq: number): string;
  upload(path: string, data: Uint8Array): string;
  download(path: string): { manifest: string; data?: Uint8Array };
  stat(path: string): string;
  remove(path: string): string;
  log(path: string): string;
  undelete(path: string, version: number): string;
  crash(node: string): void;
  restart(node: string): void;
  freeze(node: string, on: boolean): void;
  slow(node: string, ms: number): void;
  partition(node: string, on: boolean): void;
  corrupt(node: string, chunk: string): string;
  killMeta(id: string): string;
  reviveMeta(id: string): string;
  cutMeta(id: string, on: boolean): string;
  scenarios(): string;
  runScenario(name: string): string;
}

// SimState is cluster.State in Go; events arrive incrementally by seq.
type SimState = Omit<ClusterView, 'timeline'> & { events: TimelineEvent[] | null; eventSeq: number };

const g = globalThis as unknown as { Go: new () => GoRuntime; chunkd?: ChunkdGlobal };

const FRAME_MS = 50;
let simMsPerSec = 1000;
let running = false;
let timer: ReturnType<typeof setInterval> | undefined;
let owed = 0; // fractional steps carried between frames
const timeline = new Timeline();

function post(m: FromWorker, transfer: Transferable[] = []) {
  self.postMessage(m, transfer);
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

function tick(steps: number) {
  const api = g.chunkd;
  if (!api) return;
  for (let i = 0; i < steps; i++) api.tick(STEP_MS);
  if (steps > 0) publish();
}

function frame() {
  if (!running) return;
  owed += (simMsPerSec * FRAME_MS) / 1000 / STEP_MS;
  const steps = Math.floor(owed);
  owed -= steps;
  tick(steps);
}

function publish() {
  const raw = g.chunkd?.state(timeline.seq);
  if (!raw) return;
  const { events, eventSeq, ...rest } = JSON.parse(raw) as SimState;
  post({ type: 'state', state: { ...rest, timeline: timeline.merge(events, eventSeq) } });
}

// Go returns JSON; errors arrive as {"error": ..., "code": ...}.
function parse(raw: string): unknown {
  const v = JSON.parse(raw) as { error?: string; code?: string };
  if (v && typeof v === 'object' && 'error' in v && v.error) throw { message: v.error, code: v.code ?? 'unknown' };
  return v;
}

// Calls run the client synchronously in Go; simulated time advances by the
// transfer time while they run, and the dashboard sees the jump.
function call(method: Method, args: unknown[]): { value: unknown; transfer: Transferable[] } {
  const api = g.chunkd!;
  const [a, b] = args as [string, unknown];
  const none = { value: null, transfer: [] };
  switch (method) {
    case 'upload':
      return { value: parse(api.upload(a, b as Uint8Array)), transfer: [] };
    case 'download': {
      const r = api.download(a);
      const manifest = parse(r.manifest);
      return { value: { manifest, data: r.data }, transfer: r.data ? [r.data.buffer] : [] };
    }
    case 'stat':
      return { value: parse(api.stat(a)), transfer: [] };
    case 'remove':
      return { value: parse(api.remove(a)), transfer: [] };
    case 'log':
      return { value: parse(api.log(a)), transfer: [] };
    case 'undelete':
      return { value: parse(api.undelete(a, b as number)), transfer: [] };
    case 'crash':
      api.crash(a);
      return none;
    case 'restart':
      api.restart(a);
      return none;
    case 'freeze':
      api.freeze(a, b as boolean);
      return none;
    case 'slow':
      api.slow(a, b as number);
      return none;
    case 'partition':
      api.partition(a, b as boolean);
      return none;
    case 'corrupt':
      return { value: parse(api.corrupt(a, b as string)), transfer: [] };
    case 'killMeta':
      return { value: parse(api.killMeta(a)), transfer: [] };
    case 'reviveMeta':
      return { value: parse(api.reviveMeta(a)), transfer: [] };
    case 'cutMeta':
      return { value: parse(api.cutMeta(a, b as boolean)), transfer: [] };
    case 'scenarios':
      return { value: parse(api.scenarios()), transfer: [] };
  }
}

self.onmessage = async (e: MessageEvent<ToWorker>) => {
  const m = e.data;
  try {
    switch (m.type) {
      case 'start':
        if (!g.chunkd) await load(m.baseUrl);
        g.chunkd!.start(m.seed);
        owed = 0;
        timeline.reset();
        // Before the first tick, as in the Go tests: a seed replays it.
        if (m.scenario) parse(g.chunkd!.runScenario(m.scenario));
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
      case 'step':
        tick(1000 / STEP_MS);
        break;
      case 'speed':
        simMsPerSec = m.simMsPerSec;
        break;
      case 'call':
        try {
          const { value, transfer } = call(m.method, m.args);
          post({ type: 'result', id: m.id, value }, transfer);
        } catch (err) {
          const e = err as { message?: string; code?: string };
          post({ type: 'result', id: m.id, error: { message: e.message ?? String(err), code: e.code ?? 'unknown' } });
        }
        publish();
        break;
    }
  } catch (err) {
    const e = err as { message?: string };
    post({ type: 'error', message: e.message ?? String(err) });
  }
};
