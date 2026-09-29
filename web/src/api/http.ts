import {
  ChunkdError,
  Timeline,
  type ClusterAPI,
  type ClusterView,
  type Download,
  type FileInfo,
  type Health,
  type Manifest,
  type NodeView,
  type RepairCopy,
  type TimelineEvent,
} from './cluster';
import { sha256Hex } from '../verify';

const POLL_MS = 1000;

// GatewayCluster is GET /cluster (client.Cluster in Go).
interface GatewayCluster {
  nowMs: number;
  nodes: NodeView[] | null;
  health: Health;
  fileHealth: { path: string; underReplicated: number; minLive: number }[] | null;
  copies: RepairCopy[] | null;
  events: TimelineEvent[] | null;
  eventSeq: number;
}

// HttpClusterAPI talks to a real gateway. It does not trust the gateway: a
// download is checked chunk by chunk against the manifest's hashes, and the
// whole file against its SHA-256, with WebCrypto.
export class HttpClusterAPI implements ClusterAPI {
  readonly kind = 'http' as const;
  readonly label: string;
  private subs = new Set<(v: ClusterView) => void>();
  private timer?: ReturnType<typeof setInterval>;
  private timeline = new Timeline();

  constructor(private readonly base: string) {
    this.base = base.replace(/\/+$/, '');
    this.label = `Gateway at ${this.base}`;
  }

  async start(_seed: number): Promise<void> {
    await this.poll();
    this.timer = setInterval(() => void this.poll().catch(() => {}), POLL_MS);
  }

  subscribe(fn: (v: ClusterView) => void): () => void {
    this.subs.add(fn);
    return () => this.subs.delete(fn);
  }

  private async poll() {
    const [cluster, files] = await Promise.all([
      this.json<GatewayCluster>(`/cluster?events_after=${this.timeline.seq}`),
      this.json<FileInfo[] | null>('/files?prefix=/'),
    ]);
    const health = new Map((cluster.fileHealth ?? []).map((f) => [f.path, f]));
    const view: ClusterView = {
      nowMs: cluster.nowMs,
      nodes: cluster.nodes ?? [],
      files: (files ?? []).map((f) => ({ ...f, ...health.get(f.path), path: f.path })),
      health: cluster.health,
      copies: cluster.copies ?? [],
      timeline: this.timeline.merge(cluster.events, cluster.eventSeq),
    };
    this.subs.forEach((fn) => fn(view));
  }

  private async json<T>(path: string, init?: RequestInit): Promise<T> {
    let resp: Response;
    try {
      resp = await fetch(this.base + path, init);
    } catch (e) {
      throw new ChunkdError(`gateway unreachable: ${(e as Error).message}`, 'unavailable');
    }
    if (!resp.ok) throw await toError(resp);
    return (await resp.json()) as T;
  }

  upload(path: string, data: Uint8Array): Promise<Manifest> {
    return this.json<Manifest>(`/files${encodePath(path)}`, { method: 'POST', body: data as BodyInit });
  }

  stat(path: string): Promise<Manifest> {
    return this.json<Manifest>(`/files${encodePath(path)}?manifest=1`);
  }

  async download(path: string): Promise<Download> {
    const manifest = await this.stat(path);
    const resp = await fetch(`${this.base}/files${encodePath(path)}`);
    if (!resp.ok) throw await toError(resp);
    if (resp.headers.get('X-Chunkd-Version') !== String(manifest.version)) {
      throw new ChunkdError('file changed between manifest and download; retry', 'conflict');
    }
    const data = new Uint8Array(await resp.arrayBuffer());
    let off = 0;
    for (const c of manifest.chunkList ?? []) {
      const got = await sha256Hex(data.subarray(off, off + c.size));
      if (got !== c.id) throw new ChunkdError(`chunk ${c.index}: data does not match its hash`, 'internal');
      off += c.size;
    }
    if (off !== data.length) throw new ChunkdError(`${data.length - off} bytes beyond the manifest`, 'internal');
    return { manifest, data };
  }

  async remove(path: string): Promise<void> {
    await this.json(`/files${encodePath(path)}`, { method: 'DELETE' });
  }

  pause() {}
  resume() {}
  setSpeed(_simMsPerSec: number) {}
  crash(_node: string) {}
  restart(_node: string) {}
  scenario(_node: string, _downMs: number): Promise<void> {
    return Promise.reject(new ChunkdError('scripted faults run in the simulation only; use docker compose kill', 'unimplemented'));
  }

  dispose() {
    clearInterval(this.timer);
    this.subs.clear();
  }
}

function encodePath(p: string) {
  return p.split('/').map(encodeURIComponent).join('/');
}

async function toError(resp: Response): Promise<ChunkdError> {
  try {
    const body = (await resp.json()) as { code: string; error: string };
    return new ChunkdError(body.error, body.code);
  } catch {
    return new ChunkdError(`${resp.status} ${resp.statusText}`, 'internal');
  }
}
