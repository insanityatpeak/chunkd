import {
  ChunkdError,
  TIMELINE_MAX,
  type ClusterAPI,
  type ClusterView,
  type Download,
  type FileInfo,
  type Health,
  type Manifest,
  type NodeView,
  type RepairCopy,
  type ScenarioInfo,
  type TimelineEvent,
  type VersionInfo,
  type GCStats,
  type DeletedFile,
  type MetaPeerView,
  type MetaState,
  type Redundancy,
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
  referencedBytes?: number;
  distinctBytes?: number;
  epoch?: number;
  gc?: GCStats;
  deleted?: DeletedFile[] | null;
  // The answering metadata peer's view of the group (meta.proto ClusterResponse).
  metaLeader?: string;
  metaTerm?: number;
  metaPeer?: string;
  metaRole?: string;
  metaCommit?: number;
  metaApplied?: number;
  metaPeers?: { id: string; match: number; heardAgoMs: number }[] | null;
}

// A follower the leader has not heard from for this long shows as
// unreachable: 3 election timeouts.
const UNREACHABLE_MS = 3000;

// metasFrom builds the group view from one peer's answer. A leader reports
// every voter; a follower only itself and the leader it knows.
export function metasFrom(c: GatewayCluster): MetaPeerView[] | undefined {
  if (!c.metaPeer) return undefined;
  const out: MetaPeerView[] = [];
  for (const p of c.metaPeers ?? []) {
    if (p.id === c.metaPeer) {
      const role = (c.metaRole || 'unknown') as MetaState;
      out.push({ id: p.id, state: role, leader: role === 'leader' && c.metaLeader === p.id, term: c.metaTerm, commit: c.metaCommit, applied: c.metaApplied, match: p.match });
    } else {
      const lost = p.heardAgoMs < 0 || p.heardAgoMs > UNREACHABLE_MS;
      out.push({ id: p.id, state: lost ? 'unreachable' : 'follower', leader: false, match: p.match });
    }
  }
  if (c.metaLeader && !out.some((p) => p.id === c.metaLeader)) {
    out.push({ id: c.metaLeader, state: 'leader', leader: true, term: c.metaTerm });
  }
  return out.sort((a, b) => a.id.localeCompare(b.id, undefined, { numeric: true }));
}

// LiveTimeline follows the events of whichever metadata peer answers. Each
// peer numbers its events from 1 and keeps time from its own start, so on a
// switch the kept events move onto the new peer's clock (by the gap between
// the clocks, net of the wall time between polls), the next poll asks for
// the new peer's whole ring, and only its events after the switch are kept
// (raft events excepted, below).
// Events are renumbered so keys stay unique across peers.
export class LiveTimeline {
  private events: TimelineEvent[] = [];
  private peer?: string;
  private lastNow = 0;
  private lastWall = 0;
  private cutoff = -Infinity;
  private local = 0;
  private raft = new Set<string>();
  seq = 0; // the answering peer's latest seq, for events_after

  merge(peer: string | undefined, nowMs: number, events: TimelineEvent[] | null, latest: number): TimelineEvent[] {
    const wall = performance.now();
    const key = peer ?? '';
    // A restarted peer is a new one too: a new sequence on a new clock.
    if (this.peer !== undefined && (key !== this.peer || latest < this.seq)) {
      const shift = nowMs - (this.lastNow + (wall - this.lastWall));
      this.events = this.events.map((e) => ({ ...e, atMs: e.atMs + shift }));
      this.cutoff = nowMs;
      this.seq = 0; // these events were fetched with the old peer's seq
    } else {
      // Raft events pass the cutoff: the new peer's election happened before
      // the gateway turned to it. Every peer logs "term N: …", so a raft
      // event is kept once per node and text.
      const fresh = (events ?? []).filter((e) => {
        if (e.seq <= this.seq) return false;
        if (e.kind !== 'raft') return e.atMs > this.cutoff;
        const k = `${e.node}\u0000${e.text}`;
        if (this.raft.has(k)) return false;
        this.raft.add(k);
        return true;
      });
      for (const e of fresh) this.events.push({ ...e, seq: ++this.local });
      this.events.sort((a, b) => a.atMs - b.atMs);
      this.events = this.events.slice(-TIMELINE_MAX);
      this.seq = latest;
    }
    this.peer = key;
    this.lastNow = nowMs;
    this.lastWall = wall;
    return this.events;
  }
}

// HttpClusterAPI talks to a real gateway. It does not trust the gateway: a
// download is checked chunk by chunk against the manifest's hashes, and the
// whole file against its SHA-256, with WebCrypto.
export class HttpClusterAPI implements ClusterAPI {
  readonly kind = 'http' as const;
  readonly label: string;
  readonly canInject = false;
  private subs = new Set<(v: ClusterView) => void>();
  private timer?: ReturnType<typeof setInterval>;
  private timeline = new LiveTimeline();

  constructor(private readonly base: string) {
    this.base = base.replace(/\/+$/, '');
    this.label = `Gateway at ${this.base}`;
  }

  async start(_seed: number, _scenario?: string): Promise<void> {
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
      timeline: this.timeline.merge(cluster.metaPeer, cluster.nowMs, cluster.events, cluster.eventSeq),
      metas: metasFrom(cluster),
      referencedBytes: cluster.referencedBytes,
      distinctBytes: cluster.distinctBytes,
      epoch: cluster.epoch,
      gc: cluster.gc,
      deleted: cluster.deleted ?? [],
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

  upload(path: string, data: Uint8Array, redundancy: Redundancy = ''): Promise<Manifest> {
    const q = redundancy ? `?redundancy=${encodeURIComponent(redundancy)}` : '';
    return this.json<Manifest>(`/files${encodePath(path)}${q}`, { method: 'POST', body: data as BodyInit });
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

  async log(path: string): Promise<VersionInfo[]> {
    return (await this.json<VersionInfo[] | null>(`/files${encodePath(path)}?log=1`)) ?? [];
  }

  async undelete(path: string, version: number): Promise<number> {
    const q = version > 0 ? `?version=${version}` : '';
    return (await this.json<{ version: number }>(`/undelete${encodePath(path)}${q}`, { method: 'POST' })).version;
  }

  scenarios(): Promise<ScenarioInfo[]> {
    return Promise.resolve([]);
  }

  pause() {}
  resume() {}
  step() {}
  freeze(_node: string, _on: boolean) {}
  slow(_node: string, _ms: number) {}
  partition(_node: string, _on: boolean) {}
  corrupt(_node: string, _chunk: string): Promise<void> {
    return Promise.reject(new ChunkdError('faults are injected with docker compose against a real cluster', 'unimplemented'));
  }
  setSpeed(_simMsPerSec: number) {}
  crash(_node: string) {}
  restart(_node: string) {}
  killMeta(_id: string): Promise<void> {
    return this.corrupt('', '');
  }
  reviveMeta(_id: string): Promise<void> {
    return this.corrupt('', '');
  }
  cutMeta(_id: string, _on: boolean): Promise<void> {
    return this.corrupt('', '');
  }
  // Membership changes on a real cluster go through the CLI (chunkd node).
  addNode(): Promise<void> {
    return this.corrupt('', '');
  }
  drain(_node: string, _on: boolean): Promise<void> {
    return this.corrupt('', '');
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
