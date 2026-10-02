import { useEffect, useState } from 'preact/hooks';
import type { ChunkRef, ClusterAPI, DeletedFile, FileInfo, Manifest, NodeView, Redundancy, VersionInfo } from './api/cluster';
import { formatBytes, sha256Hex } from './verify';

const MAX_UPLOAD = 50 << 20;

interface Props {
  api: ClusterAPI;
  files: FileInfo[];
  // Deleted paths undelete can still restore, and the current GC epoch.
  deleted?: DeletedFile[];
  epoch?: number;
  nodes: NodeView[];
  // Changes when copies are found corrupt or repaired: re-read placement.
  refresh?: string;
  onError?(msg: string): void;
}

interface Verified {
  manifest: Manifest;
  sha256: string;
  ok: boolean;
  url: string;
  name: string;
}

export function Files({ api, files, deleted = [], epoch = 0, nodes, refresh }: Props) {
  const [selected, setSelected] = useState<string | null>(null);
  const [manifest, setManifest] = useState<Manifest | null>(null);
  const [verified, setVerified] = useState<Verified | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [versions, setVersions] = useState<VersionInfo[] | null>(null);
  const [policy, setPolicy] = useState<Redundancy>('');

  // Replicas this page corrupted ("node/chunk"), red until the cluster
  // quarantines them and they leave the placement.
  const [rotted, setRotted] = useState<Set<string>>(new Set());

  const selectedVersion = files.find((f) => f.path === selected)?.version;
  useEffect(() => {
    if (!selected) return;
    api.stat(selected).then(setManifest, (e: Error) => setError(e.message));
  }, [api, selected, selectedVersion, refresh]);

  // Re-read the log when the path changes version or an epoch expires some.
  const deletedVersion = deleted.find((d) => d.path === selected)?.version;
  useEffect(() => {
    setVersions(null);
    if (!selected) return;
    api.log(selected).then(setVersions, () => setVersions(null));
  }, [api, selected, selectedVersion, deletedVersion, epoch]);

  const corrupt = (node: string, chunk: string) =>
    run(`Corrupting ${node}'s copy of chunk ${chunk.slice(0, 8)}`, async () => {
      await api.corrupt(node, chunk);
      setRotted(new Set(rotted).add(`${node}/${chunk}`));
    });

  async function run(label: string, f: () => Promise<void>) {
    setBusy(label);
    setError(null);
    try {
      await f();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(null);
    }
  }

  const onUpload = (e: Event) => {
    const input = e.currentTarget as HTMLInputElement;
    const file = input.files?.[0];
    input.value = '';
    if (!file) return;
    if (file.size > MAX_UPLOAD) {
      setError(`${file.name} is ${formatBytes(file.size)}; the demo keeps files in browser memory, limit 50 MiB`);
      return;
    }
    void run(`Uploading ${file.name}`, async () => {
      const data = new Uint8Array(await file.arrayBuffer());
      const m = await api.upload('/' + file.name, data, policy);
      setSelected(m.path);
      setVerified(null);
    });
  };

  const onDownload = (path: string) =>
    run(`Downloading ${path}`, async () => {
      setSelected(path);
      const { manifest, data } = await api.download(path);
      const sha = await sha256Hex(data);
      if (verified) URL.revokeObjectURL(verified.url);
      setVerified({
        manifest,
        sha256: sha,
        ok: sha === manifest.sha256,
        url: URL.createObjectURL(new Blob([data as BlobPart])),
        name: path.split('/').pop() ?? 'download',
      });
      setManifest(manifest);
    });

  const onRemove = (path: string) =>
    run(`Deleting ${path}`, async () => {
      await api.remove(path);
      if (selected === path) {
        setManifest(null);
        setVerified(null);
      }
    });

  const onUndelete = (path: string, version: number) =>
    run(`Restoring ${path} v${version}`, async () => {
      await api.undelete(path, version);
      setSelected(path);
      setVerified(null);
    });

  return (
    <section class="files" aria-label="Files">
      <div class="files-head">
        <h2>Files</h2>
        <label class="policy">
          Store as{' '}
          <select value={policy} onChange={(e) => setPolicy((e.currentTarget as HTMLSelectElement).value as Redundancy)} disabled={!!busy}>
            <option value="">3 copies (3×)</option>
            <option value="ec-4+2">EC 4+2 (1.5×, needs 6 nodes)</option>
          </select>
        </label>
        <label class="upload">
          <input type="file" onChange={onUpload} disabled={!!busy} />
          <span>Upload a file (up to 50 MiB)</span>
        </label>
      </div>
      {busy && <p class="busy">{busy}…</p>}
      {error && <p class="error">{error}</p>}
      {files.length === 0 ? (
        <p class="muted">
          No files yet. Upload one: it is split into 4 MiB chunks, each stored on 3 nodes in different racks, or with EC 4+2 as 4 data
          and 2 parity shards on 6 nodes.
        </p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Path</th>
              <th>Version</th>
              <th>Size</th>
              <th>Chunks</th>
              <th title="Fewest copies of any chunk on alive nodes, of 3; for EC, fewest shards of any stripe, of 6">Replicas</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {files.map((f) => (
              <tr key={f.path} class={f.path === selected ? 'selected' : ''}>
                <td>
                  <button type="button" class="link-button" onClick={() => { setSelected(f.path); setVerified(null); }}>
                    {f.path}
                  </button>
                </td>
                <td>v{f.version}</td>
                <td>{formatBytes(f.size)}</td>
                <td>{f.chunks}</td>
                <td>
                  <Replicas file={f} />
                </td>
                <td class="actions">
                  <button type="button" onClick={() => onDownload(f.path)} disabled={!!busy}>
                    Download
                  </button>
                  <button type="button" onClick={() => onRemove(f.path)} disabled={!!busy}>
                    Delete
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {deleted.length > 0 && (
        <>
          <h3>Deleted, still restorable</h3>
          <table>
            <thead>
              <tr>
                <th>Path</th>
                <th>Version</th>
                <th>Size</th>
                <th>Expires</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {deleted.map((d) => (
                <tr key={d.path} class={d.path === selected ? 'selected' : ''}>
                  <td>
                    <button type="button" class="link-button" onClick={() => { setSelected(d.path); setVerified(null); }}>
                      {d.path}
                    </button>
                  </td>
                  <td>v{d.version}</td>
                  <td>{formatBytes(d.size)}</td>
                  <td title={`dropped when the GC epoch reaches ${d.expiresEpoch}; now ${epoch}`}>
                    epoch {d.expiresEpoch} ({Math.max(d.expiresEpoch - epoch, 0)} to go)
                  </td>
                  <td class="actions">
                    <button type="button" onClick={() => onUndelete(d.path, d.version)} disabled={!!busy}>
                      Undelete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
      {selected && versions && versions.length > 0 && (
        <Versions path={selected} versions={versions} epoch={epoch} busy={!!busy} onRestore={(v) => onUndelete(selected, v)} />
      )}
      {manifest && manifest.path === selected && (
        <ChunkGrid manifest={manifest} nodes={nodes} rotted={rotted} onCorrupt={api.canInject ? corrupt : undefined} />
      )}
      {verified && verified.manifest.path === selected && (
        <p class={verified.ok ? 'verified' : 'error'}>
          {verified.ok ? 'SHA-256 verified in this browser ✓ ' : 'SHA-256 MISMATCH ✗ '}
          <code>{verified.sha256.slice(0, 16)}…</code> ·{' '}
          <a href={verified.url} download={verified.name}>
            save file
          </a>
        </p>
      )}
    </section>
  );
}

const RF = 3;

interface VersionsProps {
  path: string;
  versions: VersionInfo[];
  epoch: number;
  busy: boolean;
  onRestore(version: number): void;
}

// Versions is a path's retained history, newest first. Any retired real
// version can be restored: undelete copies it as a new version.
function Versions({ path, versions, epoch, busy, onRestore }: VersionsProps) {
  const newest = versions.at(-1)?.version;
  return (
    <figure class="versions">
      <figcaption>
        {path}: {versions.length} retained version{versions.length === 1 ? '' : 's'}. Commits are compare-and-swap on the version.
      </figcaption>
      <ol>
        {[...versions].reverse().map((v) => (
          <li key={v.version}>
            <span class="v">v{v.version}</span>
            {v.deleted ? (
              <span class="muted">delete marker</span>
            ) : (
              <span>
                {formatBytes(v.size)}, {v.chunks} chunk{v.chunks === 1 ? '' : 's'}
                {v.sha256 && <code> {v.sha256.slice(0, 12)}</code>}
              </span>
            )}
            {v.version === newest && !v.deleted && <span class="rep ok">live</span>}
            {v.retired && !v.deleted && (
              <>
                <span class="muted">
                  kept until epoch {v.expiresEpoch} ({Math.max((v.expiresEpoch ?? 0) - epoch, 0)} to go)
                </span>
                <button type="button" onClick={() => onRestore(v.version)} disabled={busy}>
                  Restore
                </button>
              </>
            )}
          </li>
        ))}
      </ol>
    </figure>
  );
}

const SHARDS = 6;
const DATA_SHARDS = 4;

// Replicas is the file's weakest chunk: RF copies is healthy, fewer is
// under-replicated (repair pending), none alive means reads use suspect
// nodes or fail. For an EC file it counts shards: 6 is healthy, 4 or 5
// still reads (decoding from parity), below 4 is unreadable.
function Replicas({ file }: { file: FileInfo }) {
  if (file.minLive === undefined) return <span class="muted">–</span>;
  if (file.chunks === 0) return <span class="rep ok">empty</span>;
  const ec = file.redundancy === 'ec-4+2';
  const target = ec ? SHARDS : RF;
  const floor = ec ? DATA_SHARDS : 1;
  const cls = file.minLive < floor ? 'lost' : file.minLive < target ? 'under' : 'ok';
  const unit = ec ? 'shards of a stripe' : 'copies';
  const detail = file.underReplicated ? `, ${file.underReplicated} of ${file.chunks} chunks below ${target}` : '';
  return (
    <span class={`rep ${cls}`} title={`fewest alive ${unit} ${file.minLive}${detail}`}>
      {file.minLive}/{target}
      {ec && ' EC'}
      {file.underReplicated ? ` · ${file.underReplicated} low` : ''}
    </span>
  );
}

interface Held {
  id: string; // the block on the node: the chunk, or one shard
  has: boolean; // stored there now; a rejected one may already be gone
  shard?: number;
  parity?: boolean;
  served: boolean;
  bad: boolean;
}

// holding is what node stores of chunk c: a copy, a shard (EC), or nothing.
// A rejected copy or shard still counts, so its cross shows.
function holding(c: ChunkRef, node: string): Held | null {
  for (const s of c.shards ?? []) {
    const has = (s.replicas ?? []).includes(node);
    const bad = (s.rejected ?? []).includes(node);
    if (has || bad) {
      return { id: s.id, has, shard: s.index, parity: s.index >= DATA_SHARDS, served: s.servedBy === node, bad };
    }
  }
  const has = (c.replicas ?? []).includes(node);
  const bad = (c.rejected ?? []).includes(node);
  if (!has && !bad) return null;
  return { id: c.id, has, served: c.servedBy === node, bad };
}

interface GridProps {
  manifest: Manifest;
  nodes: NodeView[];
  rotted: Set<string>;
  onCorrupt?(node: string, chunk: string): void;
}

// ChunkGrid shows which node holds each replica of each chunk. After a
// download it marks the replica that served each chunk and any replica whose
// data failed verification. In the simulation a filled cell can be clicked
// to flip bytes in that copy.
function ChunkGrid({ manifest, nodes, rotted, onCorrupt }: GridProps) {
  const chunks = manifest.chunkList ?? [];
  if (chunks.length === 0) return <p class="muted">{manifest.path} is empty: no chunks.</p>;
  const cell = 22;
  const labelW = 64;
  const headH = 40;
  const w = labelW + nodes.length * cell;
  const h = headH + chunks.length * cell;
  return (
    <figure class="grid">
      <figcaption>
        {manifest.path} v{manifest.version}: {chunks.length} chunks × replicas. Filled = holds the chunk; ring = served the
        last download; cross = failed verification; red = corrupted here, not yet found.
        {chunks.some((c) => (c.shards ?? []).length > 0) &&
          ' EC: a cell shows the shard the node holds, 0-3 data and 4-5 parity; any 4 rebuild the chunk.'}
        {chunks.some((c) => c.decoded) && ' * = decoded from parity on the last download.'}
        {onCorrupt && ' Click a filled cell to corrupt that copy.'}
      </figcaption>
      <svg viewBox={`0 0 ${w} ${h}`} style={{ maxWidth: `${w * 1.4}px` }} role={onCorrupt ? "group" : "img"} aria-label="Chunk placement grid">
        {nodes.map((n, j) => (
          <text key={n.id} class="grid-head" x={labelW + j * cell + cell / 2} y={headH - 8} text-anchor="middle">
            {n.id.replace('node-', 'n')}
          </text>
        ))}
        {chunks.map((c, i) => (
          <g key={c.index}>
            <text class="grid-label" x={labelW - 8} y={headH + i * cell + cell / 2 + 4} text-anchor="end">
              #{c.index}
              {c.decoded && '*'}
            </text>
            {nodes.map((n, j) => {
              const held = holding(c, n.id);
              const has = held?.has ?? false;
              const rot = has && rotted.has(`${n.id}/${held!.id}`);
              const click = has && onCorrupt && !rot ? () => onCorrupt(n.id, held!.id) : undefined;
              const what = held?.shard === undefined ? 'copy' : `shard ${held.shard}`;
              const x = labelW + j * cell;
              const y = headH + i * cell;
              return (
                <g
                  key={n.id}
                  class={click ? 'clickable' : undefined}
                  role={click ? 'button' : undefined}
                  tabIndex={click ? 0 : undefined}
                  aria-label={click ? `Corrupt ${n.id}'s ${what} of chunk ${c.index}` : undefined}
                  onClick={click}
                  onKeyDown={click ? (e) => (e.key === 'Enter' || e.key === ' ') && click() : undefined}
                >
                  <rect
                    class={`grid-cell ${has ? 'has' : ''} ${has && held?.parity ? 'parity' : ''} ${rot ? 'rot' : ''}`}
                    x={x + 2}
                    y={y + 2}
                    width={cell - 4}
                    height={cell - 4}
                    rx="3"
                  />
                  {has && held?.shard !== undefined && (
                    <text class="grid-shard" x={x + cell / 2} y={y + cell / 2 + 4} text-anchor="middle">
                      {held.shard}
                    </text>
                  )}
                  {held?.served && <circle class="served" cx={x + cell / 2} cy={y + cell / 2} r={cell / 2 - 1} />}
                  {held?.bad && <path class="bad" d={`M${x + 5} ${y + 5}L${x + cell - 5} ${y + cell - 5}M${x + cell - 5} ${y + 5}L${x + 5} ${y + cell - 5}`} />}
                </g>
              );
            })}
          </g>
        ))}
      </svg>
    </figure>
  );
}
