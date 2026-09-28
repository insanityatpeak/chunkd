import { useEffect, useState } from 'preact/hooks';
import type { ClusterAPI, FileInfo, Manifest, NodeView } from './api/cluster';
import { formatBytes, sha256Hex } from './verify';

const MAX_UPLOAD = 50 << 20;

interface Props {
  api: ClusterAPI;
  files: FileInfo[];
  nodes: NodeView[];
}

interface Verified {
  manifest: Manifest;
  sha256: string;
  ok: boolean;
  url: string;
  name: string;
}

export function Files({ api, files, nodes }: Props) {
  const [selected, setSelected] = useState<string | null>(null);
  const [manifest, setManifest] = useState<Manifest | null>(null);
  const [verified, setVerified] = useState<Verified | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const selectedVersion = files.find((f) => f.path === selected)?.version;
  useEffect(() => {
    if (!selected) return;
    api.stat(selected).then(setManifest, (e: Error) => setError(e.message));
  }, [api, selected, selectedVersion]);

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
      const m = await api.upload('/' + file.name, data);
      setSelected(m.path);
      setVerified(null);
    });
  };

  const onDownload = (path: string) =>
    run(`Downloading ${path}`, async () => {
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
        setSelected(null);
        setManifest(null);
        setVerified(null);
      }
    });

  return (
    <section class="files" aria-label="Files">
      <div class="files-head">
        <h2>Files</h2>
        <label class="upload">
          <input type="file" onChange={onUpload} disabled={!!busy} />
          <span>Upload a file (up to 50 MiB)</span>
        </label>
      </div>
      {busy && <p class="busy">{busy}…</p>}
      {error && <p class="error">{error}</p>}
      {files.length === 0 ? (
        <p class="muted">No files yet. Upload one: it is split into 4 MiB chunks, each stored on 3 nodes in different racks.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Path</th>
              <th>Version</th>
              <th>Size</th>
              <th>Chunks</th>
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
      {manifest && manifest.path === selected && <ChunkGrid manifest={manifest} nodes={nodes} />}
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

// ChunkGrid shows which node holds each replica of each chunk. After a
// download it marks the replica that served each chunk and any replica whose
// data failed verification.
function ChunkGrid({ manifest, nodes }: { manifest: Manifest; nodes: NodeView[] }) {
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
        last download; cross = failed verification.
      </figcaption>
      <svg viewBox={`0 0 ${w} ${h}`} style={{ maxWidth: `${w * 1.4}px` }} role="img" aria-label="Chunk placement grid">
        {nodes.map((n, j) => (
          <text key={n.id} class="grid-head" x={labelW + j * cell + cell / 2} y={headH - 8} text-anchor="middle">
            {n.id.replace('node-', 'n')}
          </text>
        ))}
        {chunks.map((c, i) => (
          <g key={c.index}>
            <text class="grid-label" x={labelW - 8} y={headH + i * cell + cell / 2 + 4} text-anchor="end">
              #{c.index}
            </text>
            {nodes.map((n, j) => {
              const has = (c.replicas ?? []).includes(n.id);
              const served = c.servedBy === n.id;
              const bad = (c.rejected ?? []).includes(n.id);
              const x = labelW + j * cell;
              const y = headH + i * cell;
              return (
                <g key={n.id}>
                  <rect class={`grid-cell ${has ? 'has' : ''}`} x={x + 2} y={y + 2} width={cell - 4} height={cell - 4} rx="3" />
                  {served && <circle class="served" cx={x + cell / 2} cy={y + cell / 2} r={cell / 2 - 1} />}
                  {bad && <path class="bad" d={`M${x + 5} ${y + 5}L${x + cell - 5} ${y + cell - 5}M${x + cell - 5} ${y + 5}L${x + 5} ${y + cell - 5}`} />}
                </g>
              );
            })}
          </g>
        ))}
      </svg>
    </figure>
  );
}
