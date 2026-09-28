import type { ClusterView as View, NodeView } from './api/cluster';
import { formatBytes } from './verify';

const W = 760;
const META_W = 200;
const NODE_W = 132;
const NODE_H = 96;
const META_Y = 12;
const NODE_Y = 150;

interface Props {
  view: View;
  sim: boolean;
  onToggle(node: NodeView): void;
}

// ClusterView draws the metadata server above the storage nodes. A link is
// solid while the metadata server hears the node's heartbeats and dashed
// once it stops.
export function ClusterView({ view, sim, onToggle }: Props) {
  const nodes = view.nodes;
  const slot = W / Math.max(nodes.length, 1);
  const nodeX = (i: number) => slot * i + (slot - NODE_W) / 2;
  const metaX = (W - META_W) / 2;
  const maxUsed = Math.max(1, ...nodes.map((n) => n.usedBytes));

  return (
    <figure class="cluster">
      <svg viewBox={`0 0 ${W} ${NODE_Y + NODE_H + 34}`} role="img" aria-label="Cluster topology">
        {nodes.map((n, i) => (
          <line
            key={`l-${n.id}`}
            class={`link ${status(n)}`}
            x1={metaX + META_W / 2}
            y1={META_Y + 64}
            x2={nodeX(i) + NODE_W / 2}
            y2={NODE_Y}
          />
        ))}
        <g transform={`translate(${metaX} ${META_Y})`}>
          <rect class="box meta" width={META_W} height={64} rx="8" />
          <text class="title" x="12" y="24">
            {view.meta?.id ?? 'metadata server'}
          </text>
          <text class="sub" x="12" y="46">
            {view.files.length} files
            {view.meta ? ` · log index ${view.meta.applied}` : ''}
          </text>
        </g>
        {nodes.map((n, i) => (
          <g key={n.id} transform={`translate(${nodeX(i)} ${NODE_Y})`}>
            <rect class={`box node ${status(n)}`} width={NODE_W} height={NODE_H} rx="8" />
            <text class="title" x="10" y="22">
              {n.id}
            </text>
            <text class="sub" x={NODE_W - 10} y="22" text-anchor="end">
              {n.rack}
            </text>
            <text class="stat" x="10" y="46">
              {n.chunks} chunk{n.chunks === 1 ? '' : 's'}
            </text>
            <text class="stat" x="10" y="66">
              {formatBytes(n.usedBytes)}
            </text>
            <rect class="bar-bg" x="10" y="78" width={NODE_W - 20} height="6" rx="3" />
            <rect class="bar" x="10" y="78" width={((NODE_W - 20) * n.usedBytes) / maxUsed} height="6" rx="3" />
            <text class={`state ${status(n)}`} x={NODE_W / 2} y={NODE_H + 18} text-anchor="middle">
              {LABEL[status(n)]}
            </text>
          </g>
        ))}
      </svg>
      {sim && (
        <div class="node-actions" role="group" aria-label="Fault injection">
          {nodes.map((n) => (
            <button type="button" key={n.id} onClick={() => onToggle(n)} aria-pressed={!!n.crashed}>
              {n.crashed ? `Restart ${n.id}` : `Crash ${n.id}`}
            </button>
          ))}
        </div>
      )}
      {view.net && (
        <figcaption>
          Network: {view.net.sent} messages, {view.net.dropped} dropped, {view.net.duplicated} duplicated,{' '}
          {formatBytes(view.net.bytes)} moved
        </figcaption>
      )}
    </figure>
  );
}

type Status = 'alive' | 'dead' | 'crashed';

const LABEL: Record<Status, string> = { alive: 'alive', dead: 'missed heartbeats', crashed: 'crashed' };

function status(n: NodeView): Status {
  if (n.crashed) return 'crashed';
  return n.alive ? 'alive' : 'dead';
}
