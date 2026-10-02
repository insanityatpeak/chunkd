import type { MetaPeerView, ClusterView as View, NodeView } from './api/cluster';
import { formatBytes } from './verify';

const W = 760;
const META_W = 200;
const META_H = 74;
const META_GAP = 16;
const NODE_W = 132;
const NODE_H = 128;
const META_Y = 14;
const NODE_Y = 150;

interface Props {
  view: View;
  sim: boolean;
}

// ClusterView draws the metadata group above the storage nodes. Each peer
// shows its Raft role, term and log indexes; the crown marks the leader
// that serves (a quorum confirms it). Nodes are coloured by the failure
// detector's state: alive, suspect after 3 s without a heartbeat, dead
// after 10 s. A crashed process (sim only) is dashed.
export function ClusterView({ view }: Props) {
  const nodes = view.nodes;
  const slot = W / Math.max(nodes.length, 1);
  const nodeX = (i: number) => slot * i + (slot - NODE_W) / 2;
  const maxUsed = Math.max(1, ...nodes.map((n) => n.usedBytes));
  const metas: MetaPeerView[] = view.metas?.length
    ? view.metas
    : [{ id: view.meta?.id ?? 'metadata server', state: 'unknown', leader: true, applied: view.meta?.applied }];
  const metasW = metas.length * META_W + (metas.length - 1) * META_GAP;
  const metaX = (i: number) => (W - metasW) / 2 + i * (META_W + META_GAP);
  // Nodes heartbeat every peer; the lines go to the leader that acts on them.
  const lead = metas.findIndex((m) => m.leader);
  const hubX = lead >= 0 ? metaX(lead) + META_W / 2 : W / 2;
  const leader = lead >= 0 ? metas[lead] : undefined;
  const label = `Cluster topology: metadata group of ${metas.length}, ${leader ? `leader ${leader.id}${leader.term ? ` in term ${leader.term}` : ''}` : 'no leader (election running)'}; ${nodes.length} storage nodes`;

  return (
    <figure class="cluster">
      <svg viewBox={`0 0 ${W} ${NODE_Y + NODE_H + 34}`} role="img" aria-label={label}>
        {nodes.map((n, i) => (
          <line
            key={`l-${n.id}`}
            class={`link ${status(n)}${n.partitioned ? ' cut' : ''}${lead < 0 ? ' orphan' : ''}`}
            x1={hubX}
            y1={META_Y + META_H}
            x2={nodeX(i) + NODE_W / 2}
            y2={NODE_Y}
          />
        ))}
        {metas.map((m, i) => (
          <g key={m.id} transform={`translate(${metaX(i)} ${META_Y})`}>
            <rect class={`box meta ${metaClass(m)}`} width={META_W} height={META_H} rx="8" />
            {m.leader && <path class="crown" d="M10 -4 l3 -8 l4 5 l4 -8 l4 8 l4 -5 l3 8 z" />}
            <text class="title" x="12" y="23">
              {m.id}
            </text>
            <text class={`sub role ${metaClass(m)}`} x="12" y="44">
              {metaState(m)}
            </text>
            <text class="sub" x="12" y="63">
              {indexes(m)}
            </text>
          </g>
        ))}
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
            <text class={`sub hb ${status(n)}`} x="10" y="86">
              heartbeat {formatAge(n.heartbeatAgeMs)} ago
            </text>
            <text class="sub scrub" x="10" y="104">
              <title>Scrub pass progress; copies quarantined after failing verification</title>
              scrub {scrubPct(n)}
              {n.corrupt ? <tspan class="corrupt"> · {n.corrupt} bad</tspan> : null}
            </text>
            <rect class="bar-bg" x="10" y="114" width={NODE_W - 20} height="6" rx="3" />
            <rect class="bar" x="10" y="114" width={((NODE_W - 20) * n.usedBytes) / maxUsed} height="6" rx="3" />
            <text class={`state ${status(n)}`} x={NODE_W / 2} y={NODE_H + 18} text-anchor="middle">
              {[n.crashed && 'process down', ...faults(n), n.state].filter(Boolean).join(' · ')}
            </text>
          </g>
        ))}
      </svg>
      {view.net && (
        <figcaption>
          Network: {view.net.sent} messages, {view.net.dropped} dropped, {view.net.duplicated} duplicated,{' '}
          {formatBytes(view.net.bytes)} moved
        </figcaption>
      )}
    </figure>
  );
}

type Status = NodeView['state'] | 'crashed';

function status(n: NodeView): Status {
  // The metadata server's view wins once it has noticed; until then a
  // crashed process still shows as alive there, so mark it.
  if (n.crashed && n.state === 'alive') return 'crashed';
  return n.state;
}

// faults lists injected impairments that the detector may not show yet.
function faults(n: NodeView): string[] {
  const out: string[] = [];
  if (n.frozen) out.push('frozen');
  if (n.partitioned) out.push('cut off');
  if (n.slowMs) out.push(`+${n.slowMs / 1000} s`);
  return out;
}

function scrubPct(n: NodeView): string {
  if (!n.scrubTotal) return n.scrubPasses ? 'idle' : 'not started';
  return `${Math.floor((100 * (n.scrubDone ?? 0)) / n.scrubTotal)}%`;
}

function formatAge(ms: number): string {
  if (ms < 10_000) return `${(ms / 1000).toFixed(1)} s`;
  if (ms < 120_000) return `${Math.round(ms / 1000)} s`;
  return `${Math.round(ms / 60_000)} min`;
}

// metaState is the peer's role with injected faults first: a cut-off
// leader still calls itself leader until it hears a newer term.
function metaState(m: MetaPeerView): string {
  const parts: string[] = [];
  if (m.cutOff) parts.push('cut off');
  parts.push(m.state === 'pre-candidate' ? 'pre-vote' : m.state);
  if (m.state === 'leader' && !m.leader && m.cutOff) parts.push('no quorum');
  return parts.join(' · ');
}

function metaClass(m: MetaPeerView): string {
  if (m.state === 'down' || m.state === 'unreachable') return 'dead';
  if (m.cutOff || m.state === 'frozen' || m.state === 'candidate' || m.state === 'pre-candidate') return 'suspect';
  return m.leader ? 'leader' : '';
}

function indexes(m: MetaPeerView): string {
  if (m.state === 'unreachable') return 'not heard from';
  const parts: string[] = [];
  if (m.term !== undefined) parts.push(`term ${m.term}`);
  if (m.commit !== undefined) parts.push(`commit ${m.commit}`);
  if (m.applied !== undefined && m.applied !== m.commit) parts.push(`applied ${m.applied}`);
  if (m.commit === undefined && m.match !== undefined) parts.push(`log ${m.match}`);
  return parts.join(' · ') || 'no report';
}
