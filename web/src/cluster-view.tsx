import type { ClusterState, NodeView, PeerView } from './api/cluster';

const W = 720;
const BOX_W = 180;
const BOX_H = 92;
const META_Y = 16;
const NODE_Y = 196;

interface Props {
  state: ClusterState;
}

// ClusterView draws the metadata server above its storage nodes. Each link
// shows the meta server's view of that node: solid when alive, dashed when
// its heartbeats have stopped arriving.
export function ClusterView({ state }: Props) {
  const peers = new Map<string, PeerView>((state.meta.peers ?? []).map((p) => [p.id, p]));
  const metaX = (W - BOX_W) / 2;
  const slot = W / state.nodes.length;
  const nodeX = (i: number) => slot * i + (slot - BOX_W) / 2;

  return (
    <figure class="cluster">
      <svg viewBox={`0 0 ${W} ${NODE_Y + BOX_H + 8}`} role="img" aria-label="Cluster topology and heartbeat counters">
        {state.nodes.map((n, i) => (
          <line
            key={`l-${n.id}`}
            class={`link ${status(peers.get(n.id))}`}
            x1={metaX + BOX_W / 2}
            y1={META_Y + BOX_H}
            x2={nodeX(i) + BOX_W / 2}
            y2={NODE_Y}
          />
        ))}

        <g transform={`translate(${metaX} ${META_Y})`}>
          <rect class="box meta" width={BOX_W} height={BOX_H} rx="8" />
          <text class="title" x="12" y="24">{state.meta.id}</text>
          <text class="sub" x="12" y="44">metadata server</text>
          <text class="stat" x="12" y="72">
            pings received {sum(state.meta.peers ?? [])}
          </text>
        </g>

        {state.nodes.map((n, i) => (
          <NodeBox key={n.id} node={n} peer={peers.get(n.id)} x={nodeX(i)} />
        ))}
      </svg>
      <figcaption>
        Network: {state.net.sent} sent, {state.net.delivered} delivered, {state.net.dropped} dropped,{' '}
        {state.net.duplicated} duplicated
      </figcaption>
    </figure>
  );
}

type Status = 'waiting' | 'alive' | 'dead';

const LABEL: Record<Status, string> = { waiting: 'not heard yet', alive: 'alive', dead: 'unreachable' };

// A node the meta server has never heard from is waiting, not dead: its first
// heartbeat is scheduled up to one interval after start.
function status(peer?: PeerView): Status {
  if (!peer) return 'waiting';
  return peer.alive ? 'alive' : 'dead';
}

function NodeBox({ node, peer, x }: { node: NodeView; peer?: PeerView; x: number }) {
  const s = status(peer);
  return (
    <g transform={`translate(${x} ${NODE_Y})`}>
      <rect class={`box node ${s}`} width={BOX_W} height={BOX_H} rx="8" />
      <text class="title" x="12" y="24">{node.id}</text>
      <text class="sub" x={BOX_W - 12} y="24" text-anchor="end">
        {LABEL[s]}
      </text>
      <text class="stat" x="12" y="52">
        heartbeats sent {node.sent}
      </text>
      <text class="stat" x="12" y="74">
        acked {node.acked} · meta saw {peer?.pings ?? 0}
      </text>
    </g>
  );
}

function sum(peers: PeerView[]) {
  return peers.reduce((n, p) => n + p.pings, 0);
}
