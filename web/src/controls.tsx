import type { ClusterAPI, MetaPeerView, NodeView } from './api/cluster';

const SLOW_MS = 2000;
// The simulation runs at most this many storage nodes (cluster.MaxNodes).
const MAX_NODES = 8;
const NO_LEADER_CUT = 'compose cannot cut one peer off from the others: that needs per-link rules (README, Known limitations)';

interface Props {
  api: ClusterAPI;
  nodes: NodeView[];
  metas: MetaPeerView[];
  onError(msg: string): void;
}

// NodeControls injects faults into the simulation. Against a real cluster
// the page cannot reach Docker, so each button is disabled and its tooltip
// gives the command that does the same thing.
export function NodeControls({ api, nodes, metas, onError }: Props) {
  const live = !api.canInject;
  const tip = (sim: string, cmd: string) => (live ? `Real cluster: ${cmd}` : sim);
  const report = (p: Promise<void>) => void p.catch((e: Error) => onError(e.message));
  const leader = metas.find((m) => m.leader);
  const group = metas.length > 1;
  return (
    <section class="node-controls" aria-label="Fault injection">
      <h2>Break it</h2>
      <p class="muted small">
        {live
          ? 'Faults on a real cluster come from Docker; hover a button for the command.'
          : 'Each fault acts on one process. Watch its card, the replication bar and the timeline; a metadata fault shows as an election there.'}{' '}
        To corrupt a single replica, open a file below and click a filled cell in its chunk grid.
      </p>
      <div class="fault-table" role="table">
        {group &&
          metas.map((m) => {
            const down = m.state === 'down';
            const frozen = m.state === 'frozen';
            return (
              <div class="fault-row" role="row" key={m.id}>
                <span class="fault-node" role="cell">
                  {m.id} <span class="muted">{m.leader ? 'leader' : 'metadata'}</span>
                </span>
                <button
                  type="button"
                  disabled={live}
                  aria-pressed={down}
                  title={tip(down ? 'Start a new process over the same log; it rejoins as a follower and catches up' : 'Kill the process; its log survives', `docker compose ${down ? 'start' : 'kill'} ${m.id}`)}
                  onClick={() => report(down ? api.reviveMeta(m.id) : api.killMeta(m.id))}
                >
                  {down ? 'Restart' : 'Kill'}
                </button>
                <button
                  type="button"
                  disabled={live || down}
                  aria-pressed={frozen}
                  title={tip('Pause the process. A paused leader wakes as a stale leader and must not commit', `docker compose ${frozen ? 'unpause' : 'pause'} ${m.id}`)}
                  onClick={() => api.freeze(m.id, !frozen)}
                >
                  {frozen ? 'Thaw' : 'Freeze'}
                </button>
                <button
                  type="button"
                  disabled={live || down}
                  aria-pressed={!!m.cutOff}
                  title={live ? `Real cluster: ${NO_LEADER_CUT}` : 'Cut it off from the other metadata peers; nodes and clients still reach it'}
                  onClick={() => report(api.cutMeta(m.id, !m.cutOff))}
                >
                  {m.cutOff ? 'Heal' : 'Partition'}
                </button>
              </div>
            );
          })}
        {nodes.map((n) => {
          const svc = n.id;
          const leaving = !!n.admin && n.admin !== 'active';
          return (
            <div class="fault-row" role="row" key={n.id}>
              <span class="fault-node" role="cell">
                {n.id} <span class="muted">{n.rack}</span>
              </span>
              <button
                type="button"
                disabled={live}
                aria-pressed={!!n.crashed}
                title={tip(n.crashed ? 'Start a new process over the same disk' : 'Kill the process; the disk survives', `docker compose ${n.crashed ? 'start' : 'kill'} ${svc}`)}
                onClick={() => (n.crashed ? api.restart(n.id) : api.crash(n.id))}
              >
                {n.crashed ? 'Restart' : 'Kill'}
              </button>
              <button
                type="button"
                disabled={live || !!n.crashed}
                aria-pressed={!!n.frozen}
                title={tip('Pause the process: it neither sends nor answers (a long GC pause, SIGSTOP)', `docker compose ${n.frozen ? 'unpause' : 'pause'} ${svc}`)}
                onClick={() => api.freeze(n.id, !n.frozen)}
              >
                {n.frozen ? 'Thaw' : 'Freeze'}
              </button>
              <button
                type="button"
                disabled={live || !!n.crashed}
                aria-pressed={!!n.slowMs}
                title={tip(`Add ${SLOW_MS / 1000} s to every message: heartbeats still arrive, reads crawl`, `tc qdisc add dev eth0 root netem delay ${SLOW_MS}ms (needs NET_ADMIN)`)}
                onClick={() => api.slow(n.id, n.slowMs ? 0 : SLOW_MS)}
              >
                {n.slowMs ? 'Fast' : 'Slow'}
              </button>
              <button
                type="button"
                disabled={live || !!n.crashed}
                aria-pressed={!!n.partitioned}
                title={tip('Cut the node off from the metadata server only: clients still reach it', `docker network disconnect (cuts clients too)`)}
                onClick={() => api.partition(n.id, !n.partitioned)}
              >
                {n.partitioned ? 'Heal' : 'Partition'}
              </button>
              <button
                type="button"
                disabled={live}
                aria-pressed={leaving}
                title={tip(
                  leaving
                    ? 'Return it to service: it takes new chunks again and the balancer moves copies back'
                    : 'Stop placing chunks here and move its copies to other nodes, keeping rack spread',
                  `chunkd node ${leaving ? 'undrain' : 'drain'} ${n.id}`,
                )}
                onClick={() => report(api.drain(n.id, !leaving))}
              >
                {leaving ? 'Undrain' : 'Drain'}
              </button>
            </div>
          );
        })}
        <div class="fault-row" role="row">
          <span class="fault-node" role="cell">
            cluster
          </span>
          <button
            type="button"
            disabled={live || nodes.length >= MAX_NODES}
            title={
              nodes.length >= MAX_NODES && !live
                ? `The simulation runs at most ${MAX_NODES} storage nodes`
                : tip(
                    `Start node-${nodes.length + 1}, empty, on the next rack in turn; the balancer moves copies onto it`,
                    'docker compose --profile full --profile extra up -d node-6',
                  )
            }
            onClick={() => report(api.addNode())}
          >
            Add node
          </button>
          <button
            type="button"
            disabled={live || !leader || !group}
            title={
              !group
                ? 'A single metadata server: killing it is an outage, not a failover'
                : tip(leader ? `Kill ${leader.id}; a follower times out after about a second and wins the next term` : 'No leader: an election is running', `docker compose kill ${leader?.id ?? 'meta-N'}`)
            }
            onClick={() => report(api.killMeta(''))}
          >
            Kill metadata leader
          </button>
          <button
            type="button"
            disabled={live || !leader || !group}
            title={live ? `Real cluster: ${NO_LEADER_CUT}` : leader ? `Cut ${leader.id} off from its followers: it loses its quorum, the others elect a new leader` : 'No leader: an election is running'}
            onClick={() => report(api.cutMeta('', true))}
          >
            Partition metadata leader
          </button>
        </div>
      </div>
    </section>
  );
}
