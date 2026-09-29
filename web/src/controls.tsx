import type { ClusterAPI, NodeView } from './api/cluster';

const SLOW_MS = 2000;

interface Props {
  api: ClusterAPI;
  nodes: NodeView[];
  onError(msg: string): void;
}

// NodeControls injects faults into the simulation. Against a real cluster
// the page cannot reach Docker, so each button is disabled and its tooltip
// gives the command that does the same thing.
export function NodeControls({ api, nodes }: Props) {
  const live = !api.canInject;
  const tip = (sim: string, cmd: string) => (live ? `Real cluster: ${cmd}` : sim);
  return (
    <section class="node-controls" aria-label="Fault injection">
      <h2>Break it</h2>
      <p class="muted small">
        {live
          ? 'Faults on a real cluster come from Docker; hover a button for the command.'
          : 'Each fault acts on one node. Watch the node card, the replication bar and the timeline.'}{' '}
        To corrupt a single replica, open a file below and click a filled cell in its chunk grid.
      </p>
      <div class="fault-table" role="table">
        {nodes.map((n) => {
          const svc = n.id;
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
            </div>
          );
        })}
        <div class="fault-row" role="row">
          <span class="fault-node" role="cell">
            cluster
          </span>
          <button type="button" disabled title="Adding nodes needs rebalancing, which lands in Phase 6">
            Add node
          </button>
          <button type="button" disabled title="One metadata server today; Raft leader election lands in Phase 5">
            Kill metadata leader
          </button>
        </div>
      </div>
    </section>
  );
}
