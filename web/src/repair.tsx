import type { ClusterView } from './api/cluster';
import { formatBytes } from './verify';

// Replication shows how many chunks sit at each alive-copy count, the repair
// queue, and every copy in flight. Health is null against a pre-Phase-2
// gateway.
export function Replication({ view }: { view: ClusterView }) {
  const h = view.health;
  if (!h) return null;
  const buckets = h.replicas ?? [];
  const rf = Math.max(buckets.length - 2, 1); // Go sizes the histogram RF+2
  const most = Math.max(1, ...buckets);
  return (
    <section class="replication" aria-label="Replication and repair">
      <h2>Replication</h2>
      <div class="rep-grid">
        <dl class="stats">
          <Stat label="chunks" value={h.chunks} />
          <Stat label={`below RF ${rf}`} value={h.underReplicated} bad={h.underReplicated > 0} />
          <Stat label="above RF" value={h.overReplicated} />
          <Stat label="no live copy" value={h.lost} bad={h.lost > 0} />
          <Stat label="corrupt copies found" value={h.corruptReplicas ?? 0} />
        </dl>
        <figure class="histogram">
          <figcaption>Chunks by copies on alive nodes</figcaption>
          {buckets.map((n, i) => (
            <div class="hist-row" key={i}>
              <span class="hist-label">{i === buckets.length - 1 ? `${i}+` : i}</span>
              <span class="hist-track">
                <span class={`hist-bar ${i < rf ? 'under' : i > rf ? 'over' : 'ok'}`} style={{ width: `${(100 * n) / most}%` }} />
              </span>
              <span class="hist-n">{n}</span>
            </div>
          ))}
        </figure>
      </div>

      <h3>Repair</h3>
      <dl class="stats">
        <Stat label="waiting out the delay" value={h.repairWaiting} />
        <Stat label="queued" value={h.repairQueued} />
        <Stat label="in flight" value={h.repairInFlight} />
        <Stat label="copies done" value={h.repairCompleted} />
        <Stat label="of them drain copies" value={h.repairEvacuated ?? 0} />
        <Stat label="of them balance moves" value={h.repairMoved ?? 0} />
        <Stat label="copied" value={formatBytes(h.repairBytes)} />
        <Stat label="trimmed" value={h.repairTrimmed} />
        <Stat label="timed out" value={h.repairTimedOut} />
        <Stat label="failed" value={h.repairFailed} />
      </dl>
      {view.copies.length > 0 ? (
        <table class="copies">
          <thead>
            <tr>
              <th>Copy</th>
              <th>Chunk</th>
              <th>From → to</th>
              <th>Size</th>
              <th>Running</th>
            </tr>
          </thead>
          <tbody>
            {view.copies.map((c) => (
              <tr key={c.id}>
                <td>#{c.id}</td>
                <td>
                  <code>{c.chunk.slice(0, 12)}</code>
                </td>
                <td>
                  {c.source} → {c.target}
                </td>
                <td>{formatBytes(c.bytes)}</td>
                <td>{((view.nowMs - c.startedMs) / 1000).toFixed(1)} s</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p class="muted">No copies in flight.</p>
      )}
      <Collection view={view} />
    </section>
  );
}

// Collection is dedup and garbage collection: what committed versions
// reference against what one copy of each distinct chunk costs, and the
// sweep's counters. Absent against a pre-Phase-4 gateway.
function Collection({ view }: { view: ClusterView }) {
  const gc = view.gc;
  if (!gc || view.referencedBytes === undefined || view.distinctBytes === undefined) return null;
  const ref = view.referencedBytes;
  const saved = ref > 0 ? Math.round((100 * (ref - view.distinctBytes)) / ref) : 0;
  return (
    <>
      <h3>Dedup and GC</h3>
      <dl class="stats">
        <Stat label="referenced" value={formatBytes(ref)} />
        <Stat label="distinct" value={formatBytes(view.distinctBytes)} />
        <Stat label="dedup saves" value={`${saved}%`} />
        <Stat label="GC epoch" value={view.epoch ?? 0} />
        <Stat label="orphan chunks" value={gc.orphans} />
        <Stat label="copies collected" value={gc.deleted} />
        <Stat label="deletes refused" value={gc.kept} />
        <Stat label="refcount drift" value={gc.drift} bad={gc.drift > 0} />
      </dl>
      <p class="muted small">
        Deleted and overwritten versions stay restorable for {gc.retainEpochs} epochs of {(gc.epochEveryMs / 1000).toFixed(0)} s, then
        their chunks lose the reference. A chunk nothing references or claims is deleted from nodes after a grace period.
      </p>
    </>
  );
}

function Stat({ label, value, bad }: { label: string; value: number | string; bad?: boolean }) {
  return (
    <div class={bad ? 'stat bad' : 'stat'}>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}
