import type { ClusterView } from './api/cluster';

// HealthBar is the replication summary at a glance: every chunk, by how many
// copies it has on alive nodes.
export function HealthBar({ view }: { view: ClusterView }) {
  const h = view.health;
  const buckets = h?.replicas ?? [];
  const total = buckets.reduce((a, b) => a + b, 0);
  if (!h || total === 0) return null;
  const rf = Math.max(buckets.length - 2, 1);
  const label = (i: number) => (i === buckets.length - 1 ? `${i}+ copies` : i === 1 ? '1 copy' : `${i} copies`);
  const cls = (i: number) => (i === 0 ? 'lost' : i < rf ? 'under' : i > rf ? 'over' : 'ok');
  const summary =
    h.lost > 0
      ? `${h.lost} chunk${h.lost === 1 ? '' : 's'} with no live copy`
      : h.underReplicated > 0
        ? `${h.underReplicated} of ${total} chunks below ${rf} copies`
        : `all ${total} chunks at ${rf} copies`;
  return (
    <section class="healthbar" aria-label={`Replication: ${summary}`}>
      <div class="hb-bar" role="img" aria-label={buckets.map((n, i) => `${n} with ${label(i)}`).join(', ')}>
        {buckets.map((n, i) =>
          n > 0 ? <span key={i} class={`hb-seg ${cls(i)}`} style={{ flexGrow: n }} title={`${n} chunks with ${label(i)}`} /> : null,
        )}
      </div>
      <p class={`hb-summary ${h.lost > 0 ? 'bad' : h.underReplicated > 0 ? 'warn' : ''}`}>{summary}</p>
    </section>
  );
}
