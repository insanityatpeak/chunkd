import type { ClusterView } from './api/cluster';

const SHOWN = 200;

// EventTimeline lists the metadata server's recent events, newest first:
// detector transitions, repair copies and trims. Sim times are absolute
// simulated seconds; against a real cluster they are relative to now.
export function EventTimeline({ view, sim }: { view: ClusterView; sim: boolean }) {
  const events = view.timeline.slice(-SHOWN).reverse();
  const when = (atMs: number) => (sim ? `t=${(atMs / 1000).toFixed(1)} s` : `${((view.nowMs - atMs) / 1000).toFixed(0)} s ago`);
  return (
    <section class="timeline" aria-label="Event timeline">
      <h2>Events</h2>
      {events.length === 0 ? (
        <p class="muted">No events yet.</p>
      ) : (
        <ol class="events">
          {events.map((e) => (
            <li key={e.seq} class={`ev ${e.kind} ${tone(e.text)}`}>
              <span class="ev-time">{when(e.atMs)}</span>
              <span class="ev-kind">{e.kind}</span>
              <span class="ev-node">{e.node}</span>
              <span class="ev-text">{e.text}</span>
            </li>
          ))}
        </ol>
      )}
      {view.timeline.length > SHOWN && (
        <p class="muted small">
          Showing the newest {SHOWN} of {view.timeline.length}.
        </p>
      )}
    </section>
  );
}

function tone(text: string): string {
  if (/→ dead|timed out|failed/.test(text)) return 'bad';
  if (/→ suspect|re-check/.test(text)) return 'warn';
  if (/→ alive|completed|joined/.test(text)) return 'good';
  return '';
}
