import { useState } from 'preact/hooks';
import type { ClusterView, TimelineEvent } from './api/cluster';

const SHOWN = 200;
const KINDS: TimelineEvent['kind'][] = ['raft', 'node', 'copy', 'trim', 'corrupt', 'gc', 'read', 'write'];

// EventTimeline lists recent events, newest first: elections in the metadata
// group, the metadata leader's detector transitions, repair copies, trims,
// corrupt copies and GC, plus the scripted reads and writes of a scenario.
// Sim times are absolute simulated seconds; against a real cluster they are
// relative to now.
export function EventTimeline({ view, sim }: { view: ClusterView; sim: boolean }) {
  const [hidden, setHidden] = useState<Set<string>>(new Set());
  const all = [...view.timeline.map((e) => ({ e, key: `m${e.seq}` })), ...(view.reads ?? []).map((e) => ({ e, key: `r${e.seq}` }))];
  all.sort((a, b) => a.e.atMs - b.e.atMs);
  const counts = new Map<string, number>();
  for (const { e } of all) counts.set(e.kind, (counts.get(e.kind) ?? 0) + 1);
  const events = all.filter(({ e }) => !hidden.has(e.kind)).slice(-SHOWN).reverse();
  const when = (atMs: number) => (sim ? `t=${(atMs / 1000).toFixed(1)} s` : `${((view.nowMs - atMs) / 1000).toFixed(0)} s ago`);
  const toggle = (k: string) => {
    const next = new Set(hidden);
    if (next.has(k)) next.delete(k);
    else next.add(k);
    setHidden(next);
  };
  return (
    <section class="timeline" aria-label="Event timeline">
      <h2>Events</h2>
      <div class="filters" role="group" aria-label="Show event kinds">
        {KINDS.filter((k) => counts.has(k)).map((k) => (
          <button type="button" key={k} class={`chip ${k}`} aria-pressed={!hidden.has(k)} onClick={() => toggle(k)}>
            {k} <span class="muted">{counts.get(k)}</span>
          </button>
        ))}
      </div>
      {events.length === 0 ? (
        <p class="muted">No events yet.</p>
      ) : (
        <ol class="events">
          {events.map(({ e, key }) => (
            <li key={key} class={`ev ${e.kind} ${tone(e.text)}`}>
              <span class="ev-time">{when(e.atMs)}</span>
              <span class="ev-kind">{e.kind}</span>
              <span class="ev-node">{e.node}</span>
              <span class="ev-text">{e.text}</span>
            </li>
          ))}
        </ol>
      )}
      {all.length > SHOWN && (
        <p class="muted small">
          Showing the newest {SHOWN} of {all.length}.
        </p>
      )}
    </section>
  );
}

function tone(text: string): string {
  if (/→ dead|timed out|failed|drift|no contact from leader/.test(text)) return 'bad';
  if (/→ suspect|re-check|hedged|no leader yet/.test(text)) return 'warn';
  if (/→ alive|completed|joined| leads$/.test(text)) return 'good';
  return '';
}
