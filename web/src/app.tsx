import { useEffect, useMemo, useState } from 'preact/hooks';
import type { ClusterAPI, ClusterState } from './api/cluster';
import { WasmClusterAPI } from './api/wasm';
import { ClusterView } from './cluster-view';

const SPEEDS = [
  { label: '0.5×', simMsPerSec: 500 },
  { label: '1×', simMsPerSec: 1000 },
  { label: '4×', simMsPerSec: 4000 },
  { label: '16×', simMsPerSec: 16000 },
];

function seedFromURL(): number {
  const raw = new URLSearchParams(location.search).get('seed');
  const n = raw === null ? NaN : Number(raw);
  if (Number.isSafeInteger(n) && n >= 0) return n;
  return Math.floor(Math.random() * 1_000_000);
}

export function App() {
  const seed = useMemo(seedFromURL, []);
  const [api] = useState<ClusterAPI>(() => new WasmClusterAPI());
  const [state, setState] = useState<ClusterState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [paused, setPaused] = useState(false);
  const [speed, setSpeed] = useState(1000);

  useEffect(() => {
    const unsub = api.subscribe(setState);
    api.start(seed).catch((e: Error) => setError(e.message));
    return () => {
      unsub();
      api.dispose();
    };
  }, [api, seed]);

  const togglePause = () => {
    paused ? api.resume() : api.pause();
    setPaused(!paused);
  };
  const changeSpeed = (v: number) => {
    api.setSpeed(v);
    setSpeed(v);
  };

  const replayHref = `${location.pathname}?seed=${seed}`;

  return (
    <main>
      <header>
        <h1>chunkd</h1>
        <p class="tagline">A fault-tolerant distributed file store</p>
      </header>

      <p class="banner" role="note">
        Simulated cluster running in your browser. Same core code as the real multi-process version (
        <code>docker compose up</code>).
      </p>

      <section class="controls" aria-label="Simulation controls">
        <span>
          Seed <a href={replayHref} title="Reload with this seed to replay the same run">{seed}</a>
        </span>
        <span class="clock">t = {state ? (state.nowMs / 1000).toFixed(2) : '0.00'} s</span>
        <button type="button" onClick={togglePause}>
          {paused ? 'Resume' : 'Pause'}
        </button>
        <span class="speeds" role="group" aria-label="Speed">
          {SPEEDS.map((s) => (
            <button
              type="button"
              key={s.label}
              aria-pressed={speed === s.simMsPerSec}
              onClick={() => changeSpeed(s.simMsPerSec)}
            >
              {s.label}
            </button>
          ))}
        </span>
      </section>

      {error && <p class="error">Failed to start the simulation: {error}</p>}
      {!state && !error && <p class="loading">Loading cluster.wasm…</p>}
      {state && <ClusterView state={state} />}

      <footer>
        <a href="https://github.com/insanityatpeak/chunkd">Source on GitHub</a>
      </footer>
    </main>
  );
}
