import { useEffect, useMemo, useState } from 'preact/hooks';
import type { ClusterAPI, ClusterView as View } from './api/cluster';
import { HttpClusterAPI } from './api/http';
import { WasmClusterAPI } from './api/wasm';
import { ClusterView } from './cluster-view';
import { Files } from './files';
import { Replication } from './repair';
import { EventTimeline } from './timeline';

const SPEEDS = [1, 5, 10, 25, 50].map((x) => ({ label: `${x}×`, simMsPerSec: x * 1000 }));

// The scripted failure: node-3 stays down 90 s, past dead (10 s) plus the
// repair delay (20 s), so the run shows re-replication and then the trims
// after it returns.
const SCRIPT_NODE = 'node-3';
const SCRIPT_DOWN_MS = 90_000;
// Bit rot nobody is told about: the scrubber (a pass every 10 simulated
// minutes, about 12 s at 50×) or a reader finds it.
const ROT_NODE = 'node-2';
const ROT_CHUNKS = 3;

const params = new URLSearchParams(location.search);

function seedFromURL(): number {
  const raw = params.get('seed');
  const n = raw === null ? NaN : Number(raw);
  if (Number.isSafeInteger(n) && n >= 0) return n;
  return Math.floor(Math.random() * 1_000_000);
}

// ?gateway=http://localhost:8080 points the same UI at a real cluster.
function makeAPI(): ClusterAPI {
  const gw = params.get('gateway');
  return gw ? new HttpClusterAPI(gw) : new WasmClusterAPI();
}

export function App() {
  const seed = useMemo(seedFromURL, []);
  const [api] = useState<ClusterAPI>(makeAPI);
  const [view, setView] = useState<View | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [paused, setPaused] = useState(false);
  const [speed, setSpeed] = useState(1000);
  const sim = api.kind === 'sim';

  useEffect(() => {
    // Go encodes empty slices as null; normalise once here.
    const unsub = api.subscribe((v) =>
      setView({ ...v, nodes: v.nodes ?? [], files: v.files ?? [], copies: v.copies ?? [], timeline: v.timeline ?? [] }),
    );
    api.start(seed).catch((e: Error) => setError(e.message));
    return () => {
      unsub();
      api.dispose();
    };
  }, [api, seed]);

  const togglePause = () => {
    if (paused) api.resume();
    else api.pause();
    setPaused(!paused);
  };
  const changeSpeed = (v: number) => {
    api.setSpeed(v);
    setSpeed(v);
  };
  const runScript = () => {
    setError(null);
    api.scenario(SCRIPT_NODE, SCRIPT_DOWN_MS).catch((e: Error) => setError(e.message));
  };
  const runRot = () => {
    setError(null);
    api.rot(ROT_NODE, ROT_CHUNKS).catch((e: Error) => setError(e.message));
  };

  return (
    <main>
      <header>
        <h1>chunkd</h1>
        <p class="tagline">A fault-tolerant distributed file store</p>
      </header>

      {sim ? (
        <p class="banner" role="note">
          Simulated cluster running in your browser. Same core code as the real multi-process version (
          <code>docker compose up</code>).
        </p>
      ) : (
        <p class="banner real" role="note">
          Connected to a real cluster: {api.label}. <a href={location.pathname}>Switch to the in-browser simulation</a>
        </p>
      )}

      {sim && (
        <section class="controls" aria-label="Simulation controls">
          <span>
            Seed <a href={`${location.pathname}?seed=${seed}`} title="Reload with this seed to replay the same run">{seed}</a>
          </span>
          <span class="clock">t = {view?.nowMs !== undefined ? (view.nowMs / 1000).toFixed(2) : '0.00'} s</span>
          <button type="button" onClick={togglePause}>
            {paused ? 'Resume' : 'Pause'}
          </button>
          <span class="speeds" role="group" aria-label="Speed">
            {SPEEDS.map((s) => (
              <button type="button" key={s.label} aria-pressed={speed === s.simMsPerSec} onClick={() => changeSpeed(s.simMsPerSec)}>
                {s.label}
              </button>
            ))}
          </span>
          <button type="button" class="script" onClick={runScript} title={`Kill ${SCRIPT_NODE}, restart it after ${SCRIPT_DOWN_MS / 1000} s`}>
            Run scenario: kill {SCRIPT_NODE}
          </button>
          <button type="button" class="script" onClick={runRot} title={`Flip bytes in ${ROT_CHUNKS} chunks on ${ROT_NODE}'s disk without telling anyone`}>
            Rot {ROT_CHUNKS} chunks on {ROT_NODE}
          </button>
        </section>
      )}

      {error && <p class="error">{view ? error : `Failed to start: ${error}`}</p>}
      {!view && !error && <p class="loading">{sim ? 'Loading cluster.wasm…' : 'Contacting the gateway…'}</p>}
      {view && (
        <>
          <ClusterView view={view} sim={sim} onToggle={(n) => (n.crashed ? api.restart(n.id) : api.crash(n.id))} />
          <div class="panels">
            <Replication view={view} />
            <EventTimeline view={view} sim={sim} />
          </div>
          <Files api={api} files={view.files} nodes={view.nodes} />
        </>
      )}

      <footer>
        <a href="https://github.com/insanityatpeak/chunkd">Source on GitHub</a>
        {sim && (
          <>
            {' · '}
            Running <code>docker compose up</code> locally? Open this page with <code>?gateway=http://localhost:8080</code>.
          </>
        )}
      </footer>
    </main>
  );
}
