import { useEffect, useMemo, useState } from 'preact/hooks';
import type { ClusterAPI, ClusterView as View, ScenarioInfo } from './api/cluster';
import { HttpClusterAPI } from './api/http';
import { WasmClusterAPI } from './api/wasm';
import { ClusterView } from './cluster-view';
import { NodeControls } from './controls';
import { Files } from './files';
import { HealthBar } from './healthbar';
import { Replication } from './repair';
import { EventTimeline } from './timeline';
import { Tour, tourSeen } from './tour';

const SPEEDS = [1, 5, 10, 25, 50];

const params = new URLSearchParams(location.search);

function seedFromURL(): number {
  const raw = params.get('seed');
  const n = raw === null ? NaN : Number(raw);
  if (Number.isSafeInteger(n) && n >= 0) return n;
  return Math.floor(Math.random() * 1_000_000);
}

function speedFromURL(): number {
  const n = Number(params.get('speed'));
  return SPEEDS.includes(n) ? n : 1;
}

// Which cluster: ?gateway=URL, or LIVE when the page is served by a gateway
// (it answers /mode.json), else the simulation in this browser.
async function makeAPI(): Promise<ClusterAPI> {
  const gw = params.get('gateway');
  if (gw) return new HttpClusterAPI(gw);
  if (params.get('sim') === null) {
    try {
      const r = await fetch(new URL('mode.json', location.href));
      if (r.ok && ((await r.json()) as { mode?: string }).mode === 'live') {
        return new HttpClusterAPI(new URL('.', location.href).href);
      }
    } catch {
      // Not served by a gateway.
    }
  }
  return new WasmClusterAPI();
}

// Exposed for the Playwright replay test: the timeline as the page has it.
declare global {
  interface Window {
    __chunkd?: { nowMs: number; timeline: View['timeline']; reads: View['timeline'] };
  }
}

export function App() {
  const seed = useMemo(seedFromURL, []);
  const scenario = params.get('scenario') ?? undefined;
  const [api, setAPI] = useState<ClusterAPI | null>(null);
  const [view, setView] = useState<View | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [paused, setPaused] = useState(false);
  const [speed, setSpeed] = useState(speedFromURL);
  const [scenarios, setScenarios] = useState<ScenarioInfo[]>([]);
  const [picked, setPicked] = useState(scenario ?? 'kill-node');
  const [copied, setCopied] = useState(false);
  const [tour, setTour] = useState(false);

  useEffect(() => {
    let disposed = false;
    let cleanup = () => {};
    void makeAPI().then((a) => {
      if (disposed) return a.dispose();
      setAPI(a);
      if (a.kind === 'sim' && !scenario && !tourSeen()) setTour(true);
      const unsub = a.subscribe((v) => {
        const next = { ...v, nodes: v.nodes ?? [], files: v.files ?? [], copies: v.copies ?? [], timeline: v.timeline ?? [] };
        window.__chunkd = { nowMs: next.nowMs, timeline: next.timeline, reads: next.reads ?? [] };
        setView(next);
      });
      a.setSpeed(speedFromURL() * 1000);
      a.start(seed, scenario)
        .then(() => a.scenarios().then(setScenarios))
        .catch((e: Error) => setError(e.message));
      cleanup = () => {
        unsub();
        a.dispose();
      };
    });
    return () => {
      disposed = true;
      cleanup();
    };
  }, [seed, scenario]);

  if (!api) return <main class="loading">Connecting…</main>;
  const sim = api.kind === 'sim';

  const togglePause = () => {
    if (paused) api.resume();
    else api.pause();
    setPaused(!paused);
  };
  const changeSpeed = (x: number) => {
    api.setSpeed(x * 1000);
    setSpeed(x);
  };
  // A fresh page load replays a scenario exactly: the script starts with
  // the cluster, before the first tick.
  const runScenario = () => {
    location.search = `?seed=${seed}&scenario=${picked}&speed=${speed}`;
  };
  const copyLink = () => {
    const u = new URL(location.href);
    u.search = `?seed=${seed}${scenario ? `&scenario=${scenario}` : ''}&speed=${speed}`;
    void navigator.clipboard.writeText(u.href).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  };
  const active = scenarios.find((s) => s.name === scenario);

  return (
    <main>
      <header class="top">
        <div>
          <h1>chunkd</h1>
          <p class="tagline">A fault-tolerant distributed file store</p>
        </div>
        <span class={`badge ${sim ? 'sim' : 'live'}`} title={api.label}>
          {sim ? 'SIMULATION (in your browser)' : `LIVE (${new URL(api.label.replace('Gateway at ', '')).host})`}
        </span>
      </header>

      <p class={`banner ${sim ? '' : 'real'}`} role="note">
        {sim ? (
          <>
            A simulated cluster running in your browser: a 3-peer Raft metadata group and 5 storage nodes on a lossy network, the same core Go
            code as the real multi-process cluster (<code>docker compose up</code>), compiled to WebAssembly. Nothing leaves this tab.{' '}
            <button type="button" class="link-button" onClick={() => setTour(true)}>
              Take the 5-step tour
            </button>
          </>
        ) : (
          <>
            Connected to a real cluster: {api.label}. Faults are injected with <code>docker compose</code> (kill, pause, exec); the
            buttons show the command. <a href="?sim">Switch to the in-browser simulation</a>
          </>
        )}
      </p>

      {sim && (
        <section class="controls" aria-label="Simulation controls">
          <span>
            Seed{' '}
            <a href={`?seed=${seed}`} title="Reload with this seed">
              {seed}
            </a>
          </span>
          <span class="clock">t = {view ? (view.nowMs / 1000).toFixed(2) : '0.00'} s</span>
          <button type="button" onClick={togglePause} aria-pressed={paused}>
            {paused ? 'Resume' : 'Pause'}
          </button>
          <button type="button" onClick={() => api.step()} disabled={!paused} title="Advance one simulated second">
            Step 1 s
          </button>
          <span class="speeds" role="group" aria-label="Speed">
            {SPEEDS.map((x) => (
              <button type="button" key={x} aria-pressed={speed === x} onClick={() => changeSpeed(x)}>
                {x}×
              </button>
            ))}
          </span>
          <span class="scenario-pick">
            <label for="scenario">Scenario</label>
            <select id="scenario" value={picked} onChange={(e) => setPicked((e.currentTarget as HTMLSelectElement).value)}>
              {scenarios.map((s) => (
                <option value={s.name} key={s.name}>
                  {s.title}
                </option>
              ))}
            </select>
            <button type="button" class="script" onClick={runScenario}>
              Run
            </button>
          </span>
          <button type="button" onClick={copyLink} title="A link that replays this run exactly">
            {copied ? 'Copied' : 'Copy link to this run'}
          </button>
        </section>
      )}
      {active && (
        <p class="scenario-note">
          <strong>{active.title}.</strong> {active.what}
        </p>
      )}

      {error && <p class="error">{view ? error : `Failed to start: ${error}`}</p>}
      {!view && !error && <p class="loading">{sim ? 'Loading cluster.wasm…' : 'Contacting the gateway…'}</p>}
      {view && (
        <>
          <HealthBar view={view} />
          <ClusterView view={view} sim={sim} />
          <NodeControls api={api} nodes={view.nodes} metas={view.metas ?? []} onError={setError} />
          <div class="panels">
            <Replication view={view} />
            <EventTimeline view={view} sim={sim} />
          </div>
          <Files
            api={api}
            files={view.files}
            deleted={view.deleted}
            epoch={view.epoch}
            nodes={view.nodes}
            refresh={`${view.health?.corruptReplicas ?? 0}/${view.health?.repairCompleted ?? 0}`}
            onError={setError}
          />
        </>
      )}
      {tour && <Tour onClose={() => setTour(false)} />}

      <footer>
        <a href="https://github.com/insanityatpeak/chunkd">Source on GitHub</a>
        {sim && (
          <>
            {' · '}
            Running <code>docker compose up</code> locally? Open <code>http://localhost:8080</code>.
          </>
        )}
      </footer>
    </main>
  );
}
