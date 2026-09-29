import { expect, test } from '@playwright/test';
import { readFileSync } from 'node:fs';

interface Event {
  seq: number;
  atMs: number;
  kind: string;
  node: string;
  text: string;
}
interface Golden {
  seed: number;
  scenario: string;
  untilMs: number;
  events: Event[];
  reads: Event[];
}

// A shared scenario link must replay exactly: the page's timeline up to the
// golden's time equals the one the Go test (TestScenarioGolden) records for
// the same seed and scenario, stepping the same 50 ms ticks.
for (const name of ['kill-node', 'corrupt-chunk', 'gc']) {
  test(`?scenario=${name} replays the golden timeline`, async ({ page }) => {
    const golden = JSON.parse(readFileSync(new URL(`./golden/${name}.json`, import.meta.url), 'utf8')) as Golden;
    await page.goto(`?seed=${golden.seed}&scenario=${golden.scenario}&speed=50`);
    await expect(page.getByText('SIMULATION (in your browser)')).toBeVisible();
    await page.waitForFunction((until) => (window.__chunkd?.nowMs ?? 0) >= until, golden.untilMs, { timeout: 90_000 });
    const got = await page.evaluate((until) => {
      const keep = (es: Event[]) => es.filter((e) => e.atMs <= until);
      return { events: keep(window.__chunkd!.timeline), reads: keep(window.__chunkd!.reads) };
    }, golden.untilMs);
    expect(got.events).toEqual(golden.events);
    expect(got.reads).toEqual(golden.reads);
  });
}
