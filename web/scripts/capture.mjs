// Records the dashboard replaying a scenario to docs/assets/dashboard.mp4 and
// dashboard.gif. Needs a production build served on :4173 (npm run build,
// npx vite preview --port 4173) and ffmpeg on PATH. PW_CHANNEL=msedge uses an
// installed Edge. Usage: node scripts/capture.mjs [seconds] [query]
import { chromium } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const seconds = Number(process.argv[2] ?? 26);
const query = process.argv[3] ?? '?seed=7&scenario=kill-node&speed=10';
const out = new URL('../../docs/assets/', import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1');
const dir = mkdtempSync(join(tmpdir(), 'chunkd-capture-'));

const browser = await chromium.launch({ channel: process.env.PW_CHANNEL || undefined });
const ctx = await browser.newContext({ viewport: { width: 1000, height: 1500 }, recordVideo: { dir, size: { width: 1000, height: 1500 } } });
const page = await ctx.newPage();
await page.goto('http://localhost:4173/' + query);
await page.getByText('SIMULATION (in your browser)').waitFor();
await page.waitForTimeout(seconds * 1000);
await ctx.close();
await browser.close();

const webm = readdirSync(dir).find((f) => f.endsWith('.webm'));
if (!webm) throw new Error('no video recorded');
const src = join(dir, webm);
execFileSync('ffmpeg', ['-y', '-loglevel', 'error', '-i', src, '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-crf', '28', '-movflags', '+faststart', join(out, 'dashboard.mp4')]);
// 10 fps, 800 px wide, one shared palette: keeps the GIF small.
execFileSync('ffmpeg', ['-y', '-loglevel', 'error', '-i', src, '-vf', 'fps=10,scale=560:-1:flags=lanczos,split[a][b];[a]palettegen=max_colors=128[p];[b][p]paletteuse=dither=bayer:bayer_scale=4', join(out, 'dashboard.gif')]);
