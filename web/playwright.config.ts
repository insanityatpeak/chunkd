import { defineConfig } from '@playwright/test';

// Replay smoke test against the production build (npm run build first).
// CI uses Playwright's Chromium; locally PW_CHANNEL=msedge uses an installed
// Edge instead of downloading a browser.
export default defineConfig({
  testDir: 'e2e',
  timeout: 120_000,
  use: { baseURL: 'http://localhost:4173/', channel: process.env.PW_CHANNEL || undefined },
  webServer: { command: 'npx vite preview --port 4173 --strictPort', url: 'http://localhost:4173/', reuseExistingServer: true },
});
