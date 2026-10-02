import { expect, test } from '@playwright/test';

// The ec scenario's files show as stripes: the Replicas column counts shards
// of 6, and the chunk grid labels each node's cell with the shard it holds.
test('erasure-coded files show their shards', async ({ page }) => {
  await page.goto('?seed=7&scenario=ec&speed=50');
  await expect(page.getByText('SIMULATION (in your browser)')).toBeVisible();
  await page.waitForFunction(() => (window.__chunkd?.nowMs ?? 0) >= 10_000, undefined, { timeout: 60_000 });
  const files = page.getByRole('region', { name: 'Files' });
  await expect(files.getByText('6/6 EC').first()).toBeVisible();
  await files.getByRole('button', { name: '/ec/file-0.bin' }).click();
  const grid = files.getByRole('img', { name: 'Chunk placement grid' }).or(files.getByRole('group', { name: 'Chunk placement grid' }));
  await expect(grid.locator('text.grid-shard')).toHaveCount(18); // 3 stripes × 6 shards
  await expect(files.getByText('0-3 data and 4-5 parity')).toBeVisible();
});
