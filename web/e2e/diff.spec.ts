import { expect, test } from '@playwright/test';

// Marking a placement and waiting out a repair shows what moved: node-3 dies
// with a copy of every chunk, repair places new copies elsewhere, and the
// grid reports them against the mark.
// The mark must land before node-3 is declared dead (about 21 s, 10 s after
// the scenario kills it), when its copies leave the placement: at 1x that is
// 10 s of wall time, at 50x only 200 ms.
test('the chunk grid diffs against a marked placement', async ({ page }) => {
  await page.goto('?seed=1&scenario=kill-node&speed=1');
  await expect(page.getByText('SIMULATION (in your browser)')).toBeVisible();
  const files = page.getByRole('region', { name: 'Files' });
  await files.getByRole('button', { name: '/demo/file-0.bin' }).click();
  await files.getByRole('button', { name: 'Mark this placement' }).click();
  expect(await page.evaluate(() => window.__chunkd?.nowMs ?? 0), 'marked after node-3 was declared dead').toBeLessThan(20_000);
  await expect(files.getByRole('status')).toHaveText('since the mark: 0 placed, 0 gone');
  await page.getByRole('group', { name: 'Speed' }).getByRole('button', { name: '50×' }).click();

  // Repair starts after the 20 s delay and has finished by 60 s.
  await page.waitForFunction(() => (window.__chunkd?.nowMs ?? 0) >= 60_000, undefined, { timeout: 90_000 });
  await expect(files.getByRole('status')).not.toHaveText('since the mark: 0 placed, 0 gone');
  const grid = files.getByRole('img', { name: 'Chunk placement grid' }).or(files.getByRole('group', { name: 'Chunk placement grid' }));
  expect(await grid.locator('rect.grid-cell.placed').count()).toBeGreaterThan(0);
  expect(await grid.locator('rect.grid-cell.lost').count()).toBeGreaterThan(0);

  await files.getByRole('button', { name: 'Clear the mark' }).click();
  await expect(grid.locator('rect.grid-cell.placed')).toHaveCount(0);
});
