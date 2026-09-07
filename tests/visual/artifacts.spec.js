const { test, expect } = require('@playwright/test');
const { spawn } = require('node:child_process');

test('uploaded artifacts remain readable with no execution runners', async ({ page, request }) => {
  test.setTimeout(120000);
  const env = { ...process.env, DETENT_ARTIFACT_BROWSER: '1' };
  delete env.DETENT_API_TOKEN;
  const child = spawn('go', ['test', './internal/web', '-run', '^TestArtifactBrowserFixture$', '-count=1', '-v'], { env });
  let output = '';
  const exited = new Promise(resolve => child.once('exit', code => resolve(code)));
  const url = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Artifact fixture startup timed out: ${output}`)), 60000);
    child.once('error', error => { clearTimeout(timer); reject(error); });
    child.once('exit', code => { clearTimeout(timer); reject(new Error(`Fixture exited ${code}: ${output}`)); });
    const read = chunk => {
      output += chunk.toString();
      const match = output.match(/ARTIFACT_BROWSER_URL=(http:\/\/127\.0\.0\.1:\d+)/);
      if (match) { clearTimeout(timer); resolve(match[1]); }
    };
    child.stdout.on('data', read);
    child.stderr.on('data', read);
  });
  try {
    await page.goto(url);
    await page.locator('[name=artifact_member_token]').fill('fixture-member');
    await page.getByRole('button', { name: 'Read artifact' }).click();
    await expect(page.getByRole('button', { name: /Chunk 1/ })).toBeVisible();
    await page.getByRole('button', { name: /Chunk 1/ }).click();
    await expect(page.locator('[data-artifact-status]')).toContainText('Showing the first 256 KiB');
    await expect(page.locator('[data-artifact-text]')).toContainText('<script>');
    expect(await page.evaluate(() => window.artifactExecuted)).toBeUndefined();
    await expect(page.getByRole('link', { name: 'Download verified object' })).toBeVisible();
    await page.setViewportSize({ width: 390, height: 844 });
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    await request.post(`${url}/fixture/revoke`);
    await page.getByRole('button', { name: /Chunk 2/ }).click();
    await expect(page.locator('[data-artifact-status]')).toContainText('Access revoked');
    await expect(page.locator('[data-artifact-text]')).toBeEmpty();
    await page.getByRole('button', { name: 'Read artifact' }).click();
    await expect(page.locator('[data-artifact-status]')).toContainText('permission was revoked');
  } finally {
    await page.goto('about:blank');
    await request.post(`${url}/fixture/stop`);
    expect(await exited, output).toBe(0);
  }
});
