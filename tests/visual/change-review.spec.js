const { test, expect } = require('@playwright/test');
const { spawn } = require('node:child_process');
const { mkdirSync } = require('node:fs');
const { createHash } = require('node:crypto');

let url = process.env.DETENT_REVIEW_BROWSER_URL;
let exited;
let fixtureOutput = '';
test.beforeAll(async () => {
  test.setTimeout(120000);
  if (url) return;
  const env = { ...process.env, DETENT_REVIEW_BROWSER: '1' }; delete env.DETENT_API_TOKEN;
  const child = spawn('go', ['test', './internal/web', '-run', '^TestReviewBrowserFixture$', '-count=1', '-v'], { env });
  exited = new Promise(resolve => child.once('exit', resolve));
  url = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Review fixture startup timed out: ${fixtureOutput}`)), 60000);
    child.once('error', error => { clearTimeout(timer); reject(error); });
    child.once('exit', code => { clearTimeout(timer); reject(new Error(`Review fixture exited ${code}: ${fixtureOutput}`)); });
    const read = chunk => {
      fixtureOutput += chunk.toString();
      const match = fixtureOutput.match(/REVIEW_BROWSER_URL=(http:\/\/127\.0\.0\.1:\d+)/);
      if (match) { clearTimeout(timer); resolve(match[1]); }
    };
    child.stdout.on('data', read); child.stderr.on('data', read);
  });
});
test.afterAll(async ({ request }) => {
  if (!exited) return;
  await request.post(`${url}/fixture/stop`);
  expect(await exited, fixtureOutput).toBe(0);
});
test.beforeEach(async ({ page, request }) => {
  await request.post(`${url}/fixture/reset`);
  await page.goto(url);
  await expect(page.locator('[data-review-form]')).toBeVisible();
});

const surface = page => page.locator('[data-review-config]');
const status = page => page.locator('[data-review-status]');
async function load(page) {
  await surface(page).getByRole('textbox', { name: 'Project access token', exact: true }).fill('fixture-member');
  await surface(page).getByRole('button', { name: 'Load files', exact: true }).click();
  await expect(status(page)).toContainText('Verified · main.go');
}
async function evidence(page, name) {
  mkdirSync('tmp/playwright-evidence/change-review', { recursive: true });
  await surface(page).scrollIntoViewIfNeeded();
  await page.screenshot({ path: `tmp/playwright-evidence/change-review/${name}.png` });
}

test('native files, layouts, viewed state, and offline runner', async ({ page }) => {
  const github = [];
  page.on('request', request => { if (new URL(request.url()).hostname === 'api.github.com') github.push(request.url()); });
  await load(page);
  const output = page.locator('[data-review-diff]');
  await expect(output).toContainText('<img src=x onerror=window.reviewExecuted=true>');
  expect(await page.evaluate(() => window.reviewExecuted)).toBeUndefined();
  expect(await output.locator('[class^=hljs-]').count()).toBeGreaterThan(0);
  await expect(page.locator('[data-review-files] button')).toHaveCount(5);
  await evidence(page, 'dark-unified-offline');
  await surface(page).getByRole('combobox', { name: 'Diff layout' }).selectOption('side-by-side');
  await expect(output.locator('.d2h-file-side-diff')).toHaveCount(2);
  await evidence(page, 'dark-split');
  await surface(page).getByRole('checkbox', { name: 'Viewed this version' }).check();
  await expect(page.locator('[data-review-count]')).toContainText('1 / 5');
  await page.reload(); await load(page);
  await expect(surface(page).getByRole('checkbox', { name: 'Viewed this version' })).toBeChecked();
  for (const item of [{ name: 'old.go → renamed.go', message: 'renamed.go', screenshot: 'rename' }, { name: 'image.png', message: 'Binary file.', screenshot: 'binary' }, { name: 'huge.go', message: 'Oversized file.', screenshot: 'huge' }]) {
    await page.locator('[data-review-files]').getByRole('button', { name: new RegExp(item.name) }).click();
    await expect(status(page)).toContainText(item.message);
    await evidence(page, item.screenshot);
  }
  await page.locator('[data-review-files]').getByRole('button', { name: /long.go/ }).click();
  await expect(status(page)).toContainText('Verified · long.go');
  await page.setViewportSize({ width: 390, height: 844 });
  await evidence(page, 'narrow-long-line');
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await page.locator('[data-review-diff]').focus();
  await expect(page.locator('[data-review-diff]')).toBeFocused();
  expect(github).toEqual([]);
});

test('updated version rejects stale approval and resets viewed state', async ({ page, request }) => {
  await load(page);
  await surface(page).getByRole('checkbox', { name: 'Viewed this version' }).check();
  await expect(page.locator('[data-review-count]')).toContainText('1 / 5');
  await surface(page).getByRole('button', { name: 'Approve version' }).click();
  await expect(status(page)).toContainText('Version approved');
  const oldVersion = await page.locator('[data-review-config]').evaluate(node => JSON.parse(node.dataset.reviewConfig).version.version_id);
  await request.post(`${url}/fixture/update`);
  await surface(page).getByRole('button', { name: 'Approve version' }).click();
  await expect(status(page)).toContainText('current version or action changed');
  await evidence(page, 'stale-tab-rejected');
  await page.goto(`${url}/?version=${oldVersion}`); await load(page);
  await expect(surface(page).getByRole('button', { name: 'Approve version' })).toBeDisabled();
  await expect(page.locator('[data-review-current]')).toContainText('newer version requires renewed approval');
  await evidence(page, 'older-version');
  await page.getByRole('navigation', { name: 'Review version', exact: true }).getByRole('link', { name: 'v2 · current' }).click();
  await load(page);
  await expect(page.locator('[data-review-count]')).toContainText('0 / 5');
  await expect(surface(page)).toContainText('native stale');
  await evidence(page, 'renewed-approval-required');
  await surface(page).getByRole('textbox', { name: 'Change discussion or review message' }).fill('Please cover the new behavior.');
  await surface(page).getByRole('button', { name: 'Request changes' }).click();
  await expect(status(page)).toContainText('Changes requested');
  await surface(page).getByRole('textbox', { name: 'Change discussion or review message' }).fill('Change-level discussion.');
  await surface(page).getByRole('button', { name: 'Post discussion' }).click();
  await expect(status(page)).toContainText('Discussion posted');
});

test('revocation clears source and prevents further reviews', async ({ page, request }) => {
  await load(page);
  await request.post(`${url}/fixture/revoke`);
  await page.locator('[data-review-files]').getByRole('button', { name: /long.go/ }).click();
  await expect(status(page)).toContainText('Access revoked');
  await expect(page.locator('[data-review-diff]')).toBeEmpty();
  await expect(surface(page).getByRole('button', { name: 'Approve version' })).toBeDisabled();
  await evidence(page, 'revoked-access');
});

test('corrupted manifest fails integrity before rendering', async ({ page }) => {
  await page.route('**/manifests/1', async route => {
    const response = await route.fetch();
    await route.fulfill({ response, body: Buffer.concat([await response.body(), Buffer.from(' ')]) });
  });
  await surface(page).getByRole('textbox', { name: 'Project access token', exact: true }).fill('fixture-member');
  await surface(page).getByRole('button', { name: 'Load files', exact: true }).click();
  await expect(status(page)).toContainText('integrity verification failed');
  await expect(page.locator('[data-review-diff]')).toBeEmpty();
});

for (const fixture of [
  { name: 'wrong capture head', edit: m => { m.capture.head = 'c'.repeat(40); }, message: 'Captured code does not match' },
  { name: 'wrong work item', edit: m => { m.work_item_id = 'wi_' + 'a'.repeat(32); }, message: 'identity does not match' },
  { name: 'partial bundle', edit: m => { m.state = 'partial'; }, message: 'Partial or unsupported' },
  { name: 'oversized object', edit: m => { m.objects[0].size = 16777217; }, message: 'Invalid artifact object' },
  { name: 'null object', edit: m => { m.objects[0] = null; }, message: 'Invalid artifact object' },
  { name: 'expired artifact', edit: m => { m.expires_at = '2020-01-01T00:00:00Z'; }, message: 'retention has expired' },
]) {
  test(`verified manifest rejects ${fixture.name}`, async ({ page, request }) => {
    const config = await surface(page).evaluate(node => JSON.parse(node.dataset.reviewConfig));
    const action = await page.locator('[data-review-form]').getAttribute('action');
    const ref = config.bundles[0];
    const access = await request.post(url + action, { form: { action: 'load', member_token: 'fixture-member' } });
    const { grant } = await access.json();
    const manifestResponse = await request.get(`${grant.origin}/v1/artifacts/${grant.artifact_id}/manifests/${grant.revision}`, { headers: { Authorization: 'Bearer fixture-grant' } });
    const manifest = await manifestResponse.json(); fixture.edit(manifest);
    const body = JSON.stringify(manifest); const hash = createHash('sha256').update(body).digest('hex');
    await page.route('**/*', async route => {
      const request = route.request(); const target = new URL(request.url());
      if (target.pathname.endsWith('/manifests/1')) {
        const response = await route.fetch(); await route.fulfill({ response, body });
      } else if (request.resourceType() === 'document' || target.searchParams.get('content') === '1' || target.pathname.endsWith('/review')) {
        const response = await route.fetch(); await route.fulfill({ response, body: (await response.text()).replaceAll(ref.sha256, hash) });
      } else await route.continue();
    });
    await page.reload();
    await surface(page).getByRole('textbox', { name: 'Project access token', exact: true }).fill('fixture-member');
    await surface(page).getByRole('button', { name: 'Load files', exact: true }).click();
    await expect(status(page)).toContainText(fixture.message);
    await expect(page.locator('[data-review-diff]')).toBeEmpty();
    await expect(surface(page).getByRole('button', { name: 'Approve version' })).toBeDisabled();
  });
}

test('worker rejects malformed hunks and bounds file counts', async ({ page }) => {
  const result = await page.evaluate(async () => {
    async function run(text, render) {
      const worker = new Worker('/static/js/review-worker.js');
      try {
        const send = data => new Promise(resolve => { worker.onmessage = event => resolve(event.data); worker.postMessage(data); });
        const indexed = await send({ id: 1, action: 'index', text, identity: 'fixture' });
        return render && !indexed.error ? await send({ id: 2, action: 'render', index: 0, layout: 'line-by-line' }) : indexed;
      } finally { worker.terminate(); }
    }
    const malformed = await run('diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1,2 +1,2 @@\n-x\n+y\n', true);
    const tooMany = await run('diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n'.repeat(2049), false);
    const tooManyLines = await run('diff --git a/a.go b/a.go\n' + '+\n'.repeat(100001), false);
    const prefixed = await run('diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1 +1 @@\n---old\n+++new\n', false);
    return { errors: [malformed.error, tooMany.error, tooManyLines.error], counts: prefixed.value[0] };
  });
  for (const error of result.errors) expect(error).toContain('Malformed or unsupported');
  expect(result.counts).toEqual(expect.objectContaining({ added: 1, deleted: 1 }));
});
