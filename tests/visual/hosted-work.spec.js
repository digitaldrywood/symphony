const { test, expect } = require('@playwright/test');
const { spawn } = require('node:child_process');
const { mkdirSync } = require('node:fs');

let url = process.env.DETENT_HOSTED_WORK_BROWSER_URL;
let exited;
let output = '';
test.beforeAll(async () => {
  test.setTimeout(120000);
  if (url) return;
  const env = { ...process.env, DETENT_HOSTED_WORK_BROWSER: '1' }; delete env.DETENT_API_TOKEN;
  const child = spawn('go', ['test', './internal/hubserver', '-run', '^TestHostedWorkBrowserPreview$', '-count=1', '-v'], { env });
  exited = new Promise(resolve => child.once('exit', resolve));
  url = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Hosted fixture startup timed out: ${output}`)), 90000);
    child.once('error', error => { clearTimeout(timer); reject(error); });
    child.once('exit', code => { clearTimeout(timer); reject(new Error(`Hosted fixture exited ${code}: ${output}`)); });
    const read = chunk => {
      output += chunk.toString();
      const match = output.match(/HOSTED_WORK_BROWSER_URL=(http:\/\/127\.0\.0\.1:\d+)/);
      if (match) { clearTimeout(timer); resolve(match[1]); }
    };
    child.stdout.on('data', read); child.stderr.on('data', read);
  });
});
test.afterAll(async ({ request }) => {
  if (!exited || !url) return;
  await request.post(`${url}/__preview/stop`);
  expect(await exited, output).toBe(0);
});
async function project(page) {
  await page.goto(`${url}/__preview/account/owner`);
  await page.locator('main').getByRole('link', { name: 'Browser collaboration', exact: true }).click();
}
async function narrow(page) {
  expect(await page.evaluate(() => ({width: innerWidth, overflow: [...document.querySelectorAll('main, [data-hosted-work], section')].some(el => el.scrollWidth > el.clientWidth + 1)}))).toEqual({width: 390, overflow: false});
}

test('hosted creation, discussion, history and Change navigation at 390px', async ({ page }) => {
  await page.setViewportSize({width: 390, height: 844});
  await page.goto(`${url}/login`);
  await page.getByRole('link', {name: 'Continue with WorkOS'}).click();
  await page.locator('main').getByRole('link', {name: 'Browser collaboration', exact: true}).click();
  const creation = page.locator('[data-setup-action="issue"]');
  await creation.locator('[name="title"]').fill('Browser-created native issue');
  await creation.locator('[name="body"]').fill('Private issue body ' + 'longword'.repeat(30));
  await creation.locator('button[type="submit"]').click();
  await page.getByRole('link', {name: /Browser-created native issue/}).click();
  await expect(page.locator('[data-hosted-work]')).toContainText('Private issue body');
  expect(await page.evaluate(() => document.contentType)).toBe('text/html');
  await expect(page.locator('#runs [data-work-status]')).toHaveText('No entries yet.');
  const comment = page.locator('[data-work-action="comment"]');
  await comment.locator('textarea').fill('Browser discussion <script>never execute</script>');
  await comment.getByRole('button', {name: 'Add comment', exact: true}).click();
  await expect(page.locator('#discussion')).toContainText('Browser discussion <script>never execute</script>');
  await expect(page.locator('#history')).toContainText('comment');
  await narrow(page);
  mkdirSync('tmp/playwright-evidence/hosted-work', {recursive: true});
  await page.screenshot({path: 'tmp/playwright-evidence/hosted-work/issue-390.png'});
  const change = page.locator('[data-work-action="change"]');
  await change.locator('[name="title"]').fill('Browser Change');
  await change.locator('textarea').fill('Browser Change discussion');
  await change.getByRole('button', {name: 'Open Change', exact: true}).click();
  await expect(page.locator('[data-change-id]')).toContainText('Browser Change');
  const discussion = page.locator('[data-work-action="discussion"]');
  await discussion.locator('textarea').fill('Change comment retained');
  await discussion.getByRole('button').click();
  await expect(page.locator('main')).toContainText('Change comment retained');
  await narrow(page);
  await page.screenshot({path: 'tmp/playwright-evidence/hosted-work/change-390.png'});
  await page.getByRole('navigation', {name: 'Project work navigation'}).getByRole('link', {name: 'Changes', exact: true}).click();
  await page.getByRole('link', {name: 'Browser Change', exact: true}).click();
  await expect(page.locator('[data-change-id]')).toContainText('Browser Change');
});

test('stored history and pagination remain readable without runners', async ({ page }) => {
  await page.setViewportSize({width: 390, height: 844});
  await project(page);
  await page.getByRole('link', {name: /Review the invitation flow/}).click();
  await expect(page.locator('#runs')).toContainText('offline-run');
  await expect(page.locator('#runs')).toContainText('succeeded');
  await expect(page.locator('#discussion [data-work-items] > article')).toHaveCount(25);
  await page.locator('#discussion').getByRole('button', {name: 'Load more'}).click();
  await expect(page.locator('#discussion [data-work-items] > article')).toHaveCount(27);
  await expect(page.locator('#discussion')).toContainText('Stored discussion 26');
  await narrow(page);
  await page.locator('#changes').getByRole('link', {name: 'Stored Change'}).click();
  await expect(page.locator('main')).toContainText('Change remains readable offline');
});

for (const account of ['viewer', 'staff', 'wrong-organization', 'revoked', 'expired']) {
  test(`hosted work authorization for ${account}`, async ({ page }) => {
    await project(page);
    await page.getByRole('link', {name: /Review the invitation flow/}).click();
    const issue = page.url();
    await page.locator('#changes').getByRole('link', {name: 'Stored Change'}).click();
    const change = page.url();
    await page.goto(`${url}/__preview/account/${account}`);
    for (const path of [issue, change]) {
      const response = await page.goto(path);
      if (account === 'viewer') {
        expect(response.status()).toBe(200);
        await expect(page.locator('[data-work-action]')).toHaveCount(0);
      } else {
        expect([401, 403]).toContain(response.status());
        await expect(page.locator('main')).toContainText('unavailable');
        await expect(page.locator('body')).not.toContainText('Browser fixture private body');
        await expect(page.locator('body')).not.toContainText('Change remains readable offline');
      }
    }
  });
}

test('CSRF and live project grant revocation protect open pages', async ({ page, browser }) => {
  await project(page);
  await page.getByRole('link', {name: /Review the invitation flow/}).click();
  const path = page.url();
  expect(await page.evaluate(async () => (await fetch(document.querySelector('[data-hosted-work]').dataset.api + '/comments', {method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({idempotency_key:'no-csrf', body:'must fail'})})).status)).toBe(403);
  const viewer = await browser.newPage({viewport: {width: 390, height: 844}});
  try {
    await viewer.goto(`${url}/__preview/account/viewer`);
    await viewer.goto(path);
    await expect(viewer.locator('#discussion')).toContainText('Stored discussion');
    const projectID = new URL(path).pathname.split('/')[2];
    await page.goto(`${url}/organization`);
    const form = page.locator('form[action="/organization/grants"]');
    await form.locator('[name="user"]').selectOption('user_browser_viewer');
    await form.locator('[name="project"]').fill(projectID);
    await form.getByRole('button', {name: 'Revoke project access'}).click();
    await viewer.locator('#discussion').getByRole('button', {name: 'Load more'}).click();
    await expect(viewer.locator('#discussion [data-work-status]')).toContainText('Access is unavailable');
    expect((await viewer.reload()).status()).toBe(403);
    await narrow(viewer);
  } finally { await viewer.close(); }
});

test('hosted artifact viewer requests scoped access and verifies independent downloads', async ({ page }) => {
  const { createHash } = require('node:crypto');
  const digest = value => createHash('sha256').update(value).digest('hex');
  const artifactID = 'artifact_' + 'a'.repeat(32);
  const objectID = 'object_' + 'b'.repeat(32);
  const bytes = 'Verified offline run log';
  const manifest = JSON.stringify({schema_version: 1, artifact_id: artifactID, revision: 1, kind: 'log', state: 'complete', objects: [{object_id: objectID, size: Buffer.byteLength(bytes), sha256: digest(bytes), sequence: 0, media_type: 'text/plain'}]});
  const accessRequests = [];
  const downloads = [];
  await page.route('**/work-items/*/artifacts', route => route.fulfill({json: [{artifact_id: artifactID, revision: 1, kind: 'log', state: 'complete', availability: 'available'}]}));
  await page.route('**/work-items/*/artifacts/*/access', route => {
    accessRequests.push({body: route.request().postDataJSON(), csrf: route.request().headers()['x-csrf-token']});
    return route.fulfill({json: {origin: 'https://artifacts.example.test', artifact_id: artifactID, revision: 1, token: 'artifact-test-grant', sha256: digest(manifest)}});
  });
  await page.route('https://artifacts.example.test/**', route => {
    downloads.push(route.request().headers());
    return route.fulfill({body: route.request().url().includes('/objects/') ? bytes : manifest, headers: {'Access-Control-Allow-Origin': '*'}});
  });
  await project(page);
  await page.getByRole('link', {name: /Review the invitation flow/}).click();
  await page.locator('#artifacts').getByRole('button', {name: 'Read artifact'}).click();
  await expect(page.locator('[data-artifact-status]')).toContainText('verified object references');
  expect(accessRequests).toHaveLength(1);
  expect(accessRequests[0].body).toEqual({revision: 1});
  expect(accessRequests[0].csrf).toMatch(/^[a-f0-9]{64}$/);
  await page.getByRole('button', {name: /Chunk 1/}).click();
  await expect(page.locator('[data-artifact-text]')).toContainText(bytes);
  await expect(page.getByRole('link', {name: 'Download verified object'})).toHaveAttribute('href', /^blob:/);
  for (const headers of downloads) {
    expect(headers.authorization).toBe('Bearer artifact-test-grant');
    expect(headers.cookie).toBeUndefined();
  }
});

test('project Changes paginate without loading full discussion bodies', async ({ page }) => {
  await page.setViewportSize({width: 390, height: 844});
  await project(page);
  await page.getByRole('link', {name: /Review the invitation flow/}).click();
  expect(await page.evaluate(async () => {
    const root = document.querySelector('[data-hosted-work]');
    for (let i = 0; i < 26; i++) {
      const response = await fetch(root.dataset.api + '/changes', {method: 'POST', headers: {'Content-Type': 'application/json', 'X-CSRF-Token': root.dataset.csrf}, body: JSON.stringify({idempotency_key: `pagination-${i}`, title: `Archived Change ${i}`, body: 'Full body belongs on the detail page'})});
      if (!response.ok) return response.status;
    }
    return 200;
  })).toBe(200);
  await page.getByRole('navigation', {name: 'Project work navigation'}).getByRole('link', {name: 'Changes', exact: true}).click();
  await expect(page.locator('main a[href*="/issues/"]')).toHaveCount(25);
  await expect(page.locator('main')).not.toContainText('Full body belongs on the detail page');
  await narrow(page);
  await page.getByRole('link', {name: 'Older Changes'}).click();
  await page.getByRole('link', {name: 'Archived Change 0', exact: true}).click();
  await expect(page.locator('[data-change-id]')).toContainText('Full body belongs on the detail page');
});
