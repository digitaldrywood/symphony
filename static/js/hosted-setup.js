(() => {
  const root = document.querySelector('[data-project-setup]');
  if (!root) return;
  const api = root.dataset.api;
  const organization = api.split('/projects/')[0];
  const project = api.split('/projects/')[1];
  async function request(path, method, body) {
    const response = await fetch(path, {
      method, credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': root.dataset.csrf },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      const messages = {
        401: 'Sign in again, then resume this project.',
        403: 'Action required: ask an administrator to check your project and runner grants.',
        404: 'Action required: this resource or permission is unavailable to your account.',
        409: 'Configuration changed. Reload and inspect the current values before retrying.',
        422: 'Action required: check the entered IDs and configuration. Optional GitHub features require a configured transport and repository binding.',
      };
      throw new Error(messages[response.status] || 'Infrastructure unavailable. Retry here without creating another project or host identity.');
    }
    return response.json();
  }
  root.addEventListener('submit', async (event) => {
    const form = event.target.closest('[data-setup-action]');
    if (!form) return;
    event.preventDefault();
    const button = form.querySelector('button[type="submit"]');
    if (button.disabled) return;
    button.disabled = true;
    const result = form.querySelector('[data-setup-result]');
    result.textContent = 'Saving…';
    const fields = new FormData(form);
    const action = form.dataset.setupAction;
    const value = (name) => String(fields.get(name) || '');
    try {
      let path = api, method = 'PUT', body;
      switch (action) {
        case 'progress':
          path += '/onboarding';
          body = { progress: { revision: form.dataset.revision, repository: value('repository'), doctor: fields.has('doctor'), provider: fields.has('provider'), artifacts: value('artifacts') } };
          break;
        case 'policy':
          path += '/onboarding/policy';
          body = { expected_policy_id: value('expected_policy_id'), policy: JSON.parse(value('policy')) };
          break;
        case 'issue':
          path += '/work-items'; method = 'POST';
          body = { title: value('title'), body: value('body'), state: value('state'), labels: [], assignees: [] };
          break;
        case 'enroll':
          path = organization + '/runner-enrollments'; method = 'POST';
          body = { runner_id: value('runner_id'), machine_id: value('machine_id'), project_ids: [project], operations: ['read', 'collaborate', 'claim', 'heartbeat', 'events'], ttl_seconds: 900 };
          break;
        case 'routing': {
          path = organization + '/runners/' + encodeURIComponent(form.dataset.runner) + '/routing';
          const fleet = await request(organization + '/runners', 'GET');
          const runner = fleet.find(entry => entry.runner_id === form.dataset.runner);
          if (!runner) throw new Error('Runner access changed. Reload this project.');
          if (String(runner.revision) !== form.dataset.revision) throw new Error('Runner changed. Reload before editing tags.');
          body = { expected_revision: runner.revision, display_name: runner.display_name, tags: value('tags').split(',').map(tag => tag.trim()).filter(Boolean), state: runner.state, capacity_limit: runner.capacity_limit, project_ids: runner.project_ids };
          break;
        }
        case 'binding':
          path += '/onboarding/artifact-services/' + encodeURIComponent(value('service_id'));
          body = { service_id: value('service_id'), origin: value('origin'), mode: 'customer', publisher_token_id: value('publisher_token_id') };
          break;
        case 'integration':
          path += '/onboarding/integration';
          body = { expected_revision: form.dataset.revision, intake: value('intake'), projection: value('projection'), repository_enabled: fields.has('repository_enabled') };
          break;
        case 'repository':
          path += '/onboarding/repository'; method = 'POST';
          body = { expected_revision: form.dataset.revision, repository: value('repository') };
          break;
        default: throw new Error('Unknown setup action. Reload this page.');
      }
      const durable = ['progress', 'issue', 'integration', 'repository'].includes(action);
      let storageKey;
      if (durable) {
        const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(body)));
        const fingerprint = Array.from(new Uint8Array(digest), n => n.toString(16).padStart(2, '0')).join('');
        storageKey = 'detent-setup:' + path + ':' + fingerprint;
        let key = sessionStorage.getItem(storageKey);
        if (!key) { key = crypto.randomUUID(); sessionStorage.setItem(storageKey, key); }
        body.idempotency_key = key;
      }
      const saved = await request(path, method, body);
      if (action === 'enroll') {
        result.textContent = 'One-time token: ' + saved.token + ' — expires ' + saved.expires_at + '. Use it only on the host that generated these IDs. If interrupted, create a fresh token for the same IDs.';
      } else {
        if (storageKey) sessionStorage.removeItem(storageKey);
        result.textContent = 'Saved. Reloading project readiness…';
        window.location.reload();
      }
    } catch (error) {
      result.textContent = error instanceof SyntaxError ? 'Action required: paste a valid policy descriptor JSON object.' : error instanceof TypeError ? 'Infrastructure unavailable. Retry here without recreating the project or host identity.' : error.message;
    } finally { button.disabled = false; }
  });
})();
