(() => {
  const root = document.querySelector('[data-hosted-work]');
  if (!root) return;
  const api = root.dataset.api;
  const issuePath = location.pathname.split('/changes/')[0];
  async function request(path, body) {
    const response = await fetch(path, {
      method: body === undefined ? 'GET' : 'POST', credentials: 'same-origin', cache: 'no-store', redirect: 'error',
      headers: {'Content-Type': 'application/json', 'X-CSRF-Token': root.dataset.csrf},
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      if ([401, 403, 404].includes(response.status)) throw new Error('Access is unavailable. Sign in again or check your project access.');
      if (response.status === 422) throw new Error('Check the form values and try again.');
      throw new Error('Could not load or save this work. Retry when the service is available.');
    }
    return response.json();
  }
  function element(tag, text, className = '') {
    const node = document.createElement(tag);
    node.textContent = text;
    node.className = className;
    return node;
  }
  function changeLink(change) {
    const link = element('a', change.title || 'Open Change', 'inline-flex min-h-11 items-center text-accent');
    link.href = issuePath + '/changes/' + encodeURIComponent(change.change_id);
    return link;
  }
  function artifactForm(ref) {
    const form = element('form', '', 'min-w-0 space-y-3');
    form.method = 'post';
    form.action = api + '/artifacts/' + encodeURIComponent(ref.artifact_id) + '/access';
    form.dataset.artifactForm = '';
    form.dataset.hostedCsrf = root.dataset.csrf;
    const revision = element('input', '');
    revision.type = 'hidden'; revision.name = 'revision'; revision.value = ref.revision;
    const button = element('button', 'Read artifact', 'min-h-11 rounded-card border border-line px-3 text-accent');
    button.type = 'submit'; button.disabled = ref.availability === 'expired';
    const status = element('p', '', 'text-sec'); status.dataset.artifactStatus = ''; status.setAttribute('role', 'status');
    const objects = element('div', '', 'flex min-w-0 flex-col gap-2'); objects.dataset.artifactObjects = '';
    const output = element('pre', '', 'max-h-96 max-w-full overflow-auto whitespace-pre-wrap break-words text-xs'); output.dataset.artifactText = '';
    form.append(element('p', `${ref.kind} · ${ref.state} · ${ref.availability}`), revision, button, status, objects, output);
    return form;
  }
  function render(source, item) {
    const row = element('article', '', 'min-w-0 space-y-2 border-t border-line pt-3');
    if (source === 'comments') {
      row.append(element('p', `${item.actor.principal_id} · ${item.created_at}`, 'text-xs text-sec'), element('div', item.body, 'whitespace-pre-wrap'));
    } else if (source === 'changes') {
      row.append(changeLink(item));
    } else if (source === 'attempts') {
      row.append(element('h3', `Run ${item.run_id} · ${item.status}`, 'font-semibold'));
      row.append(element('p', `Attempt ${item.attempt_id} · ${item.started_at}`, 'text-xs text-sec'));
      if (item.identity) row.append(element('p', [item.identity.role, item.identity.backend, item.identity.model].filter(Boolean).join(' · ')));
      if (item.outcome) row.append(element('p', item.outcome));
      if (item.checkpoint) {
        row.append(element('p', `Checkpoint: ${item.checkpoint.availability} · ${item.checkpoint.resume}`));
        if (item.checkpoint.change) row.append(changeLink(item.checkpoint.change));
      }
      if (item.artifact_ids?.length) {
        const link = element('a', 'Run artifacts', 'inline-flex min-h-11 items-center text-accent'); link.href = '#artifacts'; row.append(link);
      }
    } else if (source === 'history') {
      row.append(element('p', item.type.replaceAll('.', ' '), 'font-medium'), element('p', `${item.actor.principal_id} · ${item.recorded_at}`, 'text-xs text-sec'));
      if (item.data?.change) row.append(changeLink(item.data.change));
    } else if (source === 'artifacts') row.append(artifactForm(item));
    return row;
  }
  for (const section of root.querySelectorAll('[data-work-collection]')) {
    const source = section.dataset.workCollection;
    const items = section.querySelector('[data-work-items]');
    const status = section.querySelector('[data-work-status]');
    const more = section.querySelector('[data-work-more]');
    let cursor = '';
    async function load() {
      more.disabled = true; status.textContent = 'Loading…';
      try {
        const paged = ['comments', 'attempts', 'history'].includes(source);
        const result = await request(api + '/' + source + (paged ? '?limit=25' + (cursor ? '&cursor=' + encodeURIComponent(cursor) : '') : ''));
        const rows = paged ? result.items : result;
        for (const row of rows || []) items.append(render(source, row));
        cursor = result.next_cursor || '';
        more.hidden = !cursor; more.textContent = 'Load more';
        status.textContent = items.childElementCount ? '' : 'No entries yet.';
      } catch (error) {
        status.textContent = error.message; more.hidden = false; more.textContent = 'Retry';
      } finally { more.disabled = false; }
    }
    more.addEventListener('click', load);
    load();
  }
  root.addEventListener('submit', async event => {
    const form = event.target.closest('[data-work-action]');
    if (!form) return;
    event.preventDefault();
    const button = form.querySelector('button[type="submit"]');
    if (button.disabled) return;
    button.disabled = true;
    const result = form.querySelector('[data-work-result]');
    const fields = new FormData(form);
    const action = form.dataset.workAction;
    let path = api + '/comments';
    const body = {body: String(fields.get('body') || '')};
    if (action === 'change') { path = api + '/changes'; body.title = String(fields.get('title') || ''); }
    if (action === 'discussion') path = api + '/changes/' + encodeURIComponent(root.querySelector('[data-change-id]').dataset.changeId) + '/discussion';
    try {
      result.textContent = 'Saving…';
      const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(body)));
      const key = 'detent-work:' + path + ':' + [...new Uint8Array(hash)].map(n => n.toString(16).padStart(2, '0')).join('');
      body.idempotency_key = sessionStorage.getItem(key) || crypto.randomUUID();
      sessionStorage.setItem(key, body.idempotency_key);
      const saved = await request(path, body);
      sessionStorage.removeItem(key);
      if (action === 'change') location.assign(issuePath + '/changes/' + encodeURIComponent(saved.change_id));
      else location.reload();
    } catch (error) { result.textContent = error.message; }
    finally { button.disabled = false; }
  });
})();
