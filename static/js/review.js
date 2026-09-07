(() => {
  if (window.detentReview) { window.detentReview(); return; }
  const instances = new Map();
  const digest = async bytes => [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(n => n.toString(16).padStart(2, '0')).join('');
  const fail = message => { throw new Error(message); };
  const hash = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
  const id = (value, prefix) => typeof value === 'string' && new RegExp(`^${prefix}_[a-f0-9]{32}$`).test(value);
  const safeError = error => error.name === 'AbortError' ? 'Review loading cancelled.' : error instanceof SyntaxError ? 'Malformed review data. No source was rendered.' : error instanceof TypeError ? 'Review data is invalid or the artifact service is unreachable. Load files again.' : error.message;

  async function bounded(response, limit) {
    if (!response.body) fail('Artifact response is empty.');
    const reader = response.body.getReader();
    const chunks = []; let size = 0;
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        size += value.length;
        if (size > limit) { await reader.cancel(); fail('Artifact exceeds its declared size or review limit.'); }
        chunks.push(value);
      }
    } finally { reader.releaseLock(); }
    const bytes = new Uint8Array(size); let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
    return bytes;
  }

  async function checked(url, grant, expected, limit, signal) {
    const response = await fetch(url, { headers: { Authorization: `Bearer ${grant.token}` }, signal, credentials: 'omit', cache: 'no-store', referrerPolicy: 'no-referrer', redirect: 'error' });
    if (!response.ok) {
      let code = '';
      try { code = JSON.parse(new TextDecoder().decode(await bounded(response, 4096))).code; } catch {}
      const reasons = { denied: 'Access revoked or expired. Load files again.', expired: 'Artifact retention has expired.', missing: 'An expected artifact object is missing.', checksum_mismatch: 'Artifact integrity verification failed.', storage_unreachable: 'Artifact storage is unreachable.', authorization_unavailable: 'Authorization service is unreachable.' };
      fail(reasons[code] || 'Artifact access is unavailable. Load files again.');
    }
    const bytes = await bounded(response, limit);
    if (await digest(bytes) !== expected) fail('Artifact integrity verification failed.');
    return bytes;
  }

  function safeMarkup(html) {
    if (typeof html !== 'string' || html.length > 4194304) fail('Rendered diff exceeds the review limit.');
    const template = document.createElement('template'); template.innerHTML = html;
    const allowed = new Set(['DIV', 'SPAN', 'TABLE', 'TBODY', 'THEAD', 'TR', 'TD', 'TH', 'PRE', 'CODE', 'INS', 'DEL']);
    let count = 0;
    const copy = node => {
      if (++count > 50000) fail('Rendered diff exceeds the review limit.');
      if (node.nodeType === Node.TEXT_NODE) return document.createTextNode(node.textContent);
      if (node.nodeType !== Node.ELEMENT_NODE || !allowed.has(node.tagName)) return document.createDocumentFragment();
      const result = document.createElement(node.tagName.toLowerCase());
      result.className = [...node.classList].filter(name => /^(d2h-[a-z0-9-]+|hljs-[a-z0-9_-]+|line-num[12])$/.test(name)).join(' ');
      if (/^[1-4]$/.test(node.getAttribute('colspan') || '')) result.setAttribute('colspan', node.getAttribute('colspan'));
      for (const child of node.childNodes) result.append(copy(child));
      return result;
    };
    const result = document.createDocumentFragment();
    for (const child of template.content.childNodes) result.append(copy(child));
    return result;
  }

  function verifyManifest(manifest, ref, config) {
    if (!manifest || typeof manifest !== 'object' || Array.isArray(manifest)) fail('Malformed artifact manifest.');
    const version = config.version;
    if (manifest.schema_version !== 1 || manifest.kind !== 'diff' || manifest.state !== 'complete') fail('Partial or unsupported bundle cannot be reviewed.');
    for (const key of ['artifact_id', 'manifest_id', 'revision', 'organization_id', 'project_id', 'work_item_id', 'run_id', 'attempt_id']) {
      if (manifest[key] !== ref[key]) fail('Artifact identity does not match the selected version.');
    }
    if ((manifest.version_id || '') !== (ref.version_id || '') || manifest.version_id && manifest.version_id !== version.version_id || manifest.organization_id !== config.change.organization_id || manifest.project_id !== config.change.project_id || manifest.work_item_id !== config.change.work_item_id || manifest.run_id !== version.run_id || manifest.attempt_id !== version.attempt_id) fail('Artifact scope does not match the selected version.');
    const capture = manifest.capture;
    if (!capture || capture.base !== version.base_sha || capture.head !== version.head_sha || capture.merge_base !== version.merge_base_sha || capture.working_tree !== false || capture.file_context !== 'changed_files' || !Number.isInteger(capture.context_lines) || capture.context_lines < 0 || capture.context_lines > 100) fail('Captured code does not match this immutable base and head.');
    if (!Number.isFinite(Date.parse(manifest.expires_at)) || Date.parse(manifest.expires_at) !== Date.parse(ref.expires_at) || Date.parse(manifest.expires_at) <= Date.now()) fail('Artifact retention has expired.');
    if (!Array.isArray(manifest.objects) || manifest.objects.length > 1024 || manifest.objects.length !== ref.objects || manifest.total_bytes !== ref.bytes) fail('Invalid artifact manifest bounds.');
    let total = 0; const seen = new Set();
    for (const object of manifest.objects) {
      if (!object || !id(object.object_id, 'object') || seen.has(object.object_id) || !hash(object.sha256) || !Number.isSafeInteger(object.size) || object.size < 0 || object.size > 16777216 || !['diff', 'base', 'head'].includes(object.side) || !['text/plain; charset=utf-8', 'text/x-diff; charset=utf-8'].includes(object.media_type) || object.path && (typeof object.path !== 'string' || object.path.length > 4096)) fail('Invalid artifact object.');
      total += object.size; seen.add(object.object_id);
    }
    if (total !== manifest.total_bytes || total > 268435456) fail('Invalid artifact manifest size.');
    const diffs = manifest.objects.filter(object => object.side === 'diff');
    if (diffs.length !== 1 || diffs[0].media_type !== 'text/x-diff; charset=utf-8') fail('This bundle requires one unified Git patch.');
    return diffs[0];
  }

  function install(root) {
    if (instances.has(root)) return;
    const config = JSON.parse(root.dataset.reviewConfig);
    const form = root.querySelector('[data-review-form]');
    const status = root.querySelector('[data-review-status]');
    const output = root.querySelector('[data-review-diff]');
    const list = root.querySelector('[data-review-files]');
    const viewed = root.querySelector('[data-review-viewed]');
    const decisions = [...root.querySelectorAll('[data-review-decision]')];
    let worker, controller, deadline, files = [], selected, ref, loaded = false, busy = false, sequence = 0, generation = 0, current = config.change.current_version_id;
    let viewedFiles = new Map(), pendingKey;
    const pending = new Map();
    const controls = () => { for (const button of decisions) button.disabled = busy || !loaded || current !== config.version.version_id; };
    function clear() {
      generation++; sequence++;
      controller?.abort(); worker?.terminate(); worker = null;
      clearTimeout(deadline);
      for (const entry of pending.values()) { clearTimeout(entry.timer); entry.reject(new DOMException('Cancelled', 'AbortError')); }
      pending.clear(); files = []; selected = null; loaded = false;
      output.replaceChildren(); list.replaceChildren(); viewed.checked = false; viewed.disabled = true;
      root.querySelector('[data-review-count]').textContent = ''; controls();
    }
    instances.set(root, clear);
    function rpc(action, data) {
      const requestID = ++sequence;
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          worker?.terminate(); worker = null; loaded = false; controls();
          for (const entry of pending.values()) { clearTimeout(entry.timer); entry.reject(new Error('Rendering time limit reached. Load files again to retry.')); }
          pending.clear();
        }, 5000);
        pending.set(requestID, { resolve, reject, timer });
        worker.postMessage({ id: requestID, action, ...data });
      });
    }
    async function request(action, extra = {}, signal) {
      const body = new FormData();
      for (const key of ['form_token', 'member_token', 'body']) body.set(key, form.elements[key].value);
      body.set('action', action);
      if (ref) for (const [key, value] of Object.entries({ artifact_id: ref.artifact_id, revision: ref.revision, sha256: ref.sha256, head_sha: config.version.head_sha })) body.set(key, value);
      for (const [key, value] of Object.entries(extra)) body.set(key, value);
      const signature = JSON.stringify([action, form.elements.body.value, ref?.sha256, extra]);
      if (!pendingKey || pendingKey.signature !== signature) pendingKey = { signature, key: crypto.randomUUID() };
      body.set('key', pendingKey.key);
      const response = await fetch(form.getAttribute('action'), { method: 'POST', body, signal, credentials: 'same-origin', cache: 'no-store', redirect: 'error' });
      if (!response.ok) {
        const errors = { 409: 'The current version or action changed. Refresh before submitting a review.', 403: 'Access revoked, expired, or unavailable for this project.', 410: 'Artifact retention has expired.', 422: 'Review identity is invalid or the artifact is unavailable.' };
        fail(errors[response.status] || 'Review service is unreachable. Retry when it is available.');
      }
      const result = JSON.parse(new TextDecoder().decode(await bounded(response, 1048576)));
      if (action !== 'load') pendingKey = null;
      return result;
    }
    function grantBase(grant) {
      if (!grant || typeof grant.token !== 'string' || grant.token.length > 4096 || !Number.isFinite(Date.parse(grant.expires_at))) fail('Invalid artifact grant.');
      let origin;
      try { origin = new URL(grant.origin); } catch { fail('Invalid artifact service origin.'); }
      if (origin.origin !== grant.origin || origin.protocol !== 'https:' && !(origin.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(origin.hostname)) || !grant.token || grant.artifact_id !== ref.artifact_id || grant.revision !== ref.revision || grant.sha256 !== ref.sha256 || Date.parse(grant.expires_at) <= Date.now()) fail('Artifact grant does not match the selected immutable bundle.');
      return `${origin.origin}/v1/artifacts/${grant.artifact_id}/manifests/${grant.revision}`;
    }
    const setCurrent = value => {
      current = value; controls();
      root.querySelector('[data-review-current]').textContent = current === config.version.version_id ? 'Current version.' : 'Older version. A newer version requires renewed approval. Refresh to review it.';
    };
    const count = () => { root.querySelector('[data-review-count]').textContent = `${files.filter(file => viewedFiles.get(file.key)).length} / ${files.length} files viewed for this version`; };
    function navigation() {
      list.replaceChildren(); let shown = 0;
      const more = document.createElement('button'); more.type = 'button'; more.textContent = 'Show more files';
      const append = () => {
        more.remove();
        for (const file of files.slice(shown, shown + 100)) {
          const button = document.createElement('button'); button.type = 'button';
          button.dataset.fileIndex = file.index; button.setAttribute('aria-current', String(selected === file));
          button.textContent = `${file.renamed ? file.oldName + ' → ' : ''}${file.name} · +${file.added} −${file.deleted}${file.binary ? ' · binary' : file.oversized ? ' · oversized' : ''}`;
          button.addEventListener('click', () => select(file)); list.append(button);
        }
        shown += 100; if (shown < files.length) list.append(more);
      };
      more.addEventListener('click', append); append(); count();
    }
    async function select(file) {
      selected = file; const serial = ++generation;
      output.replaceChildren(); viewed.disabled = true;
      viewed.checked = !!viewedFiles.get(file.key);
      for (const button of list.querySelectorAll('[data-file-index]')) button.setAttribute('aria-current', String(Number(button.dataset.fileIndex) === file.index));
      status.textContent = 'Checking current access…';
      try {
        const access = await request('load', {}, controller.signal);
        if (serial !== generation) return;
        grantBase(access.grant); setCurrent(access.current_version_id);
        let message;
        if (file.binary) message = 'Binary file. The captured patch has no renderable source for this file.';
        else if (file.oversized) message = 'Oversized file. Rendering is limited to 256 KiB, 2,000 lines, and 16 KiB per line. Use the verified artifact download below for the complete patch.';
        else {
          const rendered = await rpc('render', { index: file.index, layout: form.elements.layout.value });
          if (serial !== generation) return;
          output.replaceChildren(safeMarkup(rendered.html));
          const highlights = new Map(rendered.highlights);
          for (const node of output.querySelectorAll('.d2h-code-line-ctn')) {
            const html = highlights.get(node.textContent);
            if (html) node.replaceChildren(safeMarkup(html));
          }
          message = `Verified · ${file.name} · +${file.added} −${file.deleted}`;
        }
        if (file.binary || file.oversized) output.textContent = message;
        status.textContent = message;
        viewed.disabled = false;
      } catch (error) {
        if (serial !== generation) return;
        output.replaceChildren(); loaded = false; controls(); status.textContent = safeError(error);
      }
    }
    async function load() {
      clear(); ref = config.bundles[Number(form.elements.bundle.value)];
      if (!ref) fail('No complete immutable review bundle is available.');
      controller = new AbortController(); const serial = generation;
      status.textContent = 'Checking access and verifying the immutable manifest…';
      const access = await request('load', {}, controller.signal);
      if (serial !== generation) return;
      const base = grantBase(access.grant); setCurrent(access.current_version_id);
      const manifestBytes = await checked(base, access.grant, ref.sha256, 1048576, controller.signal);
      const manifest = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(manifestBytes));
      const object = verifyManifest(manifest, ref, config);
      status.textContent = 'Verifying the patch and indexing changed files…';
      const patch = await checked(`${base}/objects/${object.object_id}`, access.grant, object.sha256, object.size, controller.signal);
      if (patch.length !== object.size) fail('Artifact size does not match its manifest.');
      if (serial !== generation) return;
      worker = new Worker('/static/js/review-worker.js');
      worker.onmessage = event => {
        const entry = pending.get(event.data.id); if (!entry) return;
        clearTimeout(entry.timer); pending.delete(event.data.id);
        if (event.data.error) entry.reject(new Error(event.data.error)); else entry.resolve(event.data.value);
      };
      worker.onerror = () => {
        for (const entry of pending.values()) { clearTimeout(entry.timer); entry.reject(new Error('Diff renderer could not start. Reload to retry.')); }
        pending.clear(); worker?.terminate(); worker = null; loaded = false; controls();
      };
      files = await rpc('index', { text: new TextDecoder('utf-8', { fatal: true }).decode(patch), identity: ref.sha256 + ':' + object.object_id });
      if (serial !== generation) return;
      viewedFiles = new Map(access.viewed.filter(file => file.manifest_sha256 === ref.sha256).map(file => [file.file_sha256, file.viewed]));
      loaded = true; controls(); navigation();
      const expire = () => {
        const remaining = Date.parse(manifest.expires_at) - Date.now();
        if (remaining > 0) deadline = setTimeout(expire, Math.min(remaining, 2147483647));
        else { clear(); status.textContent = 'Artifact retention has expired. Loaded source has been cleared.'; }
      };
      expire();
      status.textContent = files.length ? `${files.length} files verified. Select a file to review.` : 'Verified empty patch. No changed files.';
      if (files.length) await select(files[0]);
    }
    form.addEventListener('submit', async event => {
      event.preventDefault(); if (busy) return;
      const action = event.submitter?.value; if (!action) return;
      busy = true; controls();
      try {
        if (action === 'load') await load();
        else {
          if (action !== 'discuss' && (!loaded || current !== config.version.version_id)) fail('Load and verify the current version before reviewing.');
          const result = await request(action);
          const message = document.createElement('p'); message.className = 'text-xs text-sec';
          message.textContent = `${action === 'discuss' ? 'Discussion posted' : action === 'approved' ? 'Version approved' : 'Changes requested'} · ${result.version_id || config.version.version_id}`;
          form.querySelector('textarea').value = ''; status.replaceChildren(message);
        }
      } catch (error) { clear(); status.textContent = safeError(error); }
      finally { busy = false; controls(); }
    });
    form.elements.bundle.addEventListener('change', clear);
    form.elements.member_token.addEventListener('input', () => { clear(); status.textContent = 'Access identity changed. Load files to verify again.'; });
    form.elements.layout.addEventListener('change', () => { if (selected && loaded) select(selected); });
    viewed.addEventListener('change', async () => {
      if (!selected || !loaded) return;
      const file = selected, serial = generation, desired = viewed.checked;
      viewed.disabled = true;
      try {
        await request('viewed', { file_sha256: file.key, viewed: String(desired) });
        if (serial !== generation) return;
        viewedFiles.set(file.key, desired); count();
      } catch (error) { if (serial === generation) { viewed.checked = !desired; status.textContent = safeError(error); } }
      finally { if (serial === generation) viewed.disabled = false; }
    });
  }
  const init = () => {
    for (const [root, cleanup] of instances) if (!root.isConnected) { cleanup(); instances.delete(root); }
    document.querySelectorAll('[data-review-config]').forEach(install);
  };
  window.detentReview = init;
  document.addEventListener('htmx:afterSettle', init);
  window.addEventListener('pagehide', () => { for (const cleanup of instances.values()) cleanup(); });
  new MutationObserver(() => { for (const [root, cleanup] of instances) if (!root.isConnected) { cleanup(); instances.delete(root); } }).observe(document.body, { childList: true, subtree: true });
  init();
})();
