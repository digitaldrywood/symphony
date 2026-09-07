(() => {
  if (window.detentArtifactsInstalled) return;
  window.detentArtifactsInstalled = true;
  const digest = async bytes => [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(n => n.toString(16).padStart(2, '0')).join('');
  const checked = async (url, token, hash, limit) => {
    const response = await fetch(url, {headers: {Authorization: `Bearer ${token}`}, credentials: 'omit', cache: 'no-store', referrerPolicy: 'no-referrer', redirect: 'error'});
    if (!response.ok) {
      const failure = await response.json().catch(() => ({}));
      const reasons = {denied: 'Access revoked or expired. Request access again.', expired: 'Artifact retention has expired.', missing: 'An expected object is missing.', checksum_mismatch: 'Artifact integrity verification failed.', storage_unreachable: 'Artifact storage is unreachable.', authorization_unavailable: 'Authorization service is unreachable.'};
      throw new Error(reasons[failure.code] || 'Artifact is inaccessible. Retry when the service is reachable.');
    }
    const reader = response.body.getReader();
    const chunks = []; let size = 0;
    while (true) { const {done, value} = await reader.read(); if (done) break; size += value.length; if (size > limit) { await reader.cancel(); throw new Error('Artifact exceeds its declared size.'); } chunks.push(value); }
    const bytes = new Uint8Array(size); let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
    if (await digest(bytes) !== hash) throw new Error('Artifact integrity verification failed.');
    return bytes;
  };
  document.addEventListener('submit', async event => {
    const form = event.target.closest('[data-artifact-form]');
    if (!form) return;
    event.preventDefault();
    const status = form.querySelector('[data-artifact-status]');
    const output = form.querySelector('[data-artifact-text]');
    const objects = form.querySelector('[data-artifact-objects]');
    output.textContent = ''; objects.replaceChildren(); status.textContent = 'Checking access…';
    try {
      const body = new FormData(form);
      body.set('member_token', form.closest('[data-artifact-viewer]').querySelector('[name="artifact_member_token"]').value);
      const response = await fetch(form.action, {method: 'POST', body, credentials: 'same-origin', cache: 'no-store', redirect: 'error'});
      if (!response.ok) throw new Error('Artifact is inaccessible or your project permission was revoked.');
      const grant = await response.json();
      const base = `${grant.origin}/v1/artifacts/${grant.artifact_id}/manifests/${grant.revision}`;
      const bytes = await checked(base, grant.token, grant.sha256, 1048576);
      const manifest = JSON.parse(new TextDecoder('utf-8', {fatal: true}).decode(bytes));
      if (manifest.schema_version !== 1 || !Array.isArray(manifest.objects) || manifest.objects.length > 1024 || manifest.artifact_id !== grant.artifact_id || manifest.revision !== grant.revision) throw new Error('Unsupported artifact manifest.');
      status.textContent = `${manifest.state} · ${manifest.objects.length} verified object references. Select a chunk or file to read.`;
      for (const object of manifest.objects) {
        if (!/^object_[a-f0-9]{32}$/.test(object.object_id) || !Number.isSafeInteger(object.size) || object.size < 0 || object.size > 67108864) throw new Error('Invalid artifact object.');
        const button = document.createElement('button'); button.type = 'button';
        button.className = 'text-left text-accent break-all';
        button.textContent = manifest.kind === 'log' ? `Chunk ${object.sequence + 1} · ${object.size} bytes` : `${object.side || manifest.kind} · ${object.path || object.object_id}`;
        button.addEventListener('click', async () => {
          output.textContent = ''; status.textContent = 'Loading and verifying…';
          try {
            const data = await checked(`${base}/objects/${object.object_id}`, grant.token, object.sha256, object.size);
            if (data.length !== object.size) throw new Error('Artifact size does not match the manifest.');
            if (object.media_type.startsWith('text/')) {
              const shown = data.subarray(0, Math.min(data.length, 262144));
              output.textContent = new TextDecoder().decode(shown);
              status.textContent = data.length > shown.length ? 'Verified. Showing the first 256 KiB; download for the complete object.' : `Verified · ${manifest.state}`;
            } else status.textContent = 'Verified media. Download to view.';
            const link = document.createElement('a'); link.textContent = 'Download verified object'; link.download = 'artifact'; link.href = URL.createObjectURL(new Blob([data], {type: 'application/octet-stream'}));
            link.addEventListener('click', () => setTimeout(() => URL.revokeObjectURL(link.href), 1000), {once: true});
            output.append(document.createTextNode('\n'), link);
          } catch (error) { status.textContent = error.message; }
        });
        objects.append(button);
      }
    } catch (error) { status.textContent = error.message === 'Failed to fetch' ? 'Artifact service is unreachable. Uploaded content does not require a runner.' : error.message; }
  });
})();
