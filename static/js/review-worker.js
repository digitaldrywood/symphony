importScripts('/static/vendor/diff2html/diff2html.min.js', '/static/vendor/highlight/highlight.min.js');

let sections = [];
const limits = { bytes: 16777216, patchLines: 100000, files: 2048, fileBytes: 262144, lines: 2000, lineLength: 16384 };
const parseOptions = { diffMaxChanges: limits.lines, diffMaxLineLength: limits.lineLength };
const hash = async value => [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value)))].map(n => n.toString(16).padStart(2, '0')).join('');

function checkHunks(text) {
  let old = 0, next = 0, hunk = false;
  for (const line of text.split('\n')) {
    if (line.startsWith('@@')) {
      if (old || next) throw new Error();
      const match = /^@@ -\d+(?:,(\d+))? \+\d+(?:,(\d+))? @@/.exec(line);
      if (!match) throw new Error();
      old = Number(match[1] ?? 1); next = Number(match[2] ?? 1); hunk = true;
    } else if (hunk && line && !line.startsWith('\\ No newline')) {
      if (line[0] === ' ') { old--; next--; }
      else if (line[0] === '-') old--;
      else if (line[0] === '+') next--;
      else throw new Error();
      if (old < 0 || next < 0) throw new Error();
    }
  }
  if (old || next) throw new Error();
}

async function indexPatch(text, identity) {
  if (typeof text !== 'string' || text.length > limits.bytes || text.includes('\0')) throw new Error();
  if (!text) { sections = []; return []; }
  if (!text.startsWith('diff --git ')) throw new Error();
  let lineCount = 0;
  for (let offset = 0; offset < text.length; offset++) if (text.charCodeAt(offset) === 10 && ++lineCount > limits.patchLines) throw new Error();
  sections = text.split(/(?=^diff --git )/m, limits.files + 1);
  if (sections.length > limits.files) throw new Error();
  const files = [];
  for (let index = 0; index < sections.length; index++) {
    const section = sections[index];
    const lines = section.split('\n');
    const firstHunk = section.indexOf('\n@@');
    const header = section.slice(0, firstHunk < 0 ? Math.min(section.length, 16384) : firstHunk);
    if (header.length > 16384) throw new Error();
    const parsed = Diff2Html.parse(header + '\n', parseOptions);
    if (parsed.length !== 1 || !parsed[0].oldName || !parsed[0].newName || parsed[0].isCombined) throw new Error();
    const file = parsed[0];
    if (file.oldName.length > 4096 || file.newName.length > 4096) throw new Error();
    const oversized = new TextEncoder().encode(section).length > limits.fileBytes || lines.length > limits.lines || lines.some(line => line.length > limits.lineLength);
    const binary = !!file.isBinary || /^Binary files .* differ$/m.test(section) || /^GIT binary patch$/m.test(section);
    const changes = firstHunk < 0 ? [] : section.slice(firstHunk).split('\n');
    const added = changes.filter(line => line.startsWith('+')).length;
    const deleted = changes.filter(line => line.startsWith('-')).length;
    const name = file.newName === '/dev/null' ? file.oldName : file.newName;
    files.push({ index, name, oldName: file.oldName, renamed: !!file.isRename, binary, oversized, added, deleted, key: await hash(identity + ':' + index) });
  }
  return files;
}

function renderFile(index, layout) {
  const section = sections[index];
  if (!section || new TextEncoder().encode(section).length > limits.fileBytes || section.split('\n').length > limits.lines) throw new Error();
  checkHunks(section);
  const parsed = Diff2Html.parse(section, parseOptions);
  if (parsed.length !== 1 || parsed[0].isCombined || parsed[0].isTooBig) throw new Error();
  const file = parsed[0];
  const html = Diff2Html.html(parsed, { outputFormat: layout === 'side-by-side' ? layout : 'line-by-line', drawFileList: false, matching: 'none', maxLineLengthHighlight: 0, matchingMaxComparisons: 0, renderNothingWhenEmpty: false });
  if (html.length > 4194304) throw new Error();
  const extension = (file.newName || file.oldName).split('.').pop().toLowerCase();
  const languages = { go: 'go', js: 'javascript', ts: 'typescript', json: 'json', html: 'xml', templ: 'xml', css: 'css', py: 'python', rb: 'ruby', rs: 'rust', sh: 'bash', sql: 'sql', yaml: 'yaml', yml: 'yaml', md: 'markdown', java: 'java', c: 'c', h: 'c', cpp: 'cpp' };
  const language = languages[extension];
  const highlights = [];
  let bytes = 0;
  if (language && hljs.getLanguage(language)) {
    for (const block of file.blocks) for (const line of block.lines) {
      const text = line.content.slice(1);
      bytes += text.length;
      if (text.length > 1000 || bytes > 65536 || highlights.length >= 500) continue;
      highlights.push([text, hljs.highlight(text, { language, ignoreIllegals: true }).value]);
    }
  }
  return { html, highlights };
}

self.onmessage = async event => {
  const { id, action, text, identity, index, layout } = event.data;
  try {
    const value = action === 'index' ? await indexPatch(text, identity) : renderFile(index, layout);
    self.postMessage({ id, value });
  } catch {
    self.postMessage({ id, error: 'Malformed or unsupported diff, or rendering limit exceeded. No source was rendered.' });
  }
};
