(function () {
  'use strict';

  let runs = [];
  let currentKey = null;
  let currentData = null;
  let showSources = false;
  let pollTimer = null;

  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('btnRun').addEventListener('click', generate);
    document.getElementById('btnSources').addEventListener('click', toggleSources);
    loadRuns();
  });

  function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text == null ? '' : text;
    return div.innerHTML;
  }

  function todayET() {
    const s = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
    const [y, m, d] = s.split('-').map(Number);
    return new Date(y, m - 1, d, 12, 0, 0);
  }

  function iso(d) {
    const y = d.getFullYear();
    const mo = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return y + '-' + mo + '-' + day;
  }

  function rangeLabel(startSat, fri) {
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const same = startSat.getMonth() === fri.getMonth();
    return same
      ? months[startSat.getMonth()] + ' ' + startSat.getDate() + '–' + fri.getDate()
      : months[startSat.getMonth()] + ' ' + startSat.getDate() + '–' +
        months[fri.getMonth()] + ' ' + fri.getDate();
  }

  /** Last N completed Sat→Sat weeks in ET. Saturday = close of the week
   *  that just finished (same as fetch.week_bounds). */
  function candidateWeeks(n) {
    const weeks = [];
    const sat = todayET();
    sat.setDate(sat.getDate() - ((sat.getDay() + 1) % 7));
    for (let i = 0; i < n; i++) {
      const endSat = new Date(sat);
      endSat.setDate(sat.getDate() - i * 7);
      const fri = new Date(endSat);
      fri.setDate(endSat.getDate() - 1);
      const startSat = new Date(endSat);
      startSat.setDate(endSat.getDate() - 7);
      weeks.push({
        weekEnding: iso(endSat),
        runKey: 'week-' + iso(fri),
        label: rangeLabel(startSat, fri),
      });
    }
    return weeks;
  }

  function railWeeks() {
    const byKey = new Map(runs.map(r => [r.run_key, r]));
    const seen = new Set();
    const out = [];
    candidateWeeks(12).forEach(w => {
      const r = byKey.get(w.runKey);
      seen.add(w.runKey);
      out.push(r ? Object.assign({ label: w.label, weekEnding: w.weekEnding }, r)
                 : { run_key: w.runKey, window_label: w.label, label: w.label,
                     weekEnding: w.weekEnding, status: 'empty' });
    });
    runs.forEach(r => {
      if (!seen.has(r.run_key)) out.push(r);
    });
    return out;
  }

  function statusLabel(st) {
    if (st === 'running') return 'Running';
    if (st === 'complete') return 'Ready';
    if (st === 'failed' || st === 'error') return 'Failed';
    if (st === 'empty') return 'Empty';
    return st || '—';
  }

  function chipLabel(w) {
    if (w.label) return w.label;
    if (w.window_label) return w.window_label.replace(/, \d{4}$/, '');
    return w.run_key || '';
  }

  function weekEndingFor(w) {
    if (w.weekEnding) return w.weekEnding;
    const found = candidateWeeks(12).find(c => c.runKey === w.run_key);
    return found ? found.weekEnding : null;
  }

  function updateRunButton(w) {
    const btn = document.getElementById('btnRun');
    const st = w && w.status;
    if (st === 'running') {
      btn.textContent = 'Running…';
      btn.disabled = true;
    } else if (st === 'complete') {
      btn.textContent = 'Generate';
      btn.disabled = true;
    } else {
      btn.textContent = 'Generate';
      btn.disabled = !weekEndingFor(w);
    }
  }

  async function loadRuns() {
    const strip = document.getElementById('weekStrip');
    strip.innerHTML = '<span class="md-chip">Loading…</span>';
    try {
      const res = await fetch('/api/frontend/weekly-recap/runs');
      const data = await res.json();
      runs = data.runs || [];
      const weeks = railWeeks();
      strip.innerHTML = weeks.map(w => {
        const st = w.status || 'empty';
        const active = currentKey === w.run_key ? ' active' : '';
        return '<button type="button" class="md-chip ' + st + active +
          '" data-key="' + escapeHtml(w.run_key) + '">' +
          escapeHtml(chipLabel(w)) +
          '<span class="status">' + statusLabel(st) + '</span></button>';
      }).join('');
      strip.querySelectorAll('.md-chip[data-key]').forEach(chip => {
        chip.addEventListener('click', () => loadWeek(chip.dataset.key));
      });

      const running = runs.find(r => r.status === 'running');
      if (running) startPolling();
      else stopPolling();

      const target = currentKey || (weeks[0] && weeks[0].run_key);
      if (target) loadWeek(target);
      else {
        document.getElementById('wrContent').innerHTML =
          '<div class="md-empty">No weeks yet.</div>';
      }
    } catch (e) {
      strip.innerHTML = '<span class="md-empty">Error: ' + escapeHtml(e.message) + '</span>';
    }
  }

  function currentWeek() {
    return railWeeks().find(w => w.run_key === currentKey) || null;
  }

  async function loadWeek(runKey) {
    currentKey = runKey;
    const w = currentWeek();
    document.getElementById('wrTitle').textContent = (w && (w.window_label || w.label)) || runKey;
    document.querySelectorAll('#weekStrip .md-chip[data-key]').forEach(chip => {
      chip.classList.toggle('active', chip.dataset.key === runKey);
    });
    updateRunButton(w);
    document.getElementById('btnSources').classList.toggle('active', showSources);

    if (!w || w.status === 'empty') {
      currentData = null;
      document.getElementById('runProgressBanner').innerHTML = '';
      document.getElementById('wrContent').innerHTML =
        '<div class="generate-prompt"><p>No recap for <strong>' +
        escapeHtml((w && (w.label || w.window_label)) || runKey) +
        '</strong> yet.</p>' +
        '<button type="button" id="btnGenerateInline">Generate</button></div>';
      document.getElementById('btnGenerateInline').addEventListener('click', generate);
      return;
    }

    if (w.status === 'running') {
      renderBanner(w);
      document.getElementById('wrContent').innerHTML =
        '<div class="md-empty">Pipeline running.</div>';
      startPolling();
      return;
    }

    document.getElementById('runProgressBanner').innerHTML = '';
    document.getElementById('wrContent').innerHTML = '<div class="md-loading">Loading…</div>';
    try {
      const res = await fetch('/api/frontend/weekly-recap/run/' + encodeURIComponent(runKey));
      currentData = await res.json();
      if (currentData.error) throw new Error(currentData.error);
      renderContent();
    } catch (e) {
      document.getElementById('wrContent').innerHTML =
        '<div class="md-empty">' + escapeHtml(e.message) + '</div>';
    }
  }

  function inlineMarkdown(text) {
    let html = escapeHtml(text);
    html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');
    html = html.replace(/`([^`]+)`/g, '<code>$1</code>');
    return html;
  }

  function simpleMarkdown(md) {
    const out = [];
    let inList = false;
    (md || '').split('\n').forEach(line => {
      const isLi = /^\s*[-*] /.test(line);
      if (inList && !isLi) { out.push('</ul>'); inList = false; }
      if (/^### /.test(line)) out.push('<h3>' + inlineMarkdown(line.slice(4)) + '</h3>');
      else if (/^## /.test(line)) out.push('<h2>' + inlineMarkdown(line.slice(3)) + '</h2>');
      else if (/^# /.test(line)) out.push('<h1>' + inlineMarkdown(line.slice(2)) + '</h1>');
      else if (/^---+$/.test(line.trim())) out.push('<hr>');
      else if (isLi) {
        if (!inList) { out.push('<ul>'); inList = true; }
        out.push('<li>' + inlineMarkdown(line.replace(/^\s*[-*] /, '')) + '</li>');
      } else if (line.trim() === '') out.push('');
      else out.push('<p>' + inlineMarkdown(line) + '</p>');
    });
    if (inList) out.push('</ul>');
    return out.join('\n');
  }

  function sourcesHtml() {
    const items = (currentData || {}).items || [];
    if (!showSources || !items.length) return '';
    return '<div class="wr-sources"><div class="wr-sources-title">Sources (' +
      items.length + ')</div>' +
      items.map(i => {
        const title = i.url
          ? '<a href="' + escapeHtml(i.url) + '" target="_blank" rel="noopener">' +
            escapeHtml(i.title) + '</a>'
          : escapeHtml(i.title);
        return '<div class="wr-source"><span class="wr-source-score">' +
          escapeHtml(String(i.score == null ? '' : i.score)) +
          '</span><span class="wr-source-title">' + title + '</span></div>';
      }).join('') + '</div>';
  }

  function renderContent() {
    const run = (currentData || {}).run || {};
    const el = document.getElementById('wrContent');
    if (!run.narrative_md) {
      el.innerHTML = '<div class="md-empty">No recap for this week.</div>';
      return;
    }
    el.innerHTML = '<div class="markdown-body">' + simpleMarkdown(run.narrative_md) +
      '</div>' + sourcesHtml();
  }

  function toggleSources() {
    showSources = !showSources;
    document.getElementById('btnSources').classList.toggle('active', showSources);
    if (currentData) renderContent();
  }

  function renderBanner(run) {
    document.getElementById('runProgressBanner').innerHTML =
      '<div class="brief-progress"><strong>' +
      escapeHtml(run.window_label || 'Run') + ' in progress</strong>' +
      (run.stage ? ' · ' + escapeHtml(run.stage) : '') + '</div>';
  }

  function startPolling() {
    if (pollTimer) return;
    pollTimer = setInterval(async () => {
      try {
        const res = await fetch('/api/frontend/weekly-recap/status');
        const run = await res.json();
        if (run.status === 'running') renderBanner(run);
        else {
          stopPolling();
          currentKey = run.run_key || currentKey;
          loadRuns();
        }
      } catch (e) { /* keep polling */ }
    }, 5000);
  }

  function stopPolling() {
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  }

  async function generate() {
    const w = currentWeek();
    const weekEnding = weekEndingFor(w);
    if (!weekEnding || (w && w.status === 'complete')) return;
    const btn = document.getElementById('btnRun');
    btn.disabled = true;
    try {
      const res = await fetch('/api/frontend/weekly-recap/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ week_ending: weekEnding }),
      });
      const data = await res.json();
      if (data.error) {
        document.getElementById('runProgressBanner').innerHTML =
          '<div class="brief-progress">' + escapeHtml(data.error) + '</div>';
        updateRunButton(w);
      } else {
        startPolling();
        document.getElementById('wrContent').innerHTML =
          '<div class="md-empty">Pipeline running.</div>';
      }
    } finally {
      if (!pollTimer) updateRunButton(currentWeek());
    }
  }
})();
