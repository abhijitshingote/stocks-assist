/* What moved markets — shared desktop + mobile renderer for /narratives, /m/narratives.
 * Data: /api/frontend/narratives → user_data/market_narratives/<run>/events.json
 */
(function () {
  'use strict';

  const API = '/api/frontend/narratives';
  const GRADES = { 3: 'major', 2: 'notable', 1: 'minor' };

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }

  const fmtDate = (d) => (d
    ? new Date(d + 'T12:00:00Z').toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' })
    : '');
  const fmtRun = (d) => (d
    ? new Date(d).toLocaleString('en-US', {
      month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit',
    })
    : 'unknown');

  function init(opts) {
    const root = document.getElementById(opts.rootId);
    const mobile = !!opts.mobile;
    const state = { payload: null, run: null, poll: null, grade: Number(localStorage.getItem('nvGrade')) || 1 };
    const $ = (sel) => root.querySelector(sel);
    const href = (s) => `${mobile ? '/m' : ''}/stock/${encodeURIComponent(s)}`;

    function chip(m) {
      const sym = m.split(/\s+/)[0];
      const cls = /\+\d/.test(m) ? 'up' : /[-−]\d/.test(m) ? 'down' : '';
      const body = esc(m);
      return /^[A-Z][A-Z.\-]{0,6}$/.test(sym)
        ? `<a class="nv-mv ${cls}" href="${href(sym)}">${body}</a>`
        : `<span class="nv-mv ${cls}">${body}</span>`;
    }
    const moves = (e) => (e.moves && e.moves.length ? `<div class="nv-mvs">${e.moves.map(chip).join('')}</div>` : '');

    function setStatus(st) {
      const el = $('#nvStatus');
      const btn = $('#nvRebuild');
      const running = st && st.status === 'running';
      btn.disabled = running;
      if (running) {
        el.textContent = `rebuilding · ${st.stage || 'queued'}…`;
        el.className = 'nv-status running';
      } else if (st && st.status === 'failed') {
        el.textContent = `last rebuild failed: ${st.error || st.stage || ''}`;
        el.className = 'nv-status failed';
      } else {
        el.textContent = '';
        el.className = 'nv-status';
      }
      if (running && !state.poll) {
        state.poll = setInterval(load, 5000);
      } else if (!running && state.poll) {
        clearInterval(state.poll);
        state.poll = null;
      }
    }

    async function load() {
      try {
        const r = await fetch(API);
        const j = await r.json();
        const wasRunning = !!state.poll;
        setStatus(j.status);
        if (!state.payload || wasRunning || j.run !== state.run) {
          state.payload = j.data;
          state.run = j.run;
          render();
        }
      } catch (e) {
        $('#nvGraph').innerHTML = `<div class="nv-empty">Failed to load: ${esc(e.message)}</div>`;
      }
    }

    async function rebuild() {
      if (!confirm('Rebuild from all briefs? One Sonnet call, ~$0.31, ~4 min.')) return;
      const r = await fetch(API + '/generate', { method: 'POST' });
      const j = await r.json().catch(() => ({}));
      if (!r.ok && r.status !== 409) {
        alert(j.error || 'Failed to start rebuild');
        return;
      }
      setStatus({ status: 'running', stage: 'queued' });
    }

    function render() {
      root.querySelectorAll('#nvGrade .nv-chip').forEach((b) => b.classList.toggle('active', Number(b.dataset.val) === state.grade));
      const p = state.payload;
      if (!p) {
        $('#nvMeta').textContent = '';
        $('#nvGraph').innerHTML = '<div class="nv-empty">Nothing yet. Rebuild, or run <code>python -m market_brief.narratives</code>.</div>';
        return;
      }
      const w = p.window;
      const cost = p.cost_usd != null ? ` · $${Number(p.cost_usd).toFixed(2)}` : '';
      $('#nvMeta').textContent = `Last run ${fmtRun(p.generated_at)}${cost}`;

      const events = p.events.filter((e) => e.date && e.grade >= state.grade);
      if (!events.length) {
        $('#nvGraph').innerHTML = '<div class="nv-empty">No events at this grade.</div>';
        $('#nvDetail').innerHTML = '';
        return;
      }
      const node = (e) => `
        <article class="nv-node g${e.grade}" title="${esc(e.why || e.headline)}">
          <span class="nv-node-date">${fmtDate(e.date)}</span>
          <span class="nv-node-head">${esc(e.headline)}</span>
          ${moves(e)}
        </article>`;
      const graph = $('#nvGraph');
      graph.className = `nv-graph grade-${state.grade}`;
      graph.innerHTML = [3, 2, 1].map((grade) => {
        const lane = events.filter((e) => e.grade === grade);
        if (!lane.length) return '';
        return `<section class="nv-lane g${grade}">
          <div class="nv-lane-label">${GRADES[grade]}</div>
          <div class="nv-nodes">${lane.map(node).join('')}</div>
        </section>`;
      }).join('');
    }

    root.addEventListener('click', (ev) => {
      if (ev.target.closest('#nvRebuild')) rebuild();
      const g = ev.target.closest('#nvGrade .nv-chip');
      if (g) {
        state.grade = Number(g.dataset.val);
        localStorage.setItem('nvGrade', state.grade);
        render();
      }
    });
    load();
  }

  window.Narratives = { init };
})();
