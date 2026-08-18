(function () {
  'use strict';

  const state = { date: null, data: null, tab: 'all', q: '' };

  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('dtSearch').addEventListener('input', () => {
      state.q = document.getElementById('dtSearch').value.trim().toLowerCase();
      render();
    });
    document.querySelectorAll('#dtTabs [data-tab]').forEach(btn => {
      btn.addEventListener('click', () => {
        state.tab = btn.dataset.tab;
        document.querySelectorAll('#dtTabs [data-tab]').forEach(b => b.classList.toggle('active', b === btn));
        render();
      });
    });
    loadDates();
  });

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
  }

  async function loadDates() {
    const sel = document.getElementById('dtDate');
    try {
      const r = await fetch('/api/frontend/daily-shortlist/themes/dates');
      const data = await r.json();
      const dates = data.dates || [];
      if (!dates.length) {
        sel.innerHTML = '<option value="">(no runs)</option>';
        document.getElementById('dtGrid').innerHTML = '<div class="md-empty">No theme artifacts yet.</div>';
        return;
      }
      sel.innerHTML = dates.map(d => '<option value="' + esc(d.date) + '">' + esc(d.date) + '</option>').join('');
      state.date = dates[0].date;
      sel.value = state.date;
      sel.addEventListener('change', () => {
        state.date = sel.value;
        loadThemes();
      });
      await loadThemes();
    } catch (e) {
      document.getElementById('dtGrid').innerHTML = '<div class="md-empty">Failed to load dates.</div>';
    }
  }

  async function loadThemes() {
    if (!state.date) return;
    document.getElementById('dtGrid').innerHTML = '<div class="md-loading">Loading ' + esc(state.date) + '…</div>';
    try {
      const r = await fetch('/api/frontend/daily-shortlist/themes/' + encodeURIComponent(state.date));
      if (!r.ok) throw new Error('HTTP ' + r.status);
      state.data = await r.json();
    } catch (e) {
      document.getElementById('dtGrid').innerHTML = '<div class="md-empty">Failed to load themes.</div>';
      return;
    }
    const themes = ((state.data.merged || {}).themes) || [];
    const srcs = t => t.sources || (t.source ? [t.source] : []);
    document.getElementById('dtNAll').textContent = themes.length;
    document.getElementById('dtNUser').textContent = themes.filter(t => srcs(t).includes('user_taste')).length;
    document.getElementById('dtNCur').textContent = themes.filter(t => srcs(t).includes('curated')).length;
    document.getElementById('dtNMkt').textContent = themes.filter(t => srcs(t).includes('hot_market')).length;
    const m = state.data.merged || {};
    document.getElementById('dtMeta').textContent =
      'user=' + (m.user_taste_count || 0) +
      ' curated=' + (m.curated_count || 0) +
      ' market=' + (m.hot_market_count || 0);
    render();
  }

  function matches(t) {
    const srcs = t.sources || (t.source ? [t.source] : []);
    if (state.tab !== 'all' && !srcs.includes(state.tab)) return false;
    if (!state.q) return true;
    const hay = [
      t.tag, t.label, (t.descriptions || []).join(' '), t.description,
      (t.example_tickers || []).join(' '), srcs.join(' '),
    ].filter(Boolean).join(' ').toLowerCase();
    return hay.includes(state.q);
  }

  function render() {
    const grid = document.getElementById('dtGrid');
    const themes = (((state.data || {}).merged || {}).themes) || [];
    const list = themes.filter(matches).sort((a, b) => (b.weight || 0) - (a.weight || 0));
    if (!list.length) {
      grid.innerHTML = '<div class="md-empty">No themes match.</div>';
      return;
    }
    grid.innerHTML = list.map(t => {
      const srcs = t.sources || (t.source ? [t.source] : []);
      const examples = (t.example_tickers || []).slice(0, 8)
        .map(tk => '<a class="ds-chip" href="/m/stock/' + encodeURIComponent(tk) + '">' + esc(tk) + '</a>')
        .join('');
      const desc = (t.descriptions && t.descriptions[0]) || t.description || '';
      return '<div class="dt-card">' +
        '<div class="dt-card-head">' +
          '<div><div class="dt-title">' + esc(t.label || t.tag) + '</div>' +
            '<div class="dt-tag">' + esc(t.tag || '') + '</div></div>' +
          '<span class="dt-w">w=' + (t.weight != null ? Number(t.weight).toFixed(1) : '?') + '</span>' +
        '</div>' +
        (srcs.length ? '<div class="ds-chips">' + srcs.map(s => '<span class="ds-chip">' + esc(s) + '</span>').join('') + '</div>' : '') +
        (desc ? '<div class="ds-why">' + esc(desc) + '</div>' : '') +
        (examples ? '<div class="ds-chips">' + examples + '</div>' : '') +
      '</div>';
    }).join('');
  }
})();
