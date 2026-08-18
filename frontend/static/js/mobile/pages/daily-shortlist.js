(function () {
  'use strict';

  const state = { date: null, data: null, tab: 'PICK', q: '' };

  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('dsSearch').addEventListener('input', () => {
      state.q = document.getElementById('dsSearch').value.trim().toLowerCase();
      render();
    });
    document.querySelectorAll('#dsTabs [data-tab]').forEach(btn => {
      btn.addEventListener('click', () => {
        state.tab = btn.dataset.tab;
        document.querySelectorAll('#dsTabs [data-tab]').forEach(b => b.classList.toggle('active', b === btn));
        render();
      });
    });
    loadDates();
  });

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
  }

  function fmtRet(v) {
    if (v == null) return '—';
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return (n >= 0 ? '+' : '') + n.toFixed(1) + '%';
  }

  function retCls(v) {
    if (v == null) return 'ret-neutral';
    return Number(v) >= 0 ? 'ret-pos' : 'ret-neg';
  }

  async function loadDates() {
    const sel = document.getElementById('dsDate');
    try {
      const r = await fetch('/api/frontend/daily-shortlist/dates');
      const data = await r.json();
      const dates = (data.dates || []).filter(d => d.has_audit);
      if (!dates.length) {
        sel.innerHTML = '<option value="">(no runs)</option>';
        document.getElementById('dsList').innerHTML = '<div class="md-empty">No daily shortlist runs yet.</div>';
        return;
      }
      sel.innerHTML = dates.map(d => '<option value="' + esc(d.date) + '">' + esc(d.date) + '</option>').join('');
      state.date = dates[0].date;
      sel.value = state.date;
      sel.addEventListener('change', () => {
        state.date = sel.value;
        loadAudit();
      });
      await loadAudit();
    } catch (e) {
      document.getElementById('dsList').innerHTML = '<div class="md-empty">Failed to load dates.</div>';
    }
  }

  async function loadAudit() {
    if (!state.date) return;
    document.getElementById('dsList').innerHTML = '<div class="md-loading">Loading ' + esc(state.date) + '…</div>';
    try {
      const r = await fetch('/api/frontend/daily-shortlist/' + encodeURIComponent(state.date));
      if (!r.ok) throw new Error('HTTP ' + r.status);
      state.data = await r.json();
    } catch (e) {
      document.getElementById('dsList').innerHTML = '<div class="md-empty">Failed to load audit.</div>';
      return;
    }
    const c = (state.data && state.data.verdict_counts) || {};
    document.getElementById('dsCountPick').textContent = c.PICK || 0;
    document.getElementById('dsCountWatch').textContent = c.WATCH || 0;
    document.getElementById('dsCountSkip').textContent = c.SKIP || 0;
    document.getElementById('dsMeta').textContent = 'n=' + (state.data.total_universe || 0);
    render();
  }

  function matches(r) {
    if ((r.verdict || 'SKIP') !== state.tab) return false;
    if (!state.q) return true;
    const hay = [
      r.ticker, r.company_name, r.sector, r.industry,
      (r.matched_themes || []).join(' '),
      (r.news_theme_tags || []).join(' '),
      r.verdict_rationale, r.drop_reason,
    ].filter(Boolean).join(' ').toLowerCase();
    return hay.includes(state.q);
  }

  function render() {
    const list = document.getElementById('dsList');
    if (!state.data || !state.data.rows) {
      list.innerHTML = '<div class="md-empty">No data.</div>';
      return;
    }
    const rows = state.data.rows.filter(matches)
      .sort((a, b) => (b.composite_score || 0) - (a.composite_score || 0));
    if (!rows.length) {
      list.innerHTML = '<div class="md-empty">No ' + state.tab + ' rows match.</div>';
      return;
    }
    list.innerHTML = rows.map(r => {
      const themes = (r.matched_themes && r.matched_themes.length)
        ? r.matched_themes : (r.news_theme_tags || []);
      const chips = (r.sources || []).concat(themes).slice(0, 6)
        .map(t => '<span class="ds-chip">' + esc(t) + '</span>').join('');
      const why = r.verdict_rationale || r.drop_reason || '';
      return '<a class="ds-card" href="/m/stock/' + encodeURIComponent(r.ticker) + '">' +
        '<div class="ds-card-head">' +
          '<span class="ds-tk">' + esc(r.ticker) + '</span>' +
          '<span class="ds-co">' + esc(r.company_name || '') + '</span>' +
          (r.composite_score != null
            ? '<span class="ds-score">' + Number(r.composite_score).toFixed(1) + '</span>'
            : '') +
        '</div>' +
        '<div class="ds-card-rets">' +
          '<span class="' + retCls(r.dr_1) + '">1D ' + fmtRet(r.dr_1) + '</span>' +
          '<span class="' + retCls(r.dr_5) + '">5D ' + fmtRet(r.dr_5) + '</span>' +
          '<span class="' + retCls(r.dr_20) + '">20D ' + fmtRet(r.dr_20) + '</span>' +
        '</div>' +
        (chips ? '<div class="ds-chips">' + chips + '</div>' : '') +
        (why ? '<div class="ds-why">' + esc(why) + '</div>' : '') +
      '</a>';
    }).join('');
  }
})();
