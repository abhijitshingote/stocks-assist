(function () {
  'use strict';

  const U = window.MobileUtil;
  const SORT_KEY = 'strongStocksSort';
  const EXCLUDE_KEY = 'strongStocksExclude';
  const EXCLUDE_RULES = [
    { id: 'biotech', field: 'industry', value: 'Biotechnology', label: 'Biotech' },
  ];
  const SETUP_W = { prox: 0.35, tight: 0.25, hold: 0.20, trend: 0.20 };
  const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

  let sortMode = 'adj';
  try {
    const saved = localStorage.getItem(SORT_KEY);
    if (saved === 'ready' || saved === 'adj' || saved === 'raw') sortMode = saved;
  } catch (e) {}

  const defaultExcluded = EXCLUDE_RULES.map(r => r.id);
  let excluded = new Set(defaultExcluded);
  try {
    const savedEx = JSON.parse(localStorage.getItem(EXCLUDE_KEY));
    if (Array.isArray(savedEx)) excluded = new Set(savedEx.filter(id => EXCLUDE_RULES.some(r => r.id === id)));
  } catch (e) {}

  function persistExcluded() {
    try { localStorage.setItem(EXCLUDE_KEY, JSON.stringify([...excluded])); } catch (e) {}
  }

  function isExcluded(s) {
    return EXCLUDE_RULES.some(r => excluded.has(r.id) && s[r.field] === r.value);
  }

  let setupMap = null;
  let screenerApp = null;

  function setupParts(m) {
    if (!m) return { total: 0 };
    const distAtr = m.ma_dist_atr != null ? m.ma_dist_atr : Math.abs(m.ma_dist_pct) / 2;
    const prox = clamp(1 - distAtr / 1.5, 0, 1);
    const tight = clamp((3.5 - m.range10_atr) / 2, 0, 1);
    const pos = m.pos_in_range10 == null ? 0.5 : m.pos_in_range10;
    const hold = clamp((pos - 0.4) / 0.5, 0, 1);
    const trend = m.above_all_ma ? 1 : m.above_ema20 ? 0.6 : 0;
    return {
      total: SETUP_W.prox * prox + SETUP_W.tight * tight +
        SETUP_W.hold * hold + SETUP_W.trend * trend,
    };
  }

  fetch('/api/frontend/strong-stocks-setup')
    .then(r => r.json())
    .then(m => {
      if (!m || m.error) return;
      setupMap = m;
      if (screenerApp) screenerApp.loadData(screenerApp.currentCap);
    })
    .catch(() => {});

  function listValue(s) {
    return s.ti65 != null ? s.ti65.toFixed(2) : '—';
  }

  function sortKey(s) {
    if (sortMode === 'ready') {
      return setupParts(setupMap ? setupMap[s.ticker] : null).total;
    }
    if (sortMode === 'raw') return s.ti65 == null ? -Infinity : s.ti65;
    const v = s.adjusted_ti65;
    return v == null ? -Infinity : v;
  }

  window.MobileScreener.init({
    pageTitle: 'Strong Stocks',
    fetchStocks: cap => fetch('/api/frontend/strong-stocks/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: stocks => [...stocks].sort((a, b) => {
      const d = sortKey(b) - sortKey(a);
      if (d) return d;
      return (b.market_cap || 0) - (a.market_cap || 0);
    }),
    listValueLabel: 'TI65',
    listValueFn: listValue,
    extraFilterHtml:
      '<div class="strip recency-strip" role="group" aria-label="Exclude">' +
      '<span class="strip-label">Excl</span>' +
      EXCLUDE_RULES.map(r =>
        '<button type="button" class="pill recency-pill' + (excluded.has(r.id) ? ' active' : '') +
        '" data-exclude="' + r.id + '">− ' + r.label + '</button>'
      ).join('') +
      '</div>' +
      '<div class="strip recency-strip" role="tablist" aria-label="Sort">' +
      '<span class="strip-label">Sort</span>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'adj' ? ' active' : '') + '" data-sort="adj">Adj</button>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'raw' ? ' active' : '') + '" data-sort="raw">Raw</button>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'ready' ? ' active' : '') + '" data-sort="ready">Ready</button>' +
      '</div>',
    filterStocks: stocks => stocks.filter(s => !isExcluded(s)),
    onSetup: app => {
      screenerApp = app;
      document.querySelectorAll('[data-exclude]').forEach(btn => {
        btn.addEventListener('click', () => {
          const id = btn.dataset.exclude;
          if (excluded.has(id)) excluded.delete(id);
          else excluded.add(id);
          persistExcluded();
          btn.classList.toggle('active', excluded.has(id));
          app.renderList();
          app.updateCounts();
        });
      });
      document.querySelectorAll('[data-sort]').forEach(btn => {
        btn.addEventListener('click', () => {
          sortMode = btn.dataset.sort;
          try { localStorage.setItem(SORT_KEY, sortMode); } catch (e) {}
          document.querySelectorAll('[data-sort]').forEach(b => b.classList.toggle('active', b === btn));
          app.loadData(app.currentCap);
        });
      });
    },
    renderList: (visible, app) => {
      const chipStrip = document.getElementById('chipStrip');
      const tbody = document.getElementById('stockTableBody');

      chipStrip.innerHTML = visible.slice(0, 24).map(s => {
        const active = s.ticker === app.selectedTicker ? ' active' : '';
        return '<div class="tchip' + active + '" data-ticker="' + U.escAttr(s.ticker) + '">' +
          '<span class="tk">' + U.escAttr(s.ticker) +
          '</span><span class="ret">' + listValue(s) + '</span></div>';
      }).join('');

      tbody.innerHTML = visible.map((s, i) => {
        const active = s.ticker === app.selectedTicker ? ' active' : '';
        const adj = s.adjusted_ti65 != null ? s.adjusted_ti65.toFixed(2) : '—';
        return '<tr class="' + active.trim() + '" data-ticker="' + U.escAttr(s.ticker) + '">' +
          '<td>' + (i + 1) + '</td>' +
          '<td>' + U.escAttr(s.ticker) + '</td>' +
          '<td>' + U.fmtMktCap(s.market_cap) + '</td>' +
          '<td>' + listValue(s) + '</td>' +
          '<td class="stars muted" style="font-size:0.5rem">' + U.escAttr(adj) + '</td></tr>';
      }).join('');

      chipStrip.querySelectorAll('.tchip').forEach(c => {
        c.addEventListener('click', () => app.selectStock(c.dataset.ticker));
      });
      tbody.querySelectorAll('tr').forEach(r => {
        r.addEventListener('click', () => app.selectStock(r.dataset.ticker));
      });
    },
  });
})();
