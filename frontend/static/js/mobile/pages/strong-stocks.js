(function () {
  'use strict';

  const U = window.MobileUtil;
  const SORT_KEY = 'strongStocksSort';
  const SETUP_W = { prox: 0.35, tight: 0.25, hold: 0.20, trend: 0.20 };
  const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

  let sortMode = 'adj';
  try {
    const saved = localStorage.getItem(SORT_KEY);
    if (saved === 'ready' || saved === 'adj' || saved === 'raw') sortMode = saved;
  } catch (e) {}

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
    pageLabel: 'Strong',
    weeklyDisposition: 'strong',
    fetchStocks: cap => fetch('/api/frontend/strong-stocks/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: stocks => [...stocks].sort((a, b) => {
      const d = sortKey(b) - sortKey(a);
      if (d) return d;
      return (b.market_cap || 0) - (a.market_cap || 0) || (a.ticker || '').localeCompare(b.ticker || '');
    }),
    listValueLabel: 'TI65',
    listValueFn: listValue,
    listMetaFn: s => s.adjusted_ti65 != null ? s.adjusted_ti65.toFixed(2) : '',
    extraFilterHtml:
      '<div class="strip recency-strip" role="tablist" aria-label="Sort">' +
      '<span class="strip-label">Sort</span>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'adj' ? ' active' : '') + '" data-sort="adj">Adj</button>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'raw' ? ' active' : '') + '" data-sort="raw">Raw</button>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'ready' ? ' active' : '') + '" data-sort="ready">Ready</button>' +
      '</div>',
    onSetup: app => {
      screenerApp = app;
      document.querySelectorAll('[data-sort]').forEach(btn => {
        btn.addEventListener('click', () => {
          sortMode = btn.dataset.sort;
          try { localStorage.setItem(SORT_KEY, sortMode); } catch (e) {}
          document.querySelectorAll('[data-sort]').forEach(b => b.classList.toggle('active', b === btn));
          app.loadData(app.currentCap);
        });
      });
    },
  });
})();
