(function () {
  'use strict';

  const U = window.MobileUtil;
  const SORT_KEY = 'topReturns520Sort';
  const SETUP_W = { prox: 0.35, tight: 0.25, hold: 0.20, trend: 0.20 };
  const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

  let sortMode = 'adj';
  try {
    const saved = localStorage.getItem(SORT_KEY);
    if (saved === 'flat' || saved === 'adj' || saved === 'ready') sortMode = saved;
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
    return U.fmtRet(s.dr_5);
  }

  function sortKey(s) {
    if (sortMode === 'ready') {
      return setupParts(setupMap ? setupMap[s.ticker] : null).total;
    }
    const v = sortMode === 'adj' ? s.adjusted_dr_5 : s.dr_5;
    return v == null ? -Infinity : v;
  }

  function setChip(s) {
    const parts = [];
    if (s.in_5d) parts.push('5');
    if (s.in_20d) parts.push('20');
    return parts.join('/');
  }

  window.MobileScreener.init({
    pageTitle: 'Top 5D/20D',
    fetchStocks: cap => fetch('/api/frontend/top-returns-5-20/all')
      .then(r => r.json())
      .then(data => {
        const rows = Array.isArray(data) ? data : [];
        if (!cap || cap === 'all') return rows;
        return rows.filter(s => s.cap_bucket === cap);
      }),
    sortStocks: stocks => [...stocks].sort((a, b) => {
      const d = sortKey(b) - sortKey(a);
      if (d) return d;
      return (b.market_cap || 0) - (a.market_cap || 0);
    }),
    listValueLabel: '5D',
    listValueFn: listValue,
    extraFilterHtml:
      '<div class="strip recency-strip" role="tablist" aria-label="Sort">' +
      '<span class="strip-label">Sort</span>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'adj' ? ' active' : '') + '" data-sort="adj">Adj 5D</button>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'flat' ? ' active' : '') + '" data-sort="flat">5D</button>' +
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
        const score = sortMode === 'ready'
          ? setupParts(setupMap ? setupMap[s.ticker] : null).total.toFixed(2)
          : (s.adjusted_dr_5 != null ? s.adjusted_dr_5.toFixed(1) : '—');
        const set = setChip(s);
        return '<tr class="' + active.trim() + '" data-ticker="' + U.escAttr(s.ticker) + '">' +
          '<td>' + (i + 1) + '</td>' +
          '<td>' + U.escAttr(s.ticker) + (set ? ' <span class="evt-badge">' + set + '</span>' : '') + '</td>' +
          '<td>' + U.fmtMktCap(s.market_cap) + '</td>' +
          '<td>' + listValue(s) + '</td>' +
          '<td class="stars muted" style="font-size:0.5rem">' + U.escAttr(score) + '</td></tr>';
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
