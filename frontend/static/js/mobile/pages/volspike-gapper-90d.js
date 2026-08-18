(function () {
  'use strict';

  const U = window.MobileUtil;
  const SORT_KEY = 'vsg90dSort';
  const SETUP_W = { prox: 0.35, tight: 0.25, hold: 0.20, trend: 0.20 };
  const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

  let sortMode = 'adj';
  try {
    const saved = localStorage.getItem(SORT_KEY);
    if (saved === 'ready' || saved === 'adj' || saved === 'date') sortMode = saved;
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

  fetch('/api/frontend/volspike-gapper-setup')
    .then(r => r.json())
    .then(m => {
      if (!m || m.error) return;
      setupMap = m;
      if (screenerApp) screenerApp.loadData(screenerApp.currentCap);
    })
    .catch(() => {});

  function eventRetPct(s) {
    return s.last_event_return != null ? s.last_event_return * 100 : null;
  }

  function eventMag(s) {
    return VsgEvent.magStr(s, {compact: true}) || '—';
  }

  function listValue(s) {
    const v = eventRetPct(s);
    return v != null ? U.fmtRet(v) : eventMag(s);
  }

  function sortKey(s) {
    if (sortMode === 'ready') {
      return setupParts(setupMap ? setupMap[s.ticker] : null).total;
    }
    if (sortMode === 'date') return s.last_event_date ? Date.parse(s.last_event_date) : -Infinity;
    const v = s.adjusted_event_return;
    return v == null ? -Infinity : v;
  }

  window.MobileScreener.init({
    pageTitle: 'Vol Spike & Gaps (90d)',
    weeklyDisposition: 'vsg90',
    fetchStocks: cap => fetch('/api/frontend/volspike-gapper-90d/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: stocks => [...stocks].sort((a, b) => {
      const d = sortKey(b) - sortKey(a);
      if (d) return d;
      return (b.market_cap || 0) - (a.market_cap || 0) || (a.ticker || '').localeCompare(b.ticker || '');
    }),
    listValueLabel: 'Evt',
    listValueFn: listValue,
    extraFilterHtml:
      '<div class="strip recency-strip" role="tablist" aria-label="Sort">' +
      '<span class="strip-label">Sort</span>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'adj' ? ' active' : '') + '" data-sort="adj">Adj</button>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'ready' ? ' active' : '') + '" data-sort="ready">Ready</button>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'date' ? ' active' : '') + '" data-sort="date">Date</button>' +
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
        const badge = VsgEvent.badge(s);
        return '<div class="tchip' + active + '" data-ticker="' + U.escAttr(s.ticker) + '">' +
          '<span class="tk">' + U.escAttr(s.ticker) + (badge ? '<span class="evt-badge">' + badge + '</span>' : '') +
          '</span><span class="ret">' + listValue(s) + '</span></div>';
      }).join('');

      tbody.innerHTML = visible.map((s, i) => {
        const active = s.ticker === app.selectedTicker ? ' active' : '';
        const badge = VsgEvent.badge(s);
        const date = s.last_event_date || '';
        return '<tr class="' + active.trim() + '" data-ticker="' + U.escAttr(s.ticker) + '">' +
          '<td>' + (i + 1) + '</td>' +
          '<td>' + U.escAttr(s.ticker) + (badge ? ' <span class="evt-badge">' + badge + '</span>' : '') + '</td>' +
          '<td>' + U.fmtMktCap(s.market_cap) + '</td>' +
          '<td>' + listValue(s) + '</td>' +
          '<td class="stars muted" style="font-size:0.5rem">' + U.escAttr(date.slice(5)) + '</td></tr>';
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
