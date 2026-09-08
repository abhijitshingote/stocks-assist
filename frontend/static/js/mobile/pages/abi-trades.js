(function () {
  'use strict';

  const U = window.MobileUtil;
  const SORT_KEY = 'abiTradesSort';
  let sortMode = 'dr1';
  try {
    const saved = localStorage.getItem(SORT_KEY);
    if (saved === 'dr1' || saved === 'added') sortMode = saved;
  } catch (e) {}

  function addedTs(s) {
    const t = Date.parse(s.trade_added_at || '');
    return Number.isNaN(t) ? -Infinity : t;
  }

  function sideHtml(s) {
    if (!s.trade_side) return '';
    return '<span class="trade-side ' + U.escAttr(s.trade_side) + '">' + U.escAttr(s.trade_side) + '</span>';
  }

  window.MobileScreener.init({
    pageTitle: 'Trades',
    pageLabel: 'Trades',
    usesCapFilter: false,
    showTi65: true,
    showRank: false,
    notesFromStock: true,
    watchlistFromStock: true,
    tradeRemove: true,
    fetchStocks: () => fetch('/api/frontend/abi-trades/data')
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: stocks => [...stocks].sort((a, b) => {
      if (sortMode === 'added') {
        const d = addedTs(b) - addedTs(a);
        if (d) return d;
        return (a.ticker || '').localeCompare(b.ticker || '');
      }
      const av = a.dr_1, bv = b.dr_1;
      if (av == null) return 1;
      if (bv == null) return -1;
      return bv - av;
    }),
    listValueFn: s => U.fmtRet(s.dr_1),
    listValueClsFn: s => U.retCls(s.dr_1),
    listValueLabel: '1D',
    listBadgeFn: sideHtml,
    onSetup: app => {
      const toolbar = document.querySelector('.dr-toolbar');
      if (!toolbar) return;
      const strip = document.createElement('div');
      strip.className = 'strip sort-strip';
      strip.setAttribute('role', 'tablist');
      strip.setAttribute('aria-label', 'Sort');
      strip.innerHTML =
        '<button class="pill recency-pill' + (sortMode === 'dr1' ? ' active' : '') + '" type="button" data-sort="dr1">1D</button>' +
        '<button class="pill recency-pill' + (sortMode === 'added' ? ' active' : '') + '" type="button" data-sort="added">Added</button>';
      toolbar.appendChild(strip);
      strip.querySelectorAll('[data-sort]').forEach(btn => {
        btn.addEventListener('click', () => {
          sortMode = btn.dataset.sort;
          try { localStorage.setItem(SORT_KEY, sortMode); } catch (e) {}
          strip.querySelectorAll('[data-sort]').forEach(b => b.classList.toggle('active', b === btn));
          app.loadData(app.currentCap);
        });
      });
    },
  });
})();
