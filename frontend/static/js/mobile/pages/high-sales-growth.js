(function () {
  'use strict';

  const U = window.MobileUtil;
  let sortMode = 'return';

  function psAvg(s) {
    const vals = [s.ps_t, s.ps_t_plus_1].filter(v => v != null && v > 0);
    if (!vals.length) return null;
    return vals.reduce((a, b) => a + b, 0) / vals.length;
  }

  function applySort(stocks) {
    if (sortMode === 'expensive') {
      return [...stocks].sort((a, b) => {
        const av = psAvg(a), bv = psAvg(b);
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        return bv - av;
      });
    }
    return [...stocks].sort((a, b) => {
      const av = a.dr_1, bv = b.dr_1;
      if (av == null) return 1;
      if (bv == null) return -1;
      return bv - av;
    });
  }

  window.MobileScreener.init({
    pageTitle: 'High Growth',
    fetchStocks: cap => fetch('/api/frontend/high-sales-growth/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: applySort,
    listValueLabel: '1D',
    extraFilterHtml:
      '<div class="strip sort-strip" role="tablist" aria-label="Sort">' +
      '<span class="strip-label">Sort</span>' +
      '<button type="button" class="pill sort-pill active" data-sort="return">Top Returns</button>' +
      '<button type="button" class="pill sort-pill" data-sort="expensive">High P/S</button>' +
      '</div>',
    onSetup: app => {
      document.querySelectorAll('.sort-pill').forEach(btn => {
        btn.addEventListener('click', () => {
          sortMode = btn.dataset.sort;
          document.querySelectorAll('.sort-pill').forEach(b => b.classList.toggle('active', b === btn));
          app.allStocks = applySort(app.allStocks);
          const header = document.getElementById('listValueHeader');
          if (header) header.textContent = sortMode === 'expensive' ? 'P/S' : '1D';
          app.renderList();
        });
      });
    },
    listValueFn: stock => {
      if (sortMode === 'expensive') {
        const avg = psAvg(stock);
        return avg == null ? '—' : avg.toFixed(1);
      }
      return U.fmtRet(stock.dr_1);
    },
  });
})();
