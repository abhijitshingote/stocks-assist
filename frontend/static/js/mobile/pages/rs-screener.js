(function () {
  'use strict';
  window.MobileScreener.init({
    pageTitle: 'Slow / Fast RS',
    fetchStocks: cap => fetch('/api/frontend/rs-screener/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: stocks => [...stocks].sort((a, b) => {
      const ra = a.rs_rating != null ? a.rs_rating : -1;
      const rb = b.rs_rating != null ? b.rs_rating : -1;
      if (rb !== ra) return rb - ra;
      const va = a.rs_vs_spy != null ? a.rs_vs_spy : -1;
      const vb = b.rs_vs_spy != null ? b.rs_vs_spy : -1;
      return vb - va;
    }),
    listValueFn: s => s.rs_rating != null ? s.rs_rating.toFixed(0) : '—',
    listValueLabel: 'RS',
  });
})();
