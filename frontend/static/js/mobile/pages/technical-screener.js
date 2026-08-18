(function () {
  'use strict';
  window.MobileScreener.init({
    pageTitle: 'Technical',
    pageLabel: 'Tech',
    fetchStocks: cap => fetch('/api/frontend/technical-screener/reversal/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: stocks => [...stocks].sort((a, b) => {
      const av = a.reversal_pct != null ? a.reversal_pct : -1;
      const bv = b.reversal_pct != null ? b.reversal_pct : -1;
      return bv - av;
    }),
    listValueFn: s => s.reversal_pct != null
      ? (s.reversal_pct >= 0 ? '+' : '') + s.reversal_pct.toFixed(1) + '%'
      : '—',
    listValueLabel: 'Rev',
  });
})();
