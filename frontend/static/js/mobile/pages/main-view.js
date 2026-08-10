(function () {
  'use strict';
  window.MobileScreener.init({
    pageTitle: 'Main View',
    fetchStocks: cap => fetch('/api/frontend/main-view/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: stocks => [...stocks].sort((a, b) => {
      const av = a.ti65 != null ? a.ti65 : -1;
      const bv = b.ti65 != null ? b.ti65 : -1;
      return bv - av;
    }),
    listValueFn: s => s.ti65 != null ? s.ti65.toFixed(1) : '—',
    listValueLabel: 'TI65',
  });
})();
