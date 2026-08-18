(function () {
  'use strict';
  window.MobileScreener.init({
    pageTitle: 'All Stocks',
    pageLabel: 'All',
    usesCapFilter: false,
    fetchStocks: () => fetch('/api/frontend/all-stocks')
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: stocks => [...stocks].sort((a, b) => {
      const av = a.ti65 != null ? a.ti65 : -1;
      const bv = b.ti65 != null ? b.ti65 : -1;
      return bv - av;
    }),
    listValueFn: s => s.ti65 != null ? s.ti65.toFixed(0) : '—',
    listValueLabel: 'TI65',
  });
})();
