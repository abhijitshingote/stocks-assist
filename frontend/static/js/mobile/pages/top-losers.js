(function () {
  'use strict';
  window.MobileScreener.init({
    pageTitle: 'Top Losers',
    pageLabel: 'Losers',
    fetchStocks: cap => fetch('/api/frontend/top-losers/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: stocks => [...stocks].sort((a, b) => {
      const av = a.dr_1, bv = b.dr_1;
      if (av == null) return 1;
      if (bv == null) return -1;
      return av - bv;
    }),
  });
})();
