(function () {
  'use strict';

  window.MobileScreener.init({
    pageTitle: 'Abi Watchlist',
    usesCapFilter: false,
    showTi65: true,
    showRank: false,
    notesFromStock: true,
    watchlistFromStock: true,
    fetchStocks: () => fetch('/api/frontend/abi-watchlist/data')
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: (stocks) => [...stocks].sort((a, b) => {
      const av = a.dr_1, bv = b.dr_1;
      if (av == null) return 1;
      if (bv == null) return -1;
      return bv - av;
    }),
  });
})();
