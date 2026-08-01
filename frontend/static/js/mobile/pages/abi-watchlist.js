(function () {
  'use strict';

  function sortByStars(stocks) {
    return [...stocks].sort((a, b) => {
      const as = Number.isFinite(a.watchlist_stars) ? a.watchlist_stars : 0;
      const bs = Number.isFinite(b.watchlist_stars) ? b.watchlist_stars : 0;
      if (bs !== as) return bs - as;
      const av = a.dr_1, bv = b.dr_1;
      if (av == null) return 1;
      if (bv == null) return -1;
      return bv - av;
    });
  }

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
    sortStocks: sortByStars,
  });
})();
