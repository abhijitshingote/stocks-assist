(function () {
  'use strict';

  function sortByTi65(data) {
    return [...data].sort((a, b) => {
      const aVal = a.ti65;
      const bVal = b.ti65;
      if (aVal == null) return 1;
      if (bVal == null) return -1;
      return bVal - aVal;
    });
  }

  window.MobileScreener.init({
    pageTitle: 'Main View',
    fetchStocks: cap => fetch('/api/frontend/main-view/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: sortByTi65,
    showTi65: true,
    newsIds: {
      content: 'hybridNewsContent',
      loadBtn: 'hybridLoadNewsBtn',
      benzingaBtn: 'hybridBenzingaNewsBtn',
      filterBar: 'hybridNewsFilters',
    },
  });
})();
