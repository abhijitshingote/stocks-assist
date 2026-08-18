(function () {
  'use strict';

  const U = window.MobileUtil;

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
      const av = a.dr_1, bv = b.dr_1;
      if (av == null) return 1;
      if (bv == null) return -1;
      return bv - av;
    }),
    listValueFn: s => U.fmtRet(s.dr_1),
    listValueClsFn: s => U.retCls(s.dr_1),
    listValueLabel: '1D',
    listBadgeFn: sideHtml,
  });
})();
