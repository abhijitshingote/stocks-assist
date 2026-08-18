(function () {
  'use strict';

  const U = window.MobileUtil;

  function sideHtml(s) {
    if (!s.trade_side) return '';
    return '<span class="trade-side ' + U.escAttr(s.trade_side) + '">' + U.escAttr(s.trade_side) + '</span>';
  }

  window.MobileScreener.init({
    pageTitle: 'Trades',
    usesCapFilter: false,
    showTi65: true,
    showRank: false,
    notesFromStock: true,
    watchlistFromStock: true,
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
    listValueLabel: '1D',
    onSetup: app => {
      const row = document.querySelector('.detail-row-c');
      if (!row || document.getElementById('tradeRemoveBtn')) return;
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'icon-btn';
      btn.id = 'tradeRemoveBtn';
      btn.textContent = 'Remove';
      btn.addEventListener('click', async () => {
        const t = app.selectedTicker;
        if (!t) return;
        await fetch('/api/frontend/abi-trades/' + encodeURIComponent(t), { method: 'DELETE' });
        app.loadData();
      });
      row.appendChild(btn);
    },
    renderList: (visible, app) => {
      const chipStrip = document.getElementById('chipStrip');
      const tbody = document.getElementById('stockTableBody');

      chipStrip.innerHTML = visible.slice(0, 24).map(s => {
        const active = s.ticker === app.selectedTicker ? ' active' : '';
        return '<div class="tchip' + active + '" data-ticker="' + U.escAttr(s.ticker) + '">' +
          '<span class="tk">' + U.escAttr(s.ticker) +
          '</span><span class="ret">' + U.fmtRet(s.dr_1) + '</span></div>';
      }).join('');

      tbody.innerHTML = visible.map(s => {
        const active = s.ticker === app.selectedTicker ? ' active' : '';
        return '<tr class="' + active.trim() + '" data-ticker="' + U.escAttr(s.ticker) + '">' +
          '<td>' + sideHtml(s) + U.escAttr(s.ticker) + '</td>' +
          '<td>' + U.fmtMktCap(s.market_cap) + '</td>' +
          '<td class="' + U.retCls(s.dr_1) + '">' + U.fmtRet(s.dr_1) + '</td>' +
          '<td class="stars"></td></tr>';
      }).join('');

      chipStrip.querySelectorAll('.tchip').forEach(c => {
        c.addEventListener('click', () => app.selectStock(c.dataset.ticker));
      });
      tbody.querySelectorAll('tr').forEach(r => {
        r.addEventListener('click', () => app.selectStock(r.dataset.ticker));
      });
    },
  });
})();
