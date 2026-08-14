(function () {
  'use strict';

  const U = window.MobileUtil;
  const SORT_KEY = 'dailyReviewSort';
  let sortMode = 'adj';
  try {
    const saved = localStorage.getItem(SORT_KEY);
    if (saved === 'flat' || saved === 'adj') sortMode = saved;
  } catch (e) {}

  function eventToday(s) {
    return !!(s.event_today || (s.as_of && s.last_event_date === s.as_of));
  }

  function eventMag(s) {
    if (!eventToday(s) || s.last_event_magnitude == null) return null;
    if (s.last_event_type === 'volume_spike') return s.last_event_magnitude.toFixed(1) + 'x';
    if (s.last_event_type === 'gapper') {
      return (s.last_event_magnitude >= 0 ? '+' : '') + (s.last_event_magnitude * 100).toFixed(0) + '%';
    }
    return null;
  }

  function listValue(s) {
    const mag = eventMag(s);
    if (mag) return mag;
    return U.fmtRet(s.dr_1);
  }

  function sortKey(s) {
    const v = sortMode === 'adj' ? s.adjusted_dr_1 : s.dr_1;
    return v == null ? -Infinity : v;
  }

  function sortStocks(stocks) {
    return [...stocks].sort((a, b) => sortKey(b) - sortKey(a));
  }

  window.MobileScreener.init({
    pageTitle: 'Daily Review',
    fetchStocks: cap => fetch('/api/frontend/daily-review/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: sortStocks,
    listValueLabel: '1D',
    listValueFn: listValue,
    extraFilterHtml:
      '<div class="strip recency-strip" role="tablist" aria-label="Sort">' +
      '<span class="strip-label">Sort</span>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'adj' ? ' active' : '') + '" data-sort="adj">Adj 1D</button>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'flat' ? ' active' : '') + '" data-sort="flat">Flat 1D</button>' +
      '</div>',
    onSetup: app => {
      document.querySelectorAll('.recency-pill').forEach(btn => {
        btn.addEventListener('click', () => {
          sortMode = btn.dataset.sort;
          try { localStorage.setItem(SORT_KEY, sortMode); } catch (e) {}
          document.querySelectorAll('.recency-pill').forEach(b => b.classList.toggle('active', b === btn));
          app.loadData(app.currentCap);
        });
      });
    },
    renderList: (visible, app) => {
      const chipStrip = document.getElementById('chipStrip');
      const tbody = document.getElementById('stockTableBody');

      chipStrip.innerHTML = visible.slice(0, 24).map(s => {
        const active = s.ticker === app.selectedTicker ? ' active' : '';
        const badge = eventToday(s)
          ? (s.last_event_type === 'volume_spike' ? 'S' : s.last_event_type === 'gapper' ? 'G' : '')
          : '';
        return '<div class="tchip' + active + '" data-ticker="' + U.escAttr(s.ticker) + '">' +
          '<span class="tk">' + U.escAttr(s.ticker) + (badge ? '<span class="evt-badge">' + badge + '</span>' : '') +
          '</span><span class="ret">' + U.fmtRet(s.dr_1) + '</span></div>';
      }).join('');

      tbody.innerHTML = visible.map((s, i) => {
        const active = s.ticker === app.selectedTicker ? ' active' : '';
        const badge = eventToday(s)
          ? (s.last_event_type === 'volume_spike' ? 'S' : s.last_event_type === 'gapper' ? 'G' : '')
          : '';
        const mag = eventMag(s);
        return '<tr class="' + active.trim() + '" data-ticker="' + U.escAttr(s.ticker) + '">' +
          '<td>' + (i + 1) + '</td>' +
          '<td>' + U.escAttr(s.ticker) + (badge ? ' <span class="evt-badge">' + badge + '</span>' : '') + '</td>' +
          '<td>' + U.fmtMktCap(s.market_cap) + '</td>' +
          '<td>' + U.fmtRet(s.dr_1) + (mag ? ' ' + mag : '') + '</td>' +
          '<td class="stars muted" style="font-size:0.5rem">' + U.escAttr((s.as_of || '').slice(5)) + '</td></tr>';
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
