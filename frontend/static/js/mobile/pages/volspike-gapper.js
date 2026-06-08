(function () {
  'use strict';

  const U = window.MobileUtil;
  let recency = 'all';

  function recencyCutoff(filter) {
    if (filter === 'all') return null;
    const days = parseInt(filter, 10);
    const d = new Date();
    d.setDate(d.getDate() - days);
    return d.toISOString().split('T')[0];
  }

  function eventMag(s) {
    if (s.last_event_magnitude == null) return '—';
    if (s.last_event_type === 'volume_spike') return s.last_event_magnitude.toFixed(1) + 'x';
    if (s.last_event_type === 'gapper') {
      return (s.last_event_magnitude >= 0 ? '+' : '') + (s.last_event_magnitude * 100).toFixed(0) + '%';
    }
    return '—';
  }

  function filterRecency(stocks) {
    const cutoff = recencyCutoff(recency);
    if (!cutoff) return stocks;
    return stocks.filter(s => s.last_event_date && s.last_event_date >= cutoff);
  }

  function sortEvents(stocks) {
    return [...stocks].sort((a, b) => {
      const ad = a.last_event_date || '', bd = b.last_event_date || '';
      if (ad !== bd) return bd.localeCompare(ad);
      const ar = a.last_event_return, br = b.last_event_return;
      if (ar == null) return 1;
      if (br == null) return -1;
      return br - ar;
    });
  }

  window.MobileScreener.init({
    pageTitle: 'Vol Spike & Gaps',
    fetchStocks: cap => fetch('/api/frontend/volspike-gapper/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    filterStocks: filterRecency,
    sortStocks: sortEvents,
    listValueLabel: 'Event',
    listValueFn: eventMag,
    extraFilterHtml:
      '<div class="strip recency-strip" role="tablist" aria-label="Recency">' +
      '<span class="strip-label">When</span>' +
      '<button type="button" class="pill recency-pill active" data-recency="all">All</button>' +
      '<button type="button" class="pill recency-pill" data-recency="1">1D</button>' +
      '<button type="button" class="pill recency-pill" data-recency="5">5D</button>' +
      '<button type="button" class="pill recency-pill" data-recency="20">20D</button>' +
      '</div>',
    onSetup: app => {
      document.querySelectorAll('.recency-pill').forEach(btn => {
        btn.addEventListener('click', () => {
          recency = btn.dataset.recency;
          document.querySelectorAll('.recency-pill').forEach(b => b.classList.toggle('active', b === btn));
          app.renderList();
          app.updateCounts();
        });
      });
    },
    renderList: (visible, app) => {
      const chipStrip = document.getElementById('chipStrip');
      const tbody = document.getElementById('stockTableBody');

      chipStrip.innerHTML = visible.slice(0, 24).map(s => {
        const active = s.ticker === app.selectedTicker ? ' active' : '';
        const badge = s.last_event_type === 'volume_spike' ? 'S' : s.last_event_type === 'gapper' ? 'G' : '';
        return '<div class="tchip' + active + '" data-ticker="' + U.escAttr(s.ticker) + '">' +
          '<span class="tk">' + U.escAttr(s.ticker) + (badge ? '<span class="evt-badge">' + badge + '</span>' : '') +
          '</span><span class="ret">' + eventMag(s) + '</span></div>';
      }).join('');

      tbody.innerHTML = visible.map((s, i) => {
        const active = s.ticker === app.selectedTicker ? ' active' : '';
        const badge = s.last_event_type === 'volume_spike' ? 'S' : s.last_event_type === 'gapper' ? 'G' : '';
        const date = s.last_event_date || '';
        return '<tr class="' + active.trim() + '" data-ticker="' + U.escAttr(s.ticker) + '">' +
          '<td>' + (i + 1) + '</td>' +
          '<td>' + U.escAttr(s.ticker) + (badge ? ' <span class="evt-badge">' + badge + '</span>' : '') + '</td>' +
          '<td>' + U.fmtMktCap(s.market_cap) + '</td>' +
          '<td>' + eventMag(s) + '</td>' +
          '<td class="stars muted" style="font-size:0.5rem">' + U.escAttr(date.slice(5)) + '</td></tr>';
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
