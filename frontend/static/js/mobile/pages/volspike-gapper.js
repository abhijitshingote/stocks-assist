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
    return VsgEvent.magStr(s, {compact: true}) || '—';
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
    pageLabel: 'VSG',
    fetchStocks: cap => fetch('/api/frontend/volspike-gapper/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    filterStocks: filterRecency,
    sortStocks: sortEvents,
    listValueLabel: 'Event',
    listValueFn: eventMag,
    listValueClsFn: s => s.last_event_return != null ? U.retCls(s.last_event_return) : '',
    listBadgeFn: s => {
      const b = VsgEvent.badge(s);
      return b ? '<span class="dr-evt ' + VsgEvent.badgeClass(s) + '">' + b + '</span>' : '';
    },
    listMetaFn: s => (s.last_event_date || '').slice(5),
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
  });
})();
