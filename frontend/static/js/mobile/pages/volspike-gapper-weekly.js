(function () {
  'use strict';

  const U = window.MobileUtil;

  function eventMag(s) {
    return VsgEvent.magStr(s, {compact: true}) || '—';
  }

  window.MobileScreener.init({
    pageTitle: 'Vol Spike & Gaps (W)',
    pageLabel: 'VSG W',
    fetchStocks: cap => fetch('/api/frontend/volspike-gapper/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: stocks => [...stocks].sort((a, b) => {
      const ad = a.last_event_date || '', bd = b.last_event_date || '';
      if (bd < ad) return -1;
      if (bd > ad) return 1;
      return 0;
    }),
    listValueFn: s => eventMag(s),
    listValueLabel: 'Event',
    listValueClsFn: s => s.last_event_return != null ? U.retCls(s.last_event_return) : '',
    listBadgeFn: s => {
      const b = VsgEvent.badge(s);
      return b ? '<span class="dr-evt ' + VsgEvent.badgeClass(s) + '">' + b + '</span>' : '';
    },
    listMetaFn: s => (s.last_event_date || '').slice(5),
  });
})();
