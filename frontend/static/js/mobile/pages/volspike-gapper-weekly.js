(function () {
  'use strict';

  function eventMag(s) {
    return VsgEvent.magStr(s, {compact: true}) || '—';
  }

  window.MobileScreener.init({
    pageTitle: 'Vol Spike & Gaps (W)',
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
  });
})();
