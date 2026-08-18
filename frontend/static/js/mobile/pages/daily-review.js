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

  function evtBadge(s) {
    if (!eventToday(s)) return '';
    const b = VsgEvent.badge(s);
    if (!b) return '';
    return '<span class="dr-evt ' + VsgEvent.badgeClass(s) + '">' + b + '</span>';
  }

  function fmtDr1(v) {
    if (v == null) return '—';
    return (v >= 0 ? '+' : '') + v.toFixed(1) + '%';
  }

  function formatDisplayDate(dateStr) {
    if (!dateStr) return '';
    try {
      const [y, m, d] = dateStr.split('-').map(Number);
      const date = new Date(y, m - 1, d);
      const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
      const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      return days[date.getDay()] + ' ' + months[date.getMonth()] + ' ' + d;
    } catch (e) {
      return dateStr;
    }
  }

  function sortKey(s) {
    const v = sortMode === 'adj' ? s.adjusted_dr_1 : s.dr_1;
    return v == null ? -Infinity : v;
  }

  window.MobileScreener.init({
    pageTitle: 'Daily Review',
    pageLabel: 'Daily',
    weeklyDisposition: 'daily',
    fetchStocks: cap => fetch('/api/frontend/daily-review/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: stocks => [...stocks].sort((a, b) => sortKey(b) - sortKey(a)),
    listValueLabel: '1D',
    listValueFn: s => fmtDr1(s.dr_1),
    listValueClsFn: s => U.retCls(s.dr_1),
    listBadgeFn: evtBadge,
    listRowClassFn: s => eventToday(s) ? 'event' : '',
    subtitleFn: visible => {
      const asOf = visible[0] && visible[0].as_of;
      return asOf ? formatDisplayDate(asOf) : '';
    },
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
  });
})();
