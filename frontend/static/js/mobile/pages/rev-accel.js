(function () {
  'use strict';

  const U = window.MobileUtil;
  const RA = window.RevAccel;
  const SORT_KEY = 'revAccelSort';
  const FILTER_KEY = 'revAccelFilters';

  let sortMode = 'score';
  const active = new Set();
  try {
    const s = localStorage.getItem(SORT_KEY);
    if (RA.SORTS.some(x => x.id === s)) sortMode = s;
    const f = JSON.parse(localStorage.getItem(FILTER_KEY));
    if (Array.isArray(f)) f.filter(id => RA.FILTERS.some(x => x.id === id)).forEach(id => active.add(id));
  } catch (e) {}

  window.MobileScreener.init({
    pageTitle: 'Rev Accel',
    pageLabel: 'Rev Accel',
    weeklyDisposition: 'revaccel',
    fetchStocks: cap => fetch('/api/frontend/rev-accel/' + cap)
      .then(r => r.json())
      .then(data => (data && data.error ? [] : data)),
    sortStocks: stocks => RA.sortBy([...stocks], sortMode),
    filterStocks: base => base.filter(s => RA.passes(s, active)),
    listValueLabel: '1D',
    listBadgeFn: s => '<span class="ra-badge">' + RA.listChip(s) + '</span>',
    prependMetricsFn: s => RA.metricsHtml(s, U.msItem),
    extraFilterHtml:
      '<div class="strip recency-strip" role="group" aria-label="Filters">' +
      '<span class="strip-label">Filter</span>' +
      RA.FILTERS.map(f =>
        '<button type="button" class="pill recency-pill' + (active.has(f.id) ? ' active' : '') +
        '" data-ra-filter="' + f.id + '">' + f.label + '</button>').join('') +
      '</div>' +
      '<div class="strip recency-strip" role="tablist" aria-label="Sort">' +
      '<span class="strip-label">Sort</span>' +
      RA.SORTS.map(x =>
        '<button type="button" class="pill recency-pill' + (x.id === sortMode ? ' active' : '') +
        '" data-ra-sort="' + x.id + '">' + x.label + '</button>').join('') +
      '</div>',
    onSetup: app => {
      document.querySelectorAll('[data-ra-sort]').forEach(btn => {
        btn.addEventListener('click', () => {
          sortMode = btn.dataset.raSort;
          try { localStorage.setItem(SORT_KEY, sortMode); } catch (e) {}
          document.querySelectorAll('[data-ra-sort]').forEach(b => b.classList.toggle('active', b === btn));
          RA.sortBy(app.allStocks, sortMode);
          app.renderList();
        });
      });
      document.querySelectorAll('[data-ra-filter]').forEach(btn => {
        btn.addEventListener('click', () => {
          const id = btn.dataset.raFilter;
          if (active.has(id)) active.delete(id); else active.add(id);
          try { localStorage.setItem(FILTER_KEY, JSON.stringify([...active])); } catch (e) {}
          btn.classList.toggle('active', active.has(id));
          app.renderList();
        });
      });
    },
  });
})();
