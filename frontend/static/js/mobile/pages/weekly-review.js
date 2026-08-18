(function () {
  'use strict';

  const U = window.MobileUtil;
  const SRC_LABEL = { vsg90: 'VSG', strong: 'STR', top520: '5/20', fastrs: 'RS' };
  const SRC_ORDER = ['vsg90', 'strong', 'top520', 'fastrs'];
  const SORT_KEY = 'weeklyReviewSort';

  let sortMode = 'ati65';
  try {
    const saved = localStorage.getItem(SORT_KEY);
    if (saved === 'best' || saved === 'ati65' || saved === 'sources' || saved === 'dr1' || saved === 'dr5') {
      sortMode = saved;
    }
  } catch (e) {}

  let cached = null;

  function fmtCutoff(id, spec) {
    if (!spec) return id;
    if (!spec.enabled) return id + '=off';
    return id + ':' + spec.field + '≥' + spec.min;
  }

  function renderMeta(json) {
    const el = document.getElementById('wrMeta');
    if (!el || !json) return;
    const f = json.funnel || {};
    const cuts = json.cutoffs
      ? Object.keys(json.cutoffs).map(k => fmtCutoff(k, json.cutoffs[k])).join(' ')
      : '';
    el.innerHTML =
      'cycle Sat ' + (json.cycle || '—') + ' → Fri ' + (json.cycle_ends || '—') +
      (cuts ? '<br>' + U.escAttr(cuts) : '') +
      '<br>union ' + (f.union || 0) +
      ' −watch ' + (f.hidden_watch || 0) +
      ' −trade ' + (f.hidden_trade || 0) +
      ' −pass ' + (f.hidden_pass || 0) +
      ' = ' + (f.queue || 0);
  }

  function srcLabel(s) {
    const have = new Set(s.sources || []);
    return SRC_ORDER.filter(id => have.has(id)).map(id => SRC_LABEL[id]).join(' ');
  }

  function listValue(s) {
    if (sortMode === 'dr1') return U.fmtRet(s.dr_1);
    if (sortMode === 'dr5') return U.fmtRet(s.dr_5);
    if (sortMode === 'sources') return String((s.sources || []).length);
    if (sortMode === 'best') return s.best_rank != null ? String(s.best_rank) : '—';
    return s.adjusted_ti65 != null ? s.adjusted_ti65.toFixed(2) : '—';
  }

  function sortStocks(stocks) {
    return [...stocks].sort((a, b) => {
      if (sortMode === 'sources') {
        const d = (b.sources || []).length - (a.sources || []).length;
        if (d) return d;
      } else if (sortMode === 'ati65') {
        const av = a.adjusted_ti65, bv = b.adjusted_ti65;
        if (av == null && bv == null) { /* tie */ }
        else if (av == null) return 1;
        else if (bv == null) return -1;
        else if (bv !== av) return bv - av;
      } else if (sortMode === 'dr1') {
        const av = a.dr_1, bv = b.dr_1;
        if (av == null) return 1;
        if (bv == null) return -1;
        if (bv !== av) return bv - av;
      } else if (sortMode === 'dr5') {
        const av = a.dr_5, bv = b.dr_5;
        if (av == null) return 1;
        if (bv == null) return -1;
        if (bv !== av) return bv - av;
      } else {
        const ar = a.best_rank, br = b.best_rank;
        if (ar == null) return 1;
        if (br == null) return -1;
        if (ar !== br) return ar - br;
      }
      const ns = (b.sources || []).length - (a.sources || []).length;
      if (ns) return ns;
      return (a.ticker || '').localeCompare(b.ticker || '');
    });
  }

  window.MobileScreener.init({
    pageTitle: 'Weekly Review',
    pageLabel: 'Weekly',
    weeklyDisposition: true,
    fetchStocks: cap => {
      const req = cached
        ? Promise.resolve(cached)
        : fetch('/api/frontend/weekly-review')
          .then(r => r.json())
          .then(data => {
            cached = data && !data.error ? data : { stocks: [] };
            renderMeta(cached);
            return cached;
          });
      return req.then(data => {
        const stocks = Array.isArray(data.stocks) ? data.stocks : [];
        if (!cap || cap === 'all') return stocks;
        return stocks.filter(s => s.cap_bucket === cap);
      }).catch(() => []);
    },
    sortStocks,
    listValueLabel: 'Val',
    listValueFn: listValue,
    listValueClsFn: s => {
      if (sortMode === 'dr1') return U.retCls(s.dr_1);
      if (sortMode === 'dr5') return U.retCls(s.dr_5);
      return '';
    },
    listMetaFn: srcLabel,
    extraFilterHtml:
      '<div class="wr-meta" id="wrMeta"></div>' +
      '<div class="strip recency-strip" role="tablist" aria-label="Sort">' +
      '<span class="strip-label">Sort</span>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'ati65' ? ' active' : '') + '" data-sort="ati65">ATI65</button>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'best' ? ' active' : '') + '" data-sort="best">Rank</button>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'sources' ? ' active' : '') + '" data-sort="sources">#src</button>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'dr1' ? ' active' : '') + '" data-sort="dr1">1D</button>' +
      '<button type="button" class="pill recency-pill' + (sortMode === 'dr5' ? ' active' : '') + '" data-sort="dr5">5D</button>' +
      '</div>',
    onSetup: app => {
      document.querySelectorAll('[data-sort]').forEach(btn => {
        btn.addEventListener('click', () => {
          sortMode = btn.dataset.sort;
          try { localStorage.setItem(SORT_KEY, sortMode); } catch (e) {}
          document.querySelectorAll('[data-sort]').forEach(b => b.classList.toggle('active', b === btn));
          app.loadData(app.currentCap);
        });
      });
    },
  });
})();
