(function () {
  'use strict';

  let themes = [];
  let stockMap = {};
  let currentSort = 'default';

  document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('#thSort .home-sort-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        currentSort = btn.dataset.sort;
        document.querySelectorAll('#thSort .home-sort-btn').forEach(b => b.classList.toggle('active', b === btn));
        render();
      });
    });
    loadData();
  });

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
  }

  function fmtRet(v) {
    if (v == null) return '—';
    return (v >= 0 ? '+' : '') + v.toFixed(1) + '%';
  }

  function retCls(v) {
    if (v == null) return 'ret-neutral';
    return v >= 0 ? 'ret-pos' : 'ret-neg';
  }

  function fmtMktCap(v) {
    if (!v) return '';
    if (v >= 1e12) return (v / 1e12).toFixed(1) + 'T';
    if (v >= 1e9) return (v / 1e9).toFixed(0) + 'B';
    if (v >= 1e6) return (v / 1e6).toFixed(0) + 'M';
    return String(v);
  }

  async function loadData() {
    const grid = document.getElementById('thGrid');
    try {
      const resp = await fetch('/api/frontend/themes');
      const data = await resp.json();
      if (Array.isArray(data)) themes = data;
    } catch (e) {
      grid.innerHTML = '<div class="md-empty">Failed to load themes.json</div>';
      return;
    }
    if (!themes.length) {
      grid.innerHTML = '<div class="md-empty">No themes configured. Edit user_data/themes.json.</div>';
      return;
    }
    const tickers = Array.from(new Set(themes.flatMap(t => t.tickers || [])));
    document.getElementById('thMeta').textContent = themes.length + ' themes · ' + tickers.length + ' tickers';
    try {
      const resp = await fetch('/api/frontend/all-stocks/by-tickers?tickers=' + encodeURIComponent(tickers.join(',')));
      const rows = await resp.json();
      if (Array.isArray(rows)) rows.forEach(s => { stockMap[s.ticker] = s; });
    } catch (e) {
      console.error(e);
    }
    render();
  }

  function sortTickers(tickers) {
    if (currentSort === 'default') return tickers.slice();
    return tickers.slice().sort((a, b) => {
      const va = stockMap[a] ? stockMap[a][currentSort] : null;
      const vb = stockMap[b] ? stockMap[b][currentSort] : null;
      if (va == null) return 1;
      if (vb == null) return -1;
      return vb - va;
    });
  }

  function render() {
    const grid = document.getElementById('thGrid');
    grid.innerHTML = themes.map(theme => {
      const rows = sortTickers(theme.tickers || []).map(ticker => {
        const s = stockMap[ticker] || {};
        return '<a class="cg-row" href="/m/stock/' + encodeURIComponent(ticker) + '">' +
          '<span class="cg-tk">' + esc(ticker) +
            '<span class="cg-name">' + esc(s.company_name || fmtMktCap(s.market_cap)) + '</span></span>' +
          '<span class="cg-rets">' +
            '<span class="' + retCls(s.dr_1) + '">' + fmtRet(s.dr_1) + '</span>' +
            '<span class="' + retCls(s.dr_5) + '">' + fmtRet(s.dr_5) + '</span>' +
            '<span class="' + retCls(s.dr_20) + '">' + fmtRet(s.dr_20) + '</span>' +
          '</span></a>';
      }).join('');
      return '<div class="cg-card">' +
        '<div class="cg-head">' + esc(theme.name) +
          '<span class="cg-count">' + (theme.tickers || []).length + '</span></div>' +
        (theme.desc ? '<div class="cg-desc">' + esc(theme.desc) + '</div>' : '') +
        '<div class="cg-cols"><span></span><span>1D</span><span>5D</span><span>20D</span></div>' +
        rows + '</div>';
    }).join('');
  }
})();
