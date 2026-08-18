(function () {
  'use strict';

  let groups = [];
  let perfMap = {};
  let currentSort = 'default';

  document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('#etfSort .home-sort-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        currentSort = btn.dataset.sort;
        document.querySelectorAll('#etfSort .home-sort-btn').forEach(b => b.classList.toggle('active', b === btn));
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

  async function loadData() {
    const grid = document.getElementById('etfGrid');
    try {
      const resp = await fetch('/api/frontend/etfs');
      const data = await resp.json();
      if (Array.isArray(data)) groups = data;
    } catch (e) {
      grid.innerHTML = '<div class="md-empty">Failed to load etfs.json</div>';
      return;
    }
    if (!groups.length) {
      grid.innerHTML = '<div class="md-empty">No ETFs configured. Edit user_data/etfs.json.</div>';
      return;
    }
    const symbols = [];
    groups.forEach(g => (g.etfs || []).forEach(e => symbols.push(e.symbol)));
    document.getElementById('etfMeta').textContent = groups.length + ' groups · ' + symbols.length + ' ETFs';
    try {
      const resp = await fetch('/api/frontend/etf-performance?symbols=' + encodeURIComponent(symbols.join(',')));
      const data = await resp.json();
      (data.etfs || []).forEach(p => { perfMap[p.symbol] = p; });
    } catch (e) {
      console.error(e);
    }
    render();
  }

  function sortEtfs(etfs) {
    if (currentSort === 'default') return etfs.slice();
    return etfs.slice().sort((a, b) => {
      const va = perfMap[a.symbol]?.[currentSort];
      const vb = perfMap[b.symbol]?.[currentSort];
      if (va == null) return 1;
      if (vb == null) return -1;
      return vb - va;
    });
  }

  function render() {
    const grid = document.getElementById('etfGrid');
    grid.innerHTML = groups.map(group => {
      const rows = sortEtfs(group.etfs || []).map(etf => {
        const p = perfMap[etf.symbol] || {};
        return '<a class="cg-row" href="/m/stock/' + encodeURIComponent(etf.symbol) + '">' +
          '<span class="cg-tk">' + esc(etf.symbol) +
            '<span class="cg-name">' + esc(etf.name || '') + '</span></span>' +
          '<span class="cg-rets">' +
            '<span class="' + retCls(p.dr_1) + '">' + fmtRet(p.dr_1) + '</span>' +
            '<span class="' + retCls(p.dr_5) + '">' + fmtRet(p.dr_5) + '</span>' +
            '<span class="' + retCls(p.dr_20) + '">' + fmtRet(p.dr_20) + '</span>' +
          '</span></a>';
      }).join('');
      return '<div class="cg-card">' +
        '<div class="cg-head">' + esc(group.category) +
          '<span class="cg-count">' + (group.etfs || []).length + '</span></div>' +
        '<div class="cg-cols"><span></span><span>1D</span><span>5D</span><span>20D</span></div>' +
        rows + '</div>';
    }).join('');
  }
})();
