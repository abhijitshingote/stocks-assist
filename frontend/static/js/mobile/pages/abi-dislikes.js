(function () {
  'use strict';

  let dislikes = {};

  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('dlAddBtn').addEventListener('click', addDislike);
    document.getElementById('dlAddTicker').addEventListener('keydown', e => {
      if (e.key === 'Enter') addDislike();
    });
    loadDislikes();
  });

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function fmtDate(iso) {
    if (!iso) return '';
    try {
      return new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    } catch (_) { return iso; }
  }

  async function loadDislikes() {
    try {
      const resp = await fetch('/api/frontend/abi-dislikes');
      dislikes = await resp.json();
      render();
    } catch (e) {
      document.getElementById('dlContent').innerHTML = '<div class="md-empty">Failed to load excludes.</div>';
    }
  }

  function badge(entry) {
    const kind = entry.kind === 'temporary' ? 'temporary' : 'permanent';
    const active = entry.is_active !== false;
    if (!active) return '<span class="dl-badge expired">expired</span>';
    if (kind === 'temporary') {
      const left = entry.days_left != null ? entry.days_left + 'd left' : '30d';
      return '<span class="dl-badge temp">' + left + '</span>';
    }
    return '<span class="dl-badge perm">perm</span>';
  }

  function rowHtml(t, entry) {
    const notes = (entry.notes || '').trim();
    const exp = entry.kind === 'temporary' && entry.expires_at
      ? ' · exp ' + fmtDate(entry.expires_at) : '';
    return '<div class="dl-row">' +
      '<div class="dl-row-head">' +
        '<a class="dl-ticker" href="/m/stock/' + encodeURIComponent(t) + '">' + esc(t) + '</a>' +
        badge(entry) +
        '<button type="button" class="md-btn danger dl-rm" data-ticker="' + esc(t) + '">Remove</button>' +
      '</div>' +
      '<div class="dl-meta">Added ' + fmtDate(entry.added_at) + exp + '</div>' +
      '<div class="dl-notes' + (notes ? '' : ' empty') + '">' + (notes ? esc(notes) : 'No ticker notes') + '</div>' +
    '</div>';
  }

  function sectionHtml(title, tickers) {
    if (!tickers.length) return '';
    let html = '<div class="dl-section"><div class="ctx2-section-title">' + title +
      ' <span class="dl-n">' + tickers.length + '</span></div>';
    for (const t of tickers) html += rowHtml(t, dislikes[t] || {});
    return html + '</div>';
  }

  function render() {
    const tickers = Object.keys(dislikes).sort();
    const active = tickers.filter(t => dislikes[t].is_active !== false);
    document.getElementById('dlCount').textContent = active.length + ' active';

    const root = document.getElementById('dlContent');
    if (!tickers.length) {
      root.innerHTML = '<div class="md-empty">No excludes. Add a ticker to hide it from every screener.</div>';
      return;
    }
    const temps = tickers.filter(t => dislikes[t].kind === 'temporary' && dislikes[t].is_active !== false);
    const perms = tickers.filter(t => dislikes[t].kind !== 'temporary' && dislikes[t].is_active !== false);
    const expired = tickers.filter(t => dislikes[t].is_active === false);
    root.innerHTML = sectionHtml('Temporary (30d)', temps)
      + sectionHtml('Permanent', perms)
      + sectionHtml('Expired', expired);
    root.querySelectorAll('.dl-rm').forEach(btn => {
      btn.addEventListener('click', () => removeDislike(btn.dataset.ticker));
    });
  }

  async function addDislike() {
    const input = document.getElementById('dlAddTicker');
    const ticker = input.value.trim().toUpperCase();
    if (!ticker) return;
    const kind = document.getElementById('dlKind').value;
    try {
      await fetch('/api/frontend/abi-dislikes', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ticker, kind }),
      });
      input.value = '';
      await loadDislikes();
    } catch (e) {
      console.error(e);
    }
  }

  async function removeDislike(ticker) {
    if (!confirm('Remove ' + ticker + ' from excludes?')) return;
    try {
      await fetch('/api/frontend/abi-dislikes/' + encodeURIComponent(ticker), { method: 'DELETE' });
      await loadDislikes();
    } catch (e) {
      console.error(e);
    }
  }
})();
